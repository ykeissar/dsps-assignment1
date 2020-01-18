import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


public class Manager {
    private final String IAM_ARN = "arn:aws:iam::592374997611:instance-profile/Worker";
    private final String AMI = "ami-00221e3ef03dfd01b";
    private final String KEY = "my_key3";
    private final String INSTANCE_TYPE = InstanceType.T3Xlarge.toString();
    private AmazonS3 s3;
    private AmazonEC2 ec2;
    private AmazonSQS sqs;
    private String localAppQueueUrl;
    private ExecutorService inputReadingPool = Executors.newCachedThreadPool();
    private ExecutorService outputReadingPool = Executors.newCachedThreadPool();
    private int workersRatio;
    private Map<Integer, String> inputBuckets = new HashMap<Integer, String>();
    private Map<String, Integer> reverseInputBuckets = new HashMap<String, Integer>();
    private int idCounter = 0;
    private Map<Integer, String> localAppQueueMap = new HashMap<Integer, String>();
    private Set<Pair> workinProgress = new HashSet<Pair>();
    private Logger logger;


    public Manager(String queueUrl, int workersRatio) {
        s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .build();

        ec2 = AmazonEC2ClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .build();

        sqs = AmazonSQSClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .build();

        localAppQueueUrl = queueUrl;
        this.workersRatio = workersRatio;

        logger = Logger.getLogger("MyLog");
        FileHandler fh;

        try {

            // This block configure the logger with handler and formatter
            fh = new FileHandler("/home/ec2-user/log.txt");
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);

            // the following statement is used to log any messages
            logger.info("Logger Stated!");

        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Manager started at: " + new Date(System.currentTimeMillis()));
    }

    public int getNextId() {
        return idCounter++;
    }

    public void addLocalApp(int id, String queue) {
        localAppQueueMap.put(id, queue);
    }

    public String getLocalAppQueueUrl() {
        return localAppQueueUrl;
    }

    public void processInput(File input, int id, int appId) {
        logger.info("Start processing input " + id);
        workinProgress.add(new Pair(appId, id));
        inputReadingPool.execute(new InputProcessor(input, this, id, appId));
    }

    public void insertToInputBuckets(String bucketName, int id) {
        inputBuckets.put(id, bucketName);
        reverseInputBuckets.put(bucketName, id);
    }

    public String getBucketName(int id) {
        return inputBuckets.get(id);
    }

    public void processOutput(String processedUrl, String unprocessedUrl, int id, int messageCount, Map<Integer, Boolean> messagesProcessed, List<Instance> workers, int appId) {
        logger.info("Start processing output " + id);
        outputReadingPool.execute(new OutputHandler(processedUrl, unprocessedUrl, this, id, messageCount, messagesProcessed, workers, messageCount / workersRatio, appId));
    }

    //----------------------------------EC2---------------------------------
    public List<Instance> runNWorkers(String processedUrl, String unprocessedUrl, int numOfWorkers, int appId, int inputId) {
        List<Instance> workers = new ArrayList<Instance>();

        RunInstancesRequest request = new RunInstancesRequest(AMI, 1, numOfWorkers);
        request.setInstanceType(INSTANCE_TYPE);
        request.setKeyName(KEY);

        List<String> jarsToDownloand = new ArrayList<String>();
        jarsToDownloand.add("worker.jar");
        jarsToDownloand.add("ejml-0.23.jar");
        jarsToDownloand.add("stanford-corenlp-3.9.2.jar");
        jarsToDownloand.add("stanford-corenlp-3.9.2-models.jar");
        jarsToDownloand.add("jollyday.jar");

        //setting workrs user-data
        StringBuilder userData = new StringBuilder().append("#! /bin/bash\n").append("cd /home/ec2-user\n");
        for (String jar : jarsToDownloand) {
            userData.append("aws s3 cp s3://yoavsbucke83838/").append(jar).append(" ").append(jar).append("\n");
        }
        userData.append("java -cp .:").append(jarsToDownloand.get(0))
                .append(":")
                .append(jarsToDownloand.get(1))
                .append(":")
                .append(jarsToDownloand.get(2))
                .append(":")
                .append(jarsToDownloand.get(3))
                .append(":")
                .append(jarsToDownloand.get(4))
                .append(" WorkerMainClass ")
                .append(processedUrl)
                .append(" ")
                .append(unprocessedUrl);

        String bootstrapManager = userData.toString();
        String base64BootstrapManager = null;
        try {
            base64BootstrapManager = new String(Base64.encodeBase64(bootstrapManager.getBytes("UTF-8")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        request.setUserData(base64BootstrapManager);
        IamInstanceProfileSpecification iam = new IamInstanceProfileSpecification();
        iam.setArn(IAM_ARN);
        request.setIamInstanceProfile(iam);

        try {
            workers = ec2.runInstances(request).getReservation().getInstances();
        } catch (AmazonServiceException ase) {
            return workers;
        }

        List<String> ids = new ArrayList<String>();
        for (Instance worker : workers)
            ids.add(worker.getInstanceId());

        //setting workers tags
        List<Tag> tags = new ArrayList<Tag>();
        String inputIds = "App-" + appId + "_Input-" + inputId;
        tags.add(new Tag("App", "Worker"));
        tags.add(new Tag("InputIds", inputIds));
        CreateTagsRequest tagsRequest = new CreateTagsRequest(ids, tags);
        ec2.createTags(tagsRequest);

        return workers;

    }

    public void shutdownWorkers(List<Instance> instances) {
        TerminateInstancesRequest request = new TerminateInstancesRequest();
        List<String> ids = new ArrayList<String>();
        for (Instance worker : instances)
            ids.add(worker.getInstanceId());
        request.setInstanceIds(ids);
        ec2.terminateInstances(request);
    }

    //----------------------------------SQS---------------------------------

    public String createQueue(String name) {
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(name + "_" + UUID.randomUUID());
        String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        logger.info(String.format("Creating Sqs queue with url - %s.", myQueueUrl));
        return myQueueUrl;
    }

    public void deleteQueue(String queueUrl) {
        sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
    }

    public void sendMessage(String message, String queueUrl) {
        sqs.sendMessage(new SendMessageRequest(queueUrl, message));
        logger.info(String.format("Sending message '%s' to queue with url - %s.", message, queueUrl));
    }

    public void deleteMessage(Message message, String queueUrl) {
        sqs.deleteMessage(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));
    }

    public Message readMessagesLookFor(String lookFor, String queueUrl) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
        receiveMessageRequest.setMaxNumberOfMessages(1);
        receiveMessageRequest.setWaitTimeSeconds(5);
        receiveMessageRequest.setVisibilityTimeout(0);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        for (Message message : messages) {
            if (message.getBody().contains(lookFor)) {
                return message;
            }
        }
        return null;
    }

    public Message readMessagesLookForFirstLine(String lookFor, String queueUrl) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
        receiveMessageRequest.setMaxNumberOfMessages(1);
        receiveMessageRequest.setWaitTimeSeconds(5);
        receiveMessageRequest.setVisibilityTimeout(0);

        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        for (Message message : messages) {
            String firstLine = message.getBody().substring(0, message.getBody().indexOf("\n"));
            if (firstLine.equals(lookFor)) {
                return message;
            }
        }
        return null;
    }

    //----------------------------------S3----------------------------------

    public File downloadFile(String bucketName, String key) {
        File file = null;
        try {
            logger.info(String.format("Downloading an object from bucket name - %s, key - %s", bucketName, key));
            S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
            S3ObjectInputStream inputStream = object.getObjectContent();

            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            file = new File(key + ".txt");

            Writer writer = new OutputStreamWriter(new FileOutputStream(file));
            while (true) {
                String line = null;
                line = bufferedReader.readLine();
                if (line == null)
                    break;

                writer.write(line + "\n");
            }
            writer.close();
        } catch (IOException e) {
            logger.info(e.getMessage());
        }
        return file;
    }

    public String uploadFile(String bucketName, String cont, int outputId, int appId) {
        String key = "output_file_number " + outputId + ".txt";
        String localPath = "output_" + appId + "/" + key;
        File file = new File(localPath);
        try {
            FileUtils.writeStringToFile(file, cont);
        } catch (IOException e) {
            e.printStackTrace();
        }

        PutObjectRequest req = new PutObjectRequest(bucketName, localPath.substring(localPath.indexOf("/") + 1), file);
        s3.putObject(req);
        return key;
    }

    public void uploadOutputFile(String bucketName, String cont, int id, int appId) {
        String key = uploadFile(bucketName, cont, id, appId);
        removePair(appId, id);
        sendMessage(id + "\n" + key + "\nOutput in bucket", localAppQueueMap.get(appId));
    }

    public void deleteObject(String bucketName, String key) {

        s3.deleteObject(bucketName, key);
    }

    public void terminate() {
        logger.info("Waiting to finish work");
        while (!workinProgress.isEmpty()) {
        }
        logger.info("All work is done.");

        inputReadingPool.shutdown();
        outputReadingPool.shutdown();
        for (String queue : localAppQueueMap.values())
            sqs.deleteQueue(new DeleteQueueRequest(queue));
        sendMessage("Terminating done!", localAppQueueUrl);
        logger.info("Manager ended at: " + new Date(System.currentTimeMillis()));

    }

    private void removePair(int appId, int id) {
        for (Pair p : workinProgress) {
            if (p.getFirst() == appId && p.getSecond() == id) {
                workinProgress.remove(p);
                break;
            }
        }
    }

    public int getWorkersRatio() {
        return workersRatio;
    }

    public void log(String s) {
        logger.info(s);
    }

    public static class Pair {
        private int first;
        private int second;

        public Pair(int appId, int id) {
            first = appId;
            second = id;
        }

        public int getFirst() {
            return first;
        }

        public int getSecond() {
            return second;
        }

    }
}
