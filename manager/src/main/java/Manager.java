import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
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


public class Manager {
    private AmazonS3 s3;
    private AmazonEC2 ec2;
    private AmazonSQS sqs;
    private String localAppQueueUrl;
    private ExecutorService inputReadingPool = Executors.newCachedThreadPool();
    private ExecutorService outputReadingPool = Executors.newCachedThreadPool();
    private int workersRatio;
    private Map<Integer, String> inputBuckets = new HashMap<Integer, String>();
    private Map<String, Integer> reverseInputBuckets = new HashMap<String, Integer>();
    //private AWSCredentialsProvider credentialsProvider;//TODO delete
    private final String IAM_ARN = "arn:aws:iam::592374997611:instance-profile/Worker";
    private final String AMI = "ami-00221e3ef03dfd01b";
    private final String KEY = "my_key3";
    private final String INSTANCE_TYPE = InstanceType.T2Xlarge.toString();


    public Manager(String queueUrl, int workersRatio) {
        //credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials()); //TODO delete

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
    }

    public String getLocalAppQueueUrl() {
        return localAppQueueUrl;
    }

    public void processInput(File input, int id) {
        System.out.println("Start processing input "+id);
        inputReadingPool.execute(new InputProcessor(input, this, id));
    }

    public void insertToInputBuckets(String bucketName, int id) {

        inputBuckets.put(id, bucketName);
        reverseInputBuckets.put(bucketName, id);
    }

    public String getBucketName(int id) {
        return inputBuckets.get(id);
    }

    public void processOutput(String queueUrl, int id, int messageCount, Map<Integer, Boolean> messagesProcessed, List<Instance> workers) {
        System.out.println("Start processing output "+id);
        outputReadingPool.execute(new OutputHandler(queueUrl, this, id, messageCount, messagesProcessed, workers));
    }

    //----------------------------------EC2---------------------------------
    public List<Instance> runNWorkers(String queueUrl, int messageCount) {
        boolean workersStarted = false;
        List<Instance> workers = new ArrayList<Instance>();

        while(!workersStarted) {
            workersStarted = true;
            RunInstancesRequest request = new RunInstancesRequest(AMI, 1, messageCount / workersRatio);
            request.setInstanceType(INSTANCE_TYPE);
            request.setKeyName(KEY);

            List<String> jarsToDownloand = new ArrayList<String>();
            jarsToDownloand.add("worker.jar");
            jarsToDownloand.add("ejml-0.23.jar");
            jarsToDownloand.add("stanford-corenlp-3.9.2.jar");
            jarsToDownloand.add("stanford-corenlp-3.9.2-models.jar");
            jarsToDownloand.add("jollyday.jar");


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
                    .append(queueUrl);//TODO stanford jars?

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

            System.out.println("Starting " + messageCount / workersRatio + " worker" + ((messageCount / workersRatio == 1) ? "." : "s."));
            try {
                workers = ec2.runInstances(request).getReservation().getInstances();
            } catch (AmazonServiceException ase) {
                System.out.println("Caught Exception: " + ase.getMessage());
                System.out.println("Reponse Status Code: " + ase.getStatusCode());
                System.out.println("Error Code: " + ase.getErrorCode());
                System.out.println("Request ID: " + ase.getRequestId());
                workersStarted = false;
            }

            List<String> ids = new ArrayList<String>();
            for (Instance worker : workers)
                ids.add(worker.getInstanceId());

            List<Tag> tags = new ArrayList<Tag>();
            tags.add(new Tag("App", "Worker"));
            CreateTagsRequest tagsRequest = new CreateTagsRequest(ids, tags);
            ec2.createTags(tagsRequest);
        }
        return workers;

    }

    public void shutdownWorkers(List<Instance> instances) {
        StopInstancesRequest request = new StopInstancesRequest();
        List<String> ids = new ArrayList<String>();
        for (Instance worker : instances)
            ids.add(worker.getInstanceId());
        request.setInstanceIds(ids);
        ec2.stopInstances(request);
    }

    //----------------------------------SQS--------------------------------- //TODO SQS Visibility Time-out - find out whats next

    public String createQueue() {
        CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyQueue" + UUID.randomUUID());
        String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        System.out.println(String.format("Creating Sqs queue with url - %s.", myQueueUrl));

        return myQueueUrl;
    }

    public void deleteQueue(String queueUrl) {
        sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
    }

    public void sendMessage(String message, String queueUrl) {
        sqs.sendMessage(new SendMessageRequest(queueUrl, message));
        System.out.println(String.format("Sending message '%s' to queue with url - %s.", message, queueUrl));
    }

    public void deleteMessage(Message message, String queueUrl) {
        sqs.deleteMessage(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));
    }

    public Message readMessagesLookFor(String lookFor, String queueUrl) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
        receiveMessageRequest.setMaxNumberOfMessages(1);
        receiveMessageRequest.setWaitTimeSeconds(5);

        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        for (Message message : messages) {
            if (message.getBody().contains(lookFor)) {
                sqs.changeMessageVisibility(queueUrl,message.getReceiptHandle(),100);
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
            System.out.println(String.format("Downloading an object from bucket name - %s, key - %s", bucketName, key));
            S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
            S3ObjectInputStream inputStream = object.getObjectContent();

            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            //int localAppId = reverseInputBuckets.get(bucketName);
            file = new File(key+".txt");

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
            System.out.println(e.getCause());
            System.out.println(e.getMessage());
        }
        return file;
    }


    public String uploadFile(String bucketName, String cont, int outPutId) {
        //TODO add logs
        String key = "output_file_number " + outPutId + ".txt";
        int localAppId = reverseInputBuckets.get(bucketName);
        String localPath = "output_" + localAppId + "/" + key;
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

    public void uploadOutputFile(String bucketName, String cont, int id) {
        String key = uploadFile(bucketName, cont, id);
        sendMessage(id + "\n" + key + "\nOutput in bucket", getLocalAppQueueUrl());//TODO verify indexes are identical!!!!!
    }//<io index>\n s3object's key\n DSPS_assignment1 output in bucket

    public void deleteObject(String bucketName, String key) {

        s3.deleteObject(bucketName, key);
    }

    public void terminate() {
        inputReadingPool.shutdown();
        outputReadingPool.shutdown();
    }
}
