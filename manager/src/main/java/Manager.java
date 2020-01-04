import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import com.amazonaws.util.IOUtils;
import org.apache.commons.codec.binary.Base64;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;


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
    private AWSCredentialsProvider credentialsProvider;//TODO delete
    private final String IAM_ARN = "arn:aws:iam::592374997611:instance-profile/Worker";
    private final String AMI = "ami-00221e3ef03dfd01b";
    private final String KEY = "my_key3";


    public Manager(String queueUrl, int workersRatio) {
        credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials()); //TODO delete

        s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .withCredentials(credentialsProvider)//TODO delete
                .build();
        ec2 = AmazonEC2ClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .withCredentials(credentialsProvider)//TODO delete
                .build();

        sqs = AmazonSQSClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .withCredentials(credentialsProvider)//TODO delete
                .build();

        localAppQueueUrl = queueUrl;
        this.workersRatio = workersRatio;
    }

    public String getLocalAppQueueUrl() {
        return localAppQueueUrl;
    }

    public void processInput(String input, int id) {
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
        outputReadingPool.execute(new OutputHandler(queueUrl, this, id, messageCount, messagesProcessed,workers));
    }

    public static void main(String[] args) {
        Manager man = new Manager("", 0);

        String queue = man.createQueue();

        RunInstancesRequest request = new RunInstancesRequest(man.AMI, 1, 1);
        request.setInstanceType(InstanceType.T1Micro.toString());
        request.setKeyName("my_key3");
        String bootstrapManager = new StringBuilder()
                .append("#! /bin/bash\n")
                .append("aws s3 cp s3://yoavsbucke83838/worker-1.0-SNAPSHOT.jar worker-1.0-SNAPSHOT.jar\n")
                .append("java -jar worker-1.0-SNAPSHOT.jar ")
                .append(queue)
                .toString();

        String base64BootstrapManager = null;
        try {
            base64BootstrapManager = new String(Base64.encodeBase64(bootstrapManager.getBytes("UTF-8")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        request.setUserData(base64BootstrapManager);
        IamInstanceProfileSpecification iam = new IamInstanceProfileSpecification();
        iam.setArn(man.IAM_ARN);
        request.setIamInstanceProfile(iam);
        man.ec2.runInstances(request);
        Message m = null;
        while (true) {
            m = man.readMessagesLookFor("", queue);
            if (m != null) {
                System.out.println(m.getBody());
                break;
            }
        }
        man.deleteQueue(queue);
    }

    //----------------------------------EC2---------------------------------
    public List<Instance> runNWorkers(String queueUrl, int messageCount) {
        RunInstancesRequest request = new RunInstancesRequest(AMI, messageCount / workersRatio, messageCount / workersRatio);
        request.setInstanceType(InstanceType.T1Micro.toString());
        request.setKeyName(KEY);

        String bootstrapManager = new StringBuilder()
                .append("#! /bin/bash\n")
                .append("aws s3 cp s3://yoavsbucke83838/worker-1.0-SNAPSHOT.jar worker-1.0-SNAPSHOT.jar\n")
                .append("java -jar worker-1.0-SNAPSHOT.jar ")//TODO stanford jars?
                .append(queueUrl)
                .toString();
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

        List<Instance> workers = ec2.runInstances(request).getReservation().getInstances();

        List<String> ids = new ArrayList<String>();
        for (Instance worker : workers)
            ids.add(worker.getInstanceId());

        List<Tag> tags = new ArrayList<Tag>();
        tags.add(new Tag("App", "Worker"));
        CreateTagsRequest tagsRequest = new CreateTagsRequest(ids, tags);
        ec2.createTags(tagsRequest);

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
        sqs.deleteMessage(queueUrl, message.getReceiptHandle());
    }

    public Message readMessagesLookFor(String lookFor, String queueUrl) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
        receiveMessageRequest.withWaitTimeSeconds(10);//TODO check if necessary
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
        receiveMessageRequest.withWaitTimeSeconds(10);
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

    public String downloadFile(String bucketName, String key) {
        System.out.println(String.format("Downloading an object from bucket name - %s, key - %s", bucketName, key));
        S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
        S3ObjectInputStream inputStream = object.getObjectContent();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String text = "";
        String temp = "";

        try {
            while ((temp = bufferedReader.readLine()) != null) {
                text = text + temp;
            }
            bufferedReader.close();
            inputStream.close();
        } catch (IOException e) {
            System.out.println(String.format("Exception while downloading from key %s, with reading buffer. Error: %s", key, e.getMessage()));
        }

        return text;
    }

    public String uploadFile(String bucketName, String cont, int outPutId) {
        //TODO add logs
        String key = "output_file_number " + outPutId + ".txt";
        int localAppId = reverseInputBuckets.get(bucketName);
        String localPath = "local_app_" + localAppId + "/" + key;
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
