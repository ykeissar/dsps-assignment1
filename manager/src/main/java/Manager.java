import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.apache.commons.codec.binary.Base64;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Manager {
    private AmazonS3 s3;
    private AmazonEC2 ec2;
    private AmazonSQS sqs;
    private String localAppQueueUrl = null; //TODO many local-app same queue?
    private ExecutorService inputReadingPool = Executors.newCachedThreadPool();
    private ExecutorService outputReadingPool = Executors.newCachedThreadPool();
    private int workersRatio;
    private Map<Integer, String> inputBuckets = new HashMap<Integer, String>();
    private int id = 0;

    public Manager(String queueUrl,int workersRatio) {        //TODO fix how to get credentials
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

    public void processInput(String input, int id) {
        inputReadingPool.execute(new InputProcessor(input, this, id));
    }

    public void insertToInputBuckets(String bucketName, int id) {
        inputBuckets.put(id, bucketName);
    }

    public String getBucketName(int id) {
        return inputBuckets.get(id);
    }

    public void processOutput(String queueUrl, int id, int messageCount, Map<Integer,Boolean> messagesProcessed) {
        outputReadingPool.execute(new OutputHandler(queueUrl, this, id, messageCount,messagesProcessed));
    }

    //----------------------------------EC2---------------------------------
    public List<Instance> runNWorkers(String queueUrl,int numOfWorkers) { //TODO continue method
        RunInstancesRequest request = new RunInstancesRequest("ami-0c5204531f799e0c6", numOfWorkers, numOfWorkers);//TODO fix ammount
        request.setInstanceType(InstanceType.T1Micro.toString());
        String bootstrapManager = new StringBuilder().append("$aws s3 s3://amiryoavbucket4848/worker.jar\n").toString();
        //"$ java -jar worker.jar " + getQueueUrl();
        //TODO run Worker jar with correct args, SQS URL with local-app

        String base64BootstrapManager = null;
        try {
            base64BootstrapManager = new String(Base64.encodeBase64(bootstrapManager.getBytes("UTF-8")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        request.setUserData(base64BootstrapManager);
        return ec2.runInstances(request).getReservation().getInstances();
    }

    //----------------------------------SQS--------------------------------- //TODO SQS Visibility Time-out - find out whats next

    public String createQueue() {
        CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyQueue" + UUID.randomUUID());
        String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        System.out.println(String.format("Creating Sqs queue with url - %s.", myQueueUrl));

        return myQueueUrl;
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

    public String uploadFile(String bucketName, String file, int id) {
        //TODO add logs
        String key = "output_file_number " + Integer.toString(id).replace('\\', '_').replace('/', '_').replace(':', '_');
        PutObjectRequest req = new PutObjectRequest(bucketName, key, file);
        s3.putObject(req);
        return key;
    }

    public static void main(String[] args) {

    }

    public void uploadOutputFile(String bucketName, String file, int id) {
        String key = uploadFile(bucketName, file, id);
        sendMessage(id + "\nkey\nOutput in bucket", getLocalAppQueueUrl());//TODO verify indexes are identical!!!!!

    }//<io index>\n s3object's key\n DSPS_assignment1 output in bucket

    public void terminate() {
        inputReadingPool.shutdown();

    }
}
