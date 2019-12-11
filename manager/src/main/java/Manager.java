import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import java.io.*;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class Manager {
    private AmazonS3 s3;
    private AmazonEC2 ec2;
    private AWSCredentialsProvider credentialsProvider;
    private AmazonSQS sqs;
    private String localAppQueueUrl = null; //TODO fix how to get that url, many local-app same queue?
    private boolean terminate = false;
    private ExecutorService pool = Executors.newFixedThreadPool(5);

    public Manager(AWSCredentials credentials) {
        credentialsProvider = new AWSStaticCredentialsProvider(credentials);//TODO fix how to get credentials

        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(Regions.US_WEST_2)
                .build();
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(Regions.US_WEST_2)
                .build();

        sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(Regions.US_WEST_2)
                .build();

    }

    public boolean shouldTerminate() {
        return terminate;
    }

    public String getLocalAppQueueUrl() {
        return localAppQueueUrl;
    }

    public void processInput(String input) {
        pool.execute(new InputProcessor(input,5,this));//change int
    }

    public void runNWorker(int n,String queueUrl){

    }

    //----------------------------------SQS---------------------------------

    public String createQueue() {
        CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyQueue" + UUID.randomUUID());
        String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        System.out.println(String.format("Creating Sqs queue with url - %s.", myQueueUrl));

        return myQueueUrl;
    }
    //----------------------------------SQS---------------------------------

    public String readMessagesLookFor(String lookFor, String queueUrl) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        for (Message message : messages) {
            if (message.getBody().contains(lookFor)) {
                return message.getBody();
            }
        }
        return "";
    }

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

    public static void main(String[] args) {
//        try {
//            File fileName = new File("/Users/yoav.keissar/Downloads/jsonFile");
//            FileReader reader = new FileReader(fileName);
//            JSONParser parser = new JSONParser();
//
//            BufferedReader br = new BufferedReader(new FileReader(fileName));
//            for(String line; (line = br.readLine()) != null; ) {
//                JSONObject obj = (JSONObject) parser.parse(line);
//                JSONArray array = (JSONArray) obj.get("reviews");
//            }
//
//        } catch (Exception e) {
//            System.out.println(e.getMessage());
//        }
        try {
            File fileName = new File("/Users/yoav.keissar/Downloads/jsonFile");
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            String input = "";
            for(String line; (line = br.readLine()) != null; )
                input +=line+"\n";
            //Manager.processInput(input);


        }catch (Exception e){}

    }
}
