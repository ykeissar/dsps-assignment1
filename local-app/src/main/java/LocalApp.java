import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
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

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class LocalApp {
    private AmazonS3 s3;
    private AmazonEC2 ec2;
    private AWSCredentialsProvider credentialsProvider;
    private String bucketName = null;
    private AmazonSQS sqs;
    private String queueUrl = null;
    private boolean terminate = false;//TODO check when to terminate exactly!!
    private AmazonIdentityManagement iam;
    private static final String ROLE = "arn:aws:iam::592374997611:role/system_admin";
    private static final String AMI = "ami-00221e3ef03dfd01b";
    private static final String KEY_PAIR = "my_key3";

    public LocalApp() {
        credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());

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


//        iam =
//                AmazonIdentityManagementClientBuilder.defaultClient();

    }

    public void terminate() {
        //TODO add content
        terminate = true;

    }

    public boolean shouldTerminate() {
        return terminate;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void toHtml(String content, String address) {
        String html = new StringBuilder().append("<!DOCTYPE html>\n")
                .append("<html>\n")
                .append("<body>\n")
                .append(content)
                .append("</body>\n")
                .append("</html>\n").toString();
        File toReturn = new File(address);
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(toReturn));
            writer.write(html);
            writer.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    //----------------------------------EC2---------------------------------
    public boolean doesManagerActive() {
        //TODO insert content
        return false;
    }

    public Instance startManager() {//TODO add logs
        RunInstancesRequest request = new RunInstancesRequest(AMI, 1, 1);
        request.setInstanceType(InstanceType.T1Micro.toString());
        request.setKeyName(KEY_PAIR);
        String bootstrapManager = "$ java -jar Manager.jar " + getQueueUrl();
        //TODO run Manager jar with correct args

        String base64BootstrapManager = null;
        try {
            base64BootstrapManager = new String(Base64.encodeBase64(bootstrapManager.getBytes("UTF-8")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        request.setUserData(base64BootstrapManager);
        //   request.setIamInstanceProfile(new IamInstanceProfileSpecification().withArn(ROLE));//TODO find out more
        Instance i = ec2.runInstances(request).getReservation().getInstances().get(0);

        List<String> ids = new ArrayList<String>();
        ids.add(i.getInstanceId());

        List<Tag> tags = new ArrayList<Tag>();
        tags.add(new Tag("Owner", "Amir_Yoav"));
        tags.add(new Tag("App", "Manager"));
        CreateTagsRequest tagsRequest = new CreateTagsRequest(ids, tags);
        ec2.createTags(tagsRequest);
        return i;
    }

    //----------------------------------S3----------------------------------

    public String uploadFile(File file) {
        if (bucketName == null) {
            String bucketName =
                    credentialsProvider.getCredentials().getAWSAccessKeyId().replace('/', '_').replace(':', '_');
            System.out.println(String.format("Creating bucket %s.", bucketName));
            s3.createBucket(bucketName);
        }//TODO add logs
        String key = file.getName().replace('\\', '_').replace('/', '_').replace(':', '_');
        PutObjectRequest req = new PutObjectRequest(bucketName, key, file);
        s3.putObject(req);
        return key;
    }

    public String downloadFile(String key) {
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

    //----------------------------------SQS---------------------------------

    public String getQueueUrl() {
        if (queueUrl == null)
            queueUrl = createQueue();
        return queueUrl;
    }

    private String createQueue() {
        CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyQueue" + UUID.randomUUID());
        String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        System.out.println(String.format("Creating Sqs queue with url - %s.", myQueueUrl));

        return myQueueUrl;
    }

    public void sendMessage(String message) {
        sqs.sendMessage(new SendMessageRequest(getQueueUrl(), message));
        System.out.println(String.format("Sending message '%s' to queue with url - %s.", message, getQueueUrl()));

    }

    public String readMessagesLookFor(String lookFor) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        for (Message message : messages) {
            if (message.getBody().contains(lookFor)) {
                return message.getBody();
            }
        }
        return "";
    }

    public static void main(String[] args) {
        LocalApp lp = new LocalApp();
        RunInstancesRequest request = new RunInstancesRequest("ami-00221e3ef03dfd01b", 1, 1);
        request.setKeyName("my_key3");
        request.setInstanceType(InstanceType.T1Micro.toString());
        List<Tag> tags = new ArrayList<Tag>();



        List<Instance> instance = lp.ec2.runInstances(request).getReservation().getInstances();
        List<String> id = new ArrayList<String>();
        id.add(instance.get(0).getInstanceId());

        CreateTagsRequest tagsRequest = new CreateTagsRequest(id, tags);
        lp.ec2.createTags(tagsRequest);
    }
}
