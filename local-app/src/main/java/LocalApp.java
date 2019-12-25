import com.amazonaws.AmazonServiceException;
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
import java.util.*;

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
    private String lpId = UUID.randomUUID().toString();
    private Queue<String> testManagerQueue = new LinkedList<String>();

    public LocalApp() {
        try {
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
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());

        }

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
    public Instance doesManagerActive() {
        try {
            boolean done = false;
            DescribeInstancesRequest request = new DescribeInstancesRequest();
            List<String> tags = new ArrayList<String>();
            tags.add("Manager");
            Filter filter = new Filter("tag:App", tags);
            request.withFilters(filter);
            while (!done) {
                DescribeInstancesResult response = ec2.describeInstances(request);
                for (Reservation reservation : response.getReservations()) {
                    if (!reservation.getInstances().isEmpty()) {
                        done = true;
                        return reservation.getInstances().get(0);
                    }
                }

                request.setNextToken(response.getNextToken());

                if (response.getNextToken() == null) {
                    done = true;
                }
            }
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());

        }
        return null;
    }

    public Instance getManager(int workerMessageRatio) {//TODO add logs
        try {
            Instance manager = doesManagerActive();
            if (manager != null)
                return manager;
            RunInstancesRequest request = new RunInstancesRequest(AMI, 1, 1);
            request.setInstanceType(InstanceType.T1Micro.toString());
            request.setKeyName(KEY_PAIR);
            String workerRatio = Integer.toString(workerMessageRatio);
            String bootstrapManager = new StringBuilder().append("$aws s3 s3://amiryoavbucket4848/managar.jar\n")
                    .append("$ java -jar Manager.jar ")
                    .append(getQueueUrl())
                    .append(" ")
                    .append(workerRatio)
                    .toString();

            //TODO run Manager jar with correct args

            String base64BootstrapManager = null;
            try {
                base64BootstrapManager = new String(Base64.encodeBase64(bootstrapManager.getBytes("UTF-8")), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }

// Manager userdata
//        String getProject = "wget https://github.com/amirtal75/Mevuzarot/archive/master.zip\n";
//        String unzip = getProject + "unzip master.zip\n";
//        String goToProjectDirectory = unzip + "cd Mevuzarot-master/Project1/\n";
//        String removeSuperPom = goToProjectDirectory + "sudo rm pom.xml\n";
//        String setWorkerPom = removeSuperPom + "sudo cp managerpom.xml pom.xml\n";
//        String buildProject = setWorkerPom + "sudo mvn -T 4 install\n";
//        String createAndRunProject = "sudo java -jar target/core-java-1.0-SNAPSHOT.jar\n";
//
//        String createManagerArgsFile = "touch src/main/java/managerArgs.txt\n";
//        String pushFirstArg =  createManagerArgsFile + "echo " + QueueUrlLocalApps + " >> src/main/java/managerArgs.txt\n";
//        String filedata = pushFirstArg + "echo " + summeryFilesIndicatorQueue + " >> src/main/java/managerArgs.txt\n";
//
//        String userdata = "#!/bin/bash\n" +  buildProject + filedata +createAndRunProject;
//
//        System.out.println("Local Queue: " + QueueUrlLocalApps + ", Summary Queue: " + summeryFilesIndicatorQueue);
//        System.out.println("UserData: " + userdata)

            request.setUserData(base64BootstrapManager);
            Instance i = ec2.runInstances(request).getReservation().getInstances().get(0);

            List<String> ids = new ArrayList<String>();
            ids.add(i.getInstanceId());

            List<Tag> tags = new ArrayList<Tag>();
            tags.add(new Tag("Owner", "Amir_Yoav"));
            tags.add(new Tag("App", "Manager"));
            CreateTagsRequest tagsRequest = new CreateTagsRequest(ids, tags);
            ec2.createTags(tagsRequest);
            return i;
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());

        }
        return null;
    }

    //----------------------------------S3----------------------------------

    public String uploadFile(File file) {
        try {
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
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());

        }
        return null;
    }

    public String downloadFile(String key) {
        try {
            System.out.println(new StringBuilder("Downloading an object from bucket name - %s, key - %s").append(bucketName).append(key).toString());
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
                System.out.println(new StringBuilder("Exception while downloading from key %s, with reading buffer. Error: %s")
                        .append(key)
                        .append(e.getMessage())
                        .toString());
            }

            return text;
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());

        }
        return null;
    }

    //----------------------------------SQS---------------------------------

    public String getQueueUrl() {
        if (queueUrl == null)
            queueUrl = createQueue();
        return queueUrl;
    }

    private String createQueue() {
        try {
            CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyQueue" + UUID.randomUUID());
            String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            System.out.println(String.format("Creating Sqs queue with url - %s.", myQueueUrl));

            return myQueueUrl;
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());

        }
        return null;
    }

    public void sendMessage(String message) {
        try {
            sqs.sendMessage(new SendMessageRequest(getQueueUrl(), message));
            System.out.println(new StringBuilder("Sending message '%s' to queue with url - %s.")
                    .append(message)
                    .append(getQueueUrl())
                    .toString());
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());

        }
    }

    public String readMessagesLookFor(String lookFor) {
        try {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);

            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            for (Message message : messages) {
                if (message.getBody().contains(lookFor)) {
                    return message.getBody();
                }
            }
            return "";
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());

        }
        return null;

    }

    public static void main(String[] args) {
        LocalApp lp = new LocalApp();
        //lp.startManager();
//        lp.s3.createBucket("yoavsbucke83838");
//        String key = "7561157";
//
//        //File f = new File("/Users/yoav.keissar/Documents/assignment1/test");
//        PutObjectRequest req = new PutObjectRequest("yoavsbucke83838", key,"/Users/yoav.keissar/Documents/assignment1/test");
//        lp.s3.putObject(req);
        //request.setIamInstanceProfile(new IamInstanceProfileSpecification().withArn(ROLE));

    }
}
