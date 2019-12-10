import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
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

import java.io.*;
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
        RunInstancesRequest request = new RunInstancesRequest("ami-0c5204531f799e0c6", 1, 1);
        request.setInstanceType(InstanceType.T1Micro.toString());
        String bootstrapManager = "#!$ cd /opt\n" +
                "$ sudo wget --no-cookies --no-check-certificate --header \"Cookie: %3A%2F%2Fwww.oracle.com%2F; -securebackup-cookie\" http://download.oracle.com/otn-pub/java/jdk/8u151-b12/e758a0de34e24606bca991d704f6dcbf/jdk-8u151-linux-x64.tar.gz\n" +
                "$ sudo tar xzf jdk-8u151-linux-x64.tar.gz\n" +
                "$ cd jdk1.8.0_151/\n" +
                "$ sudo alternatives --install /usr/bin/java java /opt/jdk1.8.0_151/bin/java 2\n" +
                "$ sudo alternatives --config java\n" +
                "There are 2 programs which provide 'java'.\n" +
                "  Selection    Command\n" +
                "-----------------------------------------------\n" +
                "*+ 1           /usr/lib/jvm/jre-1.7.0-openjdk.x86_64/bin/java\n" +
                "   2           /opt/jdk1.8.0_151/bin/java\n" +
                "Enter to keep the current selection[+], or type selection number: 2" +
                "$ sudo alternatives --install /usr/bin/jar jar /opt/jdk1.8.0_151/bin/jar 2\n" +
                "$ sudoalternatives --set jar /opt/jdk1.8.0_151/bin/jar\n" +
                "$ sudo alternatives --set javac /opt/jdk1.8.0_151/bin/javac" +
                "# vim /etc/profile\n" +
                "export JAVA_HOME=/opt/jdk1.8.0_151\n" +
                "export JRE_HOME=/opt/jdk1.8.0_151/jre\n" +
                "export PATH=$PATH:$JAVA_HOME/bin:$JRE_HOME/bin\n" +
                "Esc + :wq! (To save file)";// till here  - installing java
        //TODO add Manager tag, run Manager jar with correct args, SQS URL with local-app

        String base64BootstrapManager = null;
        try {
            base64BootstrapManager = new String(Base64.encodeBase64(bootstrapManager.getBytes("UTF-8")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        request.setUserData(base64BootstrapManager);
        return ec2.runInstances(request).getReservation().getInstances().get(0);
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

    }
}
