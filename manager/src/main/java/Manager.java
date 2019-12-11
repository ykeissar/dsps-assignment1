import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
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
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.apache.commons.codec.binary.Base64;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.util.ArrayList;
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
    private ExecutorService pool = Executors.newCachedThreadPool();
    private List<String> workingQueues = new ArrayList<String>();
    private List<ExecutorService> outputPools = new ArrayList<ExecutorService>();
    private List<String> outputsContents = new ArrayList<String>();

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
        pool.execute(new InputProcessor(input, 5, this));//change int
    }

    public void processOutput(String queueUrl) {
        ExecutorService outputPool = Executors.newCachedThreadPool();
        outputPools.add(outputPool);
        String output = "";
        outputPool.execute(new OutputProcessor(queueUrl, this, output));

    }

    public void addWorkingQueue(String queueUrl) {
        workingQueues.add(queueUrl);
        processOutput(queueUrl);
    }

    //----------------------------------EC2---------------------------------
    public List<Instance> runNWorkers(int n, String queueUrl) {
        RunInstancesRequest request = new RunInstancesRequest("ami-0c5204531f799e0c6", n, n);
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

    //----------------------------------SQS---------------------------------

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

    //----------------------------------S3----------------------------------

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
            String firstLine = message.getBody().substring(0,message.getBody().indexOf("\n"));
            if (firstLine.equals(lookFor)) {
                return message;
            }
        }
        return null;
    }

    public void deleteMessage(Message message, String queueUrl) {
        sqs.deleteMessage(queueUrl, message.getReceiptHandle());
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
            List<JSONArray> list = new ArrayList<JSONArray>();
            File fileName = new File("/Users/yoav.keissar/Downloads/jsonFile");
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            JSONParser parser = new JSONParser();

            for (String line; (line = br.readLine()) != null; ) {
                JSONObject obj2 = (JSONObject) parser.parse(line);
                JSONArray reviews = (JSONArray) obj2.get("reviews");
                list.add(reviews);
            }


        } catch (Exception e) {
        }

    }
}
