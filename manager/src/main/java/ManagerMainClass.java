import com.amazonaws.services.sqs.model.Message;

public class ManagerMainClass {
    public static void main(String[] args) {
        Manager manager = new Manager();

        while (manager.shouldTerminate()) {
            //listen to the sqs queue
            Message message = manager.readMessagesLookFor("Input_location", manager.getLocalAppQueueUrl());
            //in case we found a message with input location - processing it
            if (message != null) {//message format - Input_location-Bucket_name %s Key %s
                String content = message.getBody();
                String key = content.split(" ")[3];

                String bucketName = content.split(" ")[1];
                int id = manager.insertToInputBuckets(bucketName);


                String fileContent = manager.downloadFile(bucketName, key);
                manager.processInput(fileContent, id);

                manager.deleteMessage(message,manager.getLocalAppQueueUrl());
            }
        }
    }
}

//TODO to think about - where do we need the concurrency? in the queue reading or in the data processing? both?
//TODO add logs
//TODO switch all to Strings to StringBuilder (maby add a little StringUtil?)