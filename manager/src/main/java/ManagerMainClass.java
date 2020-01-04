import com.amazonaws.services.sqs.model.Message;

public class ManagerMainClass {
    public static void main(String[] args) {
        Manager manager = new Manager(args[0], Integer.valueOf(args[1]));

        while (true) {
            //listen to the sqs queue
            Message message = manager.readMessagesLookFor("Input_location", manager.getLocalAppQueueUrl());
            //in case we found a message with input location - processing it
            if (message != null) {//message format - Input_location-Bucket_name %s Key %s
                String content = message.getBody();
                String key = content.split(" ")[3];
                String bucketName = content.split(" ")[1];
                int id = Integer.parseInt(content.split(" ")[5]);
                manager.insertToInputBuckets(bucketName, id);

                String fileContent = manager.downloadFile(bucketName, key);

                manager.deleteObject(bucketName,key);

                manager.processInput(fileContent, id);

                manager.deleteMessage(message, manager.getLocalAppQueueUrl());
            }

            message = manager.readMessagesLookFor("Terminate", manager.getLocalAppQueueUrl());
            if (message != null)
                break;
        }
        manager.terminate();
    }
}


//TODO to think about - where do we need the concurrency? in the queue reading or in the data processing? both?
//TODO add logs
