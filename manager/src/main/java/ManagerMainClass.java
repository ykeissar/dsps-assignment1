import com.amazonaws.services.sqs.model.Message;

import java.io.File;

public class ManagerMainClass {
    public static void main(String[] args) {
        Manager manager = new Manager(args[0], Integer.valueOf(args[1]));
        manager.log("Listening for messages...");
        while (true) {
            Message message = manager.readMessagesLookFor("New_Local_App", manager.getLocalAppQueueUrl());
            if (message != null) {
                String content = message.getBody();
                String queue = content.substring(14);
                int id = manager.getNextId();
                manager.addLocalApp(id, queue);
                manager.sendMessage("This_is_ID" + id, queue);
                manager.deleteMessage(message, manager.getLocalAppQueueUrl());
            }

            //listen to the sqs queue
            message = manager.readMessagesLookFor("Input_location", manager.getLocalAppQueueUrl());
            //in case we found a message with input location - processing it
            if (message != null) {//message format - Input_location-Bucket_name %s Key %s
                String content = message.getBody();
                int appId = Integer.parseInt(content.split(" ")[0]);
                String key = content.split(" ")[4];
                String bucketName = content.split(" ")[2];
                int inputId = Integer.parseInt(content.split(" ")[6]);
                manager.insertToInputBuckets(bucketName, inputId);

                File inputFile = manager.downloadFile(bucketName, key);

                manager.deleteObject(bucketName, key);

                manager.deleteMessage(message, manager.getLocalAppQueueUrl());

                manager.processInput(inputFile, inputId, appId);
            }

            message = manager.readMessagesLookFor("Terminate", manager.getLocalAppQueueUrl());
            if (message != null)
                break;
        }

        manager.terminate();
    }
}