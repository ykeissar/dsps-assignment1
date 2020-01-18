import com.amazonaws.services.sqs.model.Message;

import java.io.File;
import java.util.*;

public class LocalAppMainClass {
    public static void main(String[] args) {
        //agruments processing
        boolean terminateManager = args.length % 2 == 0;
        int workerMessageRatio = terminateManager ? Integer.parseInt(args[args.length - 2]) : Integer.parseInt(args[args.length - 1]);
        List<String> inputs = new ArrayList<String>();
        List<String> outputs = new ArrayList<String>();
        int numOfInputs = (args.length - 1) / 2;

        LocalApp myApp = new LocalApp(workerMessageRatio);

        for (int i = 0; i < numOfInputs; i++) {
            inputs.add(args[i]);
            outputs.add(args[i + numOfInputs]);
        }

        Set<String> inWork = new HashSet<String>();

        myApp.setManagerQueue();

        myApp.startConnection();

        //upload input files to s3
        String id = "0";
        for (String address : inputs) {
            File inputFile = new File(address);
            String inputKey = myApp.uploadFile(inputFile);
            inWork.add(id);

            //sending message to the queue with input location
            myApp.sendMessage(new StringBuilder()//<ID> INPUT <BUCKET> KEY <key> ID <inputID>
                    .append(myApp.getQueueId())
                    .append(" Input_location-Bucket_name ")
                    .append(myApp.getBucketName())
                    .append(" Key ")
                    .append(inputKey)
                    .append(" ID ")
                    .append(id)
                    .toString());
            id = Integer.toString(Integer.parseInt(id) + 1);
        }
        System.out.println("Waiting for outputs from Manager.");

        //main loop, work until all works are done.
        while (!inWork.isEmpty()) {
            Message message = myApp.readMessagesLookFor("Output in bucket", myApp.getReceivingQueueUrl()); //message format - <io index>\n s3object's key\n DSPS_assignment1 output in bucket
            if (message != null) {
                String[] rows = message.getBody().split("\n");
                String ioIndex = rows[0];
                String outputKey = rows[1];
                String outputContent = myApp.downloadFile(outputKey);
                myApp.toHtml(outputContent, outputs.get(Integer.parseInt(ioIndex)));
                inWork.remove(ioIndex);
                myApp.deleteMessage(message, myApp.getReceivingQueueUrl());
                myApp.deleteObject(outputKey);
            }
        }
        if (terminateManager)
            myApp.terminateManager();
        myApp.terminate();
    }
}
