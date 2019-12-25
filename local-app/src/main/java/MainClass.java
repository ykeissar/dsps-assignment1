import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.model.Instance;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MainClass {
    public static void main(String[] args) throws Exception {
        LocalApp myApp = new LocalApp();

        //agruments processing
        boolean terminateManager = args.length % 2 == 0;
        int workerMessageRatio = terminateManager ? Integer.parseInt(args[args.length - 2]) : Integer.parseInt(args[args.length - 1]);
        List<String> inputs = new ArrayList<String>();
        List<String> outputs = new ArrayList<String>();
        int numOfInputs = (args.length - 1) / 2;

        for (int i = 0; i < numOfInputs; i++) {
            inputs.add(args[i]);
            outputs.add(args[i + numOfInputs]);
        }

        Map<String, String> inWork = new HashMap<String, String>();

        myApp.getManager(workerMessageRatio);

        //upload input files to s3
        String id = "0";
        for (String address : inputs) {
            File inputFile = new File(address); //TODO verify that right arg and syntax
            String inputKey = myApp.uploadFile(inputFile);
            inWork.put(id, inputKey);

            //sending message to the queue with input location
            myApp.sendMessage(new StringBuilder("Input_location-Bucket_name ")
                    .append(myApp.getBucketName())
                    .append(" Key ")
                    .append(inputKey)
                    .append(" ID ")
                    .append(id)
                    .toString());

        }

        //main loop, work until all works are done.
        while (!inWork.isEmpty()) {
            String messageBody = myApp.readMessagesLookFor("Output in bucket"); //message format - <io index>\n s3object's key\n DSPS_assignment1 output in bucket
            if (messageBody.length() > 0) {
                String[] rows = messageBody.split("\n");
                int ioIndex = Integer.parseInt(rows[0]);
                String outputKey = rows[1];
                String outputContent = myApp.downloadFile(outputKey);
                myApp.toHtml(outputContent, inWork.get(ioIndex));
                inWork.remove(ioIndex);
            }
        }
        myApp.terminate();

    }

    //TODO GENERAL - unify all message formats.
    //TODO GENERAL -
}
