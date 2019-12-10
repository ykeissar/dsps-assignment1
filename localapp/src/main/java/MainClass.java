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
        boolean terminateManager = args.length % 2 == 0;
        int nWorkers = terminateManager ? Integer.parseInt(args[args.length - 2]) : Integer.parseInt(args[args.length - 1]);
        List<String> inputs = new ArrayList<String>();
        List<String> outputs = new ArrayList<String>();
        Map<Integer, String> inWork = new HashMap<Integer, String>();
        int numOfInputs = (args.length - 1) / 2;

        for (int i = 0; i < numOfInputs; i++) {
            inputs.add(args[i]);
            outputs.add(args[i + numOfInputs]);
        }

        List<Instance> instanceList = new ArrayList<Instance>();
        //TODO VERY IMPORTANT - move try,catch to each method, not all together.
        try {
            if (!myApp.doesManagerActive())
                instanceList.add(myApp.startManager());//TODO use nWorkers - think where.

            //upload input files to s3
            int index = 0;
            for (String address : inputs) {
                File inputFile = new File(address); //TODO verify that right arg and syntax
                String inputKey = myApp.uploadFile(inputFile);
                inWork.put(index, inputKey);

                //sending message to the queue with input location
                myApp.sendMessage(String.format("Input location: Bucket name - %s, Key - %s", myApp.getBucketName(), inputKey));
            }

            //main loop, work until all works are done.
            while (!inWork.isEmpty()) {
                String messageBody = myApp.readMessagesLookFor("DSPS_assignment1 output in bucket"); //message format - <io index>\n s3object's key\n DSPS_assignment1 output in bucket
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

        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());

        }

    }
}
