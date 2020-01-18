import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.model.Instance;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InputProcessor implements Runnable {
    private File input;
    private Manager manager;
    private int inputId;
    private int appId;
    private Map<Integer, Boolean> messagesProcessed = new HashMap<Integer, Boolean>();


    public InputProcessor(File input, Manager manager, int id, int appId) {
        this.input = input;
        this.manager = manager;
        this.inputId = id;
        this.appId = appId;
    }

    public void run() {
        try {
            //parsing file
            List<JSONArray> totalReviews = new ArrayList<JSONArray>();

            JSONParser parser = new JSONParser();
            FileReader fileReader = new FileReader(input);

            List<JSONObject> list = new ArrayList<JSONObject>();
            BufferedReader br = new BufferedReader(fileReader);
            String line = null;
            JSONObject obj2;

            while ((line = br.readLine()) != null) {
                obj2 = (JSONObject) new JSONParser().parse(line);
                list.add(obj2);
            }

            for (JSONObject obj1 : list) {
                br = new BufferedReader(new StringReader(obj1.toString()));
                line = br.readLine();
                JSONObject obj25 = (JSONObject) parser.parse(line);
                JSONArray reviews = (JSONArray) obj25.get("reviews");
                totalReviews.addAll(reviews);
            }

            String processedUrl = manager.createQueue("Processed_AppId-" + appId + "_InputId-" + inputId);
            String unprocessedUrl = manager.createQueue("Unprocessed_AppId-" + appId + "_InputId-" + inputId);

            int messageCount = 0;

            //put all messages in queue
            for (Object review : totalReviews) {
                messagesProcessed.put(messageCount, false);
                manager.sendMessage("UNPROCESSED\n" + messageCount + "\n" + review.toString(), unprocessedUrl);// check if correct to do this like this, wont stop manager.
                messageCount++;                                                     // outputs first line is 'UNPROCESSED', second line id.
            }
            List<Instance> workers = null;
            try {
                workers = manager.runNWorkers(processedUrl, unprocessedUrl, Math.max(1, messageCount / manager.getWorkersRatio()), appId, inputId);
                manager.log("Starting " + Math.max(1, messageCount / manager.getWorkersRatio()) + " workers.");
            } catch (AmazonServiceException ase) {
                manager.log("Caught Exception: " + ase.getMessage());
                manager.log("Reponse Status Code: " + ase.getStatusCode());
                manager.log("Error Code: " + ase.getErrorCode());
                manager.log("Request ID: " + ase.getRequestId());
            }

            manager.processOutput(processedUrl, unprocessedUrl, inputId, messageCount, messagesProcessed, workers, appId);

        } catch (Exception e) {
            manager.log(e.getMessage());
        }
    }
}