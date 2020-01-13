import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.model.Instance;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InputProcessor implements Runnable {
    private File input;
    private Manager manager;
    private int inputId;
    private Map<Integer, Boolean> messagesProcessed = new HashMap<Integer, Boolean>();


    public InputProcessor(File input, Manager manager, int id) {
        this.input = input;
        this.manager = manager;
        this.inputId = id;
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

            String url = manager.createQueue();
            int messageCount = 0;

            //put all messages in queue
            for (Object review : totalReviews) {
                    messagesProcessed.put(messageCount, false);
                    manager.sendMessage("UNPROCESSED\n" + messageCount + "\n" + review.toString(), url);// check if correct to do this like this, wont stop manager.
                    messageCount++;                                                     // outputs first line is 'UNPROCESSED', second line id.
            }
            List<Instance> workers = null;
            try{
                workers = manager.runNWorkers(url, messageCount);
            } catch (AmazonServiceException ase) {
                System.out.println("Caught Exception: " + ase.getMessage());
                System.out.println("Reponse Status Code: " + ase.getStatusCode());
                System.out.println("Error Code: " + ase.getErrorCode());
                System.out.println("Request ID: " + ase.getRequestId());
            }

            manager.processOutput(url, inputId, messageCount, messagesProcessed, workers);

        } catch (Exception e) {
            System.out.println(e.getCause());
        }
    }

    public static void main(String[] args) throws ParseException, IOException {
        Manager man = new Manager("", 5);
        String path = "/Users/yoav.keissar/Documents/inputs/input1";

        List<JSONArray> totalReviews = new ArrayList<JSONArray>();

        JSONParser parser = new JSONParser();
        FileReader fileReader = new FileReader(path);

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

    }
}