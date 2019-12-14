import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class InputProcessor implements Runnable {
    private String input;
    private Manager manager;
    private int id;

    public InputProcessor(String input, Manager manager, int id) {
        this.input = input;
        this.manager = manager;
        this.id = id;
    }

    public void run() {
        try {
            List<JSONArray> list = new ArrayList<JSONArray>();
            JSONParser parser = new JSONParser();
            BufferedReader br = new BufferedReader(new StringReader(input));

            for (String line; (line = br.readLine()) != null; ) {
                JSONObject obj2 = (JSONObject) parser.parse(line);
                JSONArray reviews = (JSONArray) obj2.get("reviews");
                list.add(reviews);
            }

            String url = manager.createQueue();
            int messageCount = 0;
            //put all messages in queue
            for (JSONArray array : list) {
                for (Object obj : array) {
                    manager.sendMessage("UNPROCESSED\n" + obj.toString(), url);// check if correct to do this like this, wont stop manager.
                    messageCount++;                                                     // outputs first line is 'UNPROCESSED'.
                }
            }
            manager.runNWorkers(url);
            manager.processOutput(url, id, messageCount);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
