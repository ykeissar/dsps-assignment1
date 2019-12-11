import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class InputProcessor implements Runnable {
    private String input;
    private int nWorkers;
    private Manager manager;

    public InputProcessor(String input, int nWorkers, Manager manager) {
        this.input = input;
        this.nWorkers = nWorkers;
        this.manager = manager;
    }

    public void run() {
        try {
            List<JSONObject> list = new ArrayList<JSONObject>();
            JSONParser parser = new JSONParser();

            BufferedReader br = new BufferedReader(new StringReader(input));
            for (String line; (line = br.readLine()) != null; ) {
                list.add((JSONObject) parser.parse(line));
            }

            //setup numOfMessages/n workers in a pool
            String url = manager.createQueue();





        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
