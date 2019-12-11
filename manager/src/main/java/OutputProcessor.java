import com.amazonaws.services.sqs.model.Message;

public class OutputProcessor implements Runnable {
    private String queueUrl;
    private Manager manager;
    private String output;
    private boolean done = false;


    public OutputProcessor(String queueUrl, Manager manager, String output) {
        this.queueUrl = queueUrl;
        this.manager = manager;
        this.output = output;
    }

    public void run() {
        while(!done){
            Message message = manager.readMessagesLookForFirstLine("PROCEEED", queueUrl);
            output += message.getBody();
            //TODO WHEN IS DONE?!?!
        }
    }
}
