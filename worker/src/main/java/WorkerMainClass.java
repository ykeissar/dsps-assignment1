import com.amazonaws.services.sqs.model.Message;

public class WorkerMainClass {
    public static void main(String[] args) {
        Worker worker = new Worker(args[0]);

        while (true) {
            Message message = worker.readMessagesLookForFirstLine("UNPROCESSED");
            if (message != null) {
                String content = message.getBody().substring(message.getBody().indexOf("\n"));
                String id = content.substring(1, content.indexOf("\n", 1));
                String processedReview = worker.processReview(content);//TODO finish processReview and CHANGE METHOD CALLED HERE
                worker.sendMessage("PROCESSED\n" + id + "\n" + processedReview);
                worker.deleteMessage(message);
            }
        }
    }
}
