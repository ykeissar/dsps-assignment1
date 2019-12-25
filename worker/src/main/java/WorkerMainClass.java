import com.amazonaws.services.sqs.model.Message;

public class WorkerMainClass {
    public static void main(String[] args) {
        Worker worker = new Worker(args[0]);        //TODO maybe to 2 queues with manage

        while (true) {
            Message message = worker.readMessagesLookForFirstLine("UNPROCESSED", worker.getQueueUrl());
            if (message != null) {
                String content = message.getBody().substring(message.getBody().indexOf("\n"));
                int id = Integer.parseInt(content.substring(content.indexOf("\n")));
                String processedReview = worker.processReview(content);//TODO finish processReview
                worker.sendMessage("PROCESSED\n" + id + "\n" + processedReview, worker.getQueueUrl());
                worker.deleteMessage(message, worker.getQueueUrl());
            }
        }
    }
}
