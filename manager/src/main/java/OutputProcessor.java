import com.amazonaws.services.sqs.model.Message;

import java.util.concurrent.atomic.AtomicReference;

public class OutputProcessor implements Runnable {
    private String queueUrl;
    private Manager manager;
    private AtomicReference<String> output;
    private Message message;


    public OutputProcessor(String queueUrl, Manager manager, AtomicReference<String> output, Message message) {
        this.queueUrl = queueUrl;
        this.manager = manager;
        this.output = output;
        this.message = message;
    }

    public void run() {
        String current = output.get();
        String updated = current + "\n" + message.getBody();
        while (!output.compareAndSet(current, updated)) {
        }

        manager.deleteMessage(message, queueUrl);
    }
}
