import com.amazonaws.services.sqs.model.Message;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class OutputProcessor implements Runnable {
    private String queueUrl;
    private Manager manager;
    private AtomicReference<String> output;
    private Message message;
    private AtomicInteger messageCount;


    public OutputProcessor(String queueUrl, Manager manager, AtomicReference<String> output, Message message, AtomicInteger messageCount) {
        this.queueUrl = queueUrl;
        this.manager = manager;
        this.output = output;
        this.message = message;
        this.messageCount = messageCount;
    }

    public void run() {
        String current;
        String updated;

        do {
            current = output.get();
            updated = current + "\n" + message.getBody();
        } while (!output.compareAndSet(current, updated));


        int currentCount;
        int updatedCount;
        do {
            currentCount = messageCount.get();
            updatedCount = currentCount++;
        } while (!messageCount.compareAndSet(currentCount, updatedCount));


        manager.deleteMessage(message, queueUrl);
    }
}
