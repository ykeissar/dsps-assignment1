import com.amazonaws.services.sqs.model.Message;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class OutputHandler implements Runnable {
    private String queueUrl;
    private Manager manager;
    private ExecutorService readersPool = Executors.newCachedThreadPool();
    private AtomicReference<String> output = new AtomicReference<String>();
    private int id;
    private AtomicInteger currentMessageCount = new AtomicInteger(0);
    private int expectedMessageCount;
    private Map<Integer, Boolean> messagesProcessed;

    public OutputHandler(String queueUrl, Manager manager, int id, int count, Map<Integer, Boolean> messagesProcessed) {
        this.queueUrl = queueUrl;
        this.manager = manager;
        this.id = id;
        expectedMessageCount = count;
        this.messagesProcessed = messagesProcessed;
    }

    public void run() {
        //reads all from queue
        Message message = null;

        do {
            message = manager.readMessagesLookForFirstLine("PROCESSED\n", queueUrl); //TODO think how not to process same message twice
            int id = Integer.parseInt(message.getBody().substring(message.getBody().indexOf("\n"), message.getBody().indexOf("\n",message.getBody().indexOf("\n"))));
            if (!messagesProcessed.get(id)) {
                readersPool.execute(new OutputProcessor(queueUrl, manager, output, message, currentMessageCount));
                messagesProcessed.put(id, true);
            }

        } while (message != null && currentMessageCount.get() < expectedMessageCount);

        //uploading
        manager.uploadOutputFile(manager.getBucketName(id), output.get(), id);

        readersPool.shutdown();
    }
}
