import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.sqs.model.Message;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class OutputHandler implements Runnable {
    private String queueUrl;
    private Manager manager;
    private ExecutorService readersPool = Executors.newCachedThreadPool();
    private AtomicReference<String> output = new AtomicReference<String>("");
    private int id;
    private AtomicInteger currentMessageCount = new AtomicInteger(0);
    private int expectedMessageCount;
    private Map<Integer, Boolean> messagesProcessed;
    private List<Instance> workers;

    public OutputHandler(String queueUrl, Manager manager, int id, int count, Map<Integer, Boolean> messagesProcessed, List<Instance> workers) {
        this.queueUrl = queueUrl;
        this.manager = manager;
        this.id = id;
        expectedMessageCount = count;
        this.messagesProcessed = messagesProcessed;
        this.workers=workers;
    }

    public void run() {
        //reads all from queue
        Message message;

        do {
            message = manager.readMessagesLookForFirstLine("PROCESSED", queueUrl);
            if(message != null) {
                int id = Integer.parseInt(message.getBody().substring(10, message.getBody().indexOf("\n", 10)));
                if (!messagesProcessed.get(id)) {
                    readersPool.execute(new OutputProcessor(queueUrl, manager, output, message, currentMessageCount));
                    messagesProcessed.put(id, true);
                }
            }

        } while (currentMessageCount.get() < expectedMessageCount);

        //uploading
        manager.uploadOutputFile(manager.getBucketName(id), output.get(), id);

        manager.shutdownWorkers(workers);
        manager.deleteQueue(queueUrl);
        readersPool.shutdown();
    }
}
