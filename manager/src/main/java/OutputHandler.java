import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.sqs.model.Message;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class OutputHandler implements Runnable {
    private String processedUrl;
    private String unprocessedUrl;
    private Manager manager;
    private ExecutorService readersPool = Executors.newCachedThreadPool();
    private AtomicReference<String> output = new AtomicReference<String>("");
    private int id;
    private int appId;
    private AtomicInteger currentMessageCount = new AtomicInteger(0);
    private int expectedMessageCount;
    private Map<Integer, Boolean> messagesProcessed;
    private List<Instance> workers;
    private int numOfWorkers;

    public OutputHandler(String processedUrl, String unprocessedUrl, Manager manager, int id, int count, Map<Integer, Boolean> messagesProcessed, List<Instance> workers, int numOfWorkers, int appId) {
        this.processedUrl = processedUrl;
        this.unprocessedUrl = unprocessedUrl;
        this.manager = manager;
        this.id = id;
        expectedMessageCount = count;
        this.messagesProcessed = messagesProcessed;
        this.workers = workers;
        this.numOfWorkers = numOfWorkers;
        this.appId = appId;
    }

    public void run() {
        //reads all from queue
        Message message;

        do {
            message = manager.readMessagesLookForFirstLine("PROCESSED", processedUrl);
            if (message != null) {
                int id = Integer.parseInt(message.getBody().substring(10, message.getBody().indexOf("\n", 10)));
                if (!messagesProcessed.get(id)) {
                    readersPool.execute(new OutputProcessor(processedUrl, manager, output, message, currentMessageCount));
                    messagesProcessed.put(id, true);
                    manager.log("AppId " + appId + " Input n. " + this.id + ", Review n. " + id + " processed. (" + (currentMessageCount.get() + 1) + "/" + expectedMessageCount + ")");
                } else
                    manager.deleteMessage(message, processedUrl);

            }
            if (workers.size() < numOfWorkers) {
                workers.addAll(manager.runNWorkers(processedUrl, unprocessedUrl, numOfWorkers - workers.size(), appId, this.id));
            }
        } while (currentMessageCount.get() < expectedMessageCount);

        //uploading
        manager.uploadOutputFile(manager.getBucketName(id), output.get(), id, appId);

        manager.shutdownWorkers(workers);
        manager.deleteQueue(processedUrl);
        manager.deleteQueue(unprocessedUrl);
        readersPool.shutdown();
    }
}
