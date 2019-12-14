import com.amazonaws.services.sqs.model.Message;

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

    public OutputHandler(String queueUrl, Manager manager, int id, int count) {
        this.queueUrl = queueUrl;
        this.manager = manager;
        this.id = id;
        expectedMessageCount = count;

    }

    public void run() {
        //reads all from queue //TODO think how to verify all reviews was processed
        Message message = null;

        do {
            message = manager.readMessagesLookForFirstLine("PROCESS\n", queueUrl);//TODO think how not to process same message twice
            readersPool.execute(new OutputProcessor(queueUrl, manager, output, message, currentMessageCount));

        } while (message != null && currentMessageCount.get() < expectedMessageCount);

        //uploading
        manager.uploadOutputFile(manager.getBucketName(id), output.get(), id);
    }
}
