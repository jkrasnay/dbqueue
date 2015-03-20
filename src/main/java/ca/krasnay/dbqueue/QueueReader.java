package ca.krasnay.dbqueue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Agent that consumes messages from a queue. To process messages, create a
 * subclass that implements the {@link #onMessage(String)} method.
 *
 * @author <a href="mailto:john@krasnay.ca">John Krasnay</a>
 */
public abstract class QueueReader implements InitializingBean, DisposableBean {

    /**
     * Runnable executed by the thread pool. Each instance of this class handles
     * a single queue message.
     */
    private class MessageHandler implements Runnable {

        private QueueMessage queueMessage;

        public MessageHandler(QueueMessage queueMessage) {
            this.queueMessage = queueMessage;
        }

        @Override
        public void run() {
            new TransactionTemplate(transactionManager).execute(new TransactionCallback<Void>() {
                @Override
                public Void doInTransaction(TransactionStatus status) {
                    try {

                        onMessage(queueMessage.getPayload());
                        queueMessageDao.delete(queueMessage);

                    } catch (Exception e) {

                        int retries = queueMessage.getDeliveryAttempts();

                        queueMessage.setClaimed(false);
                        queueMessage.setClaimedTimestamp(null);
                        queueMessage.setDeliveryAttempts(queueMessage.getDeliveryAttempts() + 1);
                        queueMessage.setErrorTimestamp(new DateTime());
                        queueMessage.setErrorMessage(e.getMessage());

                        StringWriter writer = new StringWriter();
                        e.printStackTrace(new PrintWriter(writer));
                        queueMessage.setStackTrace(writer.toString());

                        boolean retry = onError(e, queueMessage);

                        if (retry && retries < maxRetries) {
                            log.warn(String.format("Message %d in queue %s failed, will retry in %d seconds", queueMessage.getId(), queueMessage.getQueueName(), retryWaitSeconds), e);
                            queueMessage.setProcessAfter(Period.seconds(retryWaitSeconds));
                        } else {
                            log.error(String.format("Message %d in queue %s permanently failed", queueMessage.getId(), queueMessage.getQueueName()), e);
                            onFinalError(e, queueMessage);
                            queueMessage.setStatus(QueueMessage.Status.FAILED);
                        }

                        queueMessageDao.updateNewTx(queueMessage);

                        status.setRollbackOnly();

                    }
                    return null;
                }
            });
        }

    }

    private static final Logger log = LoggerFactory.getLogger(QueueReader.class);;

    /**
     * Delay, in seconds, that the reader waits after finding the queue empty.
     */
    private int delay = 5;

    /**
     * Initial delay, in seconds, before the reader starts reading messages
     * from the queue.
     */
    private int initialDelay = 10;

    /**
     * The maximum number of retries.
     */
    private int maxRetries = 0;

    private final QueueMessageDao queueMessageDao;

    private final String queueName;

    private int retryWaitSeconds = 60;

    /**
     * Claims messages from the queue and submits them to the thread pool.
     */
    private ScheduledExecutorService reader;

    private ExecutorService executorService;

    private final PlatformTransactionManager transactionManager;

    public QueueReader(String queueName,
            QueueMessageDao queueMessageDao,
            PlatformTransactionManager transactionManager,
            ExecutorService threadPool) {

        this.queueName = queueName;
        this.queueMessageDao = queueMessageDao;
        this.transactionManager = transactionManager;
        this.executorService = threadPool;
    }

    @Override
    public void afterPropertiesSet() throws Exception {

        log.info("Starting " + getClass().getSimpleName());

        ThreadFactory threadFactory = new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, getName());
            }
        };

        reader = Executors.newSingleThreadScheduledExecutor(threadFactory);

        Runnable claimMessage = new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        QueueMessage message = queueMessageDao.claim(queueName);
                        if (message != null) {
                            log.debug("%s submitting message %d to thread pool", getClass().getName(), message.getId());
                            executorService.submit(new MessageHandler(message));
                        } else {
                            break; // No messages to process at the moment
                        }
                    }
                } catch (Exception e) {
                    log.error(String.format("Error checking queue in %s", getClass().getName()), e);
                }
            }
        };

        reader.scheduleWithFixedDelay(claimMessage, initialDelay, delay, TimeUnit.SECONDS);

    }

    @Override
    public void destroy() throws Exception {

        log.info("Stopping " + getClass().getSimpleName());

        reader.shutdown();

    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    protected String getName() {
        return getClass().getSimpleName();
    }

    public QueueMessageDao getQueueMessageDao() {
        return queueMessageDao;
    }

    /**
     * Error handler. Called when there is an error handling a message. Returns
     * true if the message should be retried. If the message has reached the
     * max retries count, the message is permanently failed regardless of the
     * return value of this method. The default implementation returns true.
     *
     * @param t
     *            Throwable thrown by the message handler.
     * @param message
     *            Message that was being processed.
     */
    protected boolean onError(Throwable t, QueueMessage message) {
        return true;
    }

    /**
     * Error handler that is called when onError returns false or when the
     * message has reached the max retry count.
     *
     * @param t
     *            Throwable thrown by the message handler.
     * @param message
     *            Message that was being processed.
     */
    protected void onFinalError(Throwable t, QueueMessage message) {

    }

    /**
     * Message handler. Must be implemented by a subclass.
     *
     * @param payload
     *            Payload of the message to be processed.
     */
    protected abstract void onMessage(String payload);

    public void setDelay(int delay) {
        this.delay = delay;
    }

    public void setInitialDelay(int initialDelay) {
        this.initialDelay = initialDelay;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public void setRetryWaitSeconds(int retryWaitSeconds) {
        this.retryWaitSeconds = retryWaitSeconds;
    }

}
