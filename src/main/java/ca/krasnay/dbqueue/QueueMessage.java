package ca.krasnay.dbqueue;

import org.joda.time.DateTime;
import org.joda.time.Period;

/**
 * Message in a message queue.
 *
 * @author <a href="mailto:john@krasnay.ca">John Krasnay</a>
 */
public class QueueMessage {

    public enum Status {
        PENDING,
        FAILED;
    }

    private int id;

    private int version;

    public int getId() {
        return id;
    }

    public int getVersion() {
        return version;
    }

    /**
     * Returns true if this entity has not yet been inserted into the database.
     */
    public boolean isNew() {
        return id == 0;
    }

    /**
     * Flag indicating whether the message has been claimed by a thread.
     */
    private boolean claimed;

    /**
     * Timestamp when the message was claimed. Used to determine whether a
     * message has been orphaned.
     */
    private DateTime claimedTimestamp;

    /**
     * Timestamp when the message was first queued.
     */
    private DateTime createdTimestamp;

    /**
     * Number of delivery attempts for this message.
     */
    private int deliveryAttempts;

    /**
     * Human-readable description of the message.
     */
    private String description;

    /**
     * Timestamp when the last processing error occurred.
     */
    private DateTime errorTimestamp;

    /**
     * Message from the exception from the last processing error.
     */
    private String errorMessage;

    /**
     * Message payload.
     */
    private String payload;

    /**
     * Message priority. Defaults to zero. More positive values are higher
     * priority and will be processed before others, all things being equal.
     */
    private int priority;

    /**
     * Timestamp before which this message should not be processed. Used for
     * delayed executions and retries. If null, the message can be processed
     * any time.
     */
    private DateTime processAfter;

    /**
     * Name of the queue to which the message belongs.
     */
    private String queueName;

    /**
     * Stack trace of the last error message.
     */
    private String stackTrace;

    private Status status = Status.PENDING;

    public QueueMessage() {

    }

    public QueueMessage(String queueName, String payload) {
        this.queueName = queueName;
        this.payload = payload;
    }

    public QueueMessage claim() {
        claimed = true;
        claimedTimestamp = new DateTime();
        return this;
    }

    public DateTime getClaimedTimestamp() {
        return claimedTimestamp;
    }

    public DateTime getCreatedTimestamp() {
        return createdTimestamp;
    }

    public int getDeliveryAttempts() {
        return deliveryAttempts;
    }

    public String getDescription() {
        return description;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public DateTime getErrorTimestamp() {
        return errorTimestamp;
    }

    public String getPayload() {
        return payload;
    }

    public int getPriority() {
        return priority;
    }

    public DateTime getProcessAfter() {
        return processAfter;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getStackTrace() {
        return stackTrace;
    }

    public Status getStatus() {
        return status;
    }

    public boolean isClaimed() {
        return claimed;
    }

    public void setClaimed(boolean claimed) {
        this.claimed = claimed;
    }

    public void setClaimedTimestamp(DateTime claimedTimestamp) {
        this.claimedTimestamp = claimedTimestamp;
    }

    public void setCreatedTimestamp(DateTime createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    public void setDeliveryAttempts(int deliveryAttempts) {
        this.deliveryAttempts = deliveryAttempts;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public void setErrorTimestamp(DateTime errorTimestamp) {
        this.errorTimestamp = errorTimestamp;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public QueueMessage setPriority(int priority) {
        this.priority = priority;
        return this;
    }

    public QueueMessage setProcessAfter(DateTime processAfter) {
        this.processAfter = processAfter;
        return this;
    }

    public QueueMessage setProcessAfter(Period period) {
        this.processAfter = new DateTime().plus(period);
        return this;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public void setStackTrace(String stackTrace) {
        this.stackTrace = stackTrace;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

}
