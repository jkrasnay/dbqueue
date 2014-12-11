package ca.krasnay.dbqueue;

import java.util.List;

public interface QueueMessageDao {

    public QueueMessage claim(String queueName);

    public void delete(List<Integer> ids);

    public void delete(QueueMessage entity);

    public QueueMessage findById(int id);

    public QueueMessage insert(QueueMessage entity);

    public QueueMessage update(QueueMessage entity);

    /**
     * Updates the message in a new transaction.
     */
    public QueueMessage updateNewTx(QueueMessage queueMessage);
}
