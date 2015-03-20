package ca.krasnay.dbqueue;

import static ca.krasnay.sqlbuilder.Predicates.eq;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import ca.krasnay.sqlbuilder.Supplier;
import ca.krasnay.sqlbuilder.orm.Mapping;
import ca.krasnay.sqlbuilder.orm.OptimisticLockException;
import ca.krasnay.sqlbuilder.orm.OrmConfig;

@Transactional
public class QueueMessageDaoImpl implements QueueMessageDao {

    private static final Logger log = LoggerFactory.getLogger(QueueMessageDaoImpl.class);

    private Mapping<QueueMessage> mapping;

    private Supplier<Integer> sequence;

    public QueueMessageDaoImpl(OrmConfig ormConfig) {

        mapping = new Mapping<QueueMessage>(ormConfig, QueueMessage.class, "QueueMessage")
        .setIdColumn("id")
        .setVersionColumn("version")
        .addFields();

        sequence = ormConfig.getSequence("queuemessage_id_seq");

    }

    @Override
    public QueueMessage claim(String queueName) {

        while (true) {

            // TODO this might be a problem with a very large backlog
            // However, we would need a limit clause on the createQuery()

            List<QueueMessage> messages = mapping
            .findWhere(eq("queueName", queueName))
            .and("status = 'PENDING'")
            .and("not claimed")
            .and("(processAfter is null or processAfter < current_timestamp)")
            .orderBy("priority", false)
            .orderBy("random()")
            //      .limit(10)
            .getResultList();

            if (messages.size() > 0) {
                try {
                    return mapping.update(messages.get(0).claim());
                } catch (OptimisticLockException e) {
                    // message claimed by other thread
                    // short nap and try again
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e1) {
                    }
                }
            } else {
                return null;
            }

        }

    }

    public void delete(QueueMessage queueMessage) {
        log.info("deleting message {} from queue {}", queueMessage.getId(), queueMessage.getQueueName());
        mapping.deleteById(queueMessage.getId());
    }

    @Override
    public void delete(List<Integer> ids) {
        for (int id : ids) {
            log.info("deleting message {}", id);
            mapping.deleteById(id);
        }
    }

    @Override
    public QueueMessage findById(int id) {
        log.info("finding message {}", id);
        return mapping.findById(id);
    }

    @Override
    public QueueMessage insert(QueueMessage queueMessage) {
        queueMessage.setId(sequence.get());
        log.info("inserting message {} into queue '{}'", queueMessage.getId(), queueMessage.getQueueName());
        mapping.insert(queueMessage);
        return queueMessage;
    }

    public QueueMessage update(QueueMessage queueMessage) {
        log.info("updating message {} in queue {}", queueMessage.getId(), queueMessage.getQueueName());
        return mapping.update(queueMessage);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @Override
    public QueueMessage updateNewTx(QueueMessage queueMessage) {
        return update(queueMessage);
    }
}
