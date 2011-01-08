package com.real.cassandra.queue.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.MessageChannel;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.transaction.TransactionStatus;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.PopperImpl;

/**
 * Polling thread for pop'ing messages off a queue. Also handles wiring in a
 * transaction manager, if appropriate, or committing/rolling back msgs
 * otherwise.
 * 
 * @author Andrew Ebaugh
 * @author Todd Burruss
 */
public class PopperPoller implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(PopperPoller.class);

    private final PopperImpl popper;

    private volatile MessageChannel outputChannel;

    private final MessagingTemplate messagingTemplate = new MessagingTemplate();

    private volatile long maxMessagesPerPoll;

    private CassandraQueueTxMgr txMgr;

    private CassQueueImpl cassQueue;

    public PopperPoller(PopperImpl popper, CassQueueImpl cassQueue, MessageChannel outputChannel,
            Long maxMessagesPerPoll) {

        this.popper = popper;
        this.cassQueue = cassQueue;
        this.outputChannel = outputChannel;
        this.maxMessagesPerPoll = maxMessagesPerPoll;
    }

    public void run() {
        int count = 0;
        while (maxMessagesPerPoll <= 0 || count < maxMessagesPerPoll) {
            boolean workSuccessful = false;
            try {
                if (null != txMgr) {
                    workSuccessful = doWorkWithTxMgmt();
                }
                else {
                    workSuccessful = doWorkWithoutTxMgmt();
                }

                // if work is successful (popped a message) then add to count,
                // otherwise break outta loop
                if (workSuccessful) {
                    count++;
                }
                else {
                    break;
                }
            }
            catch (Throwable e) {
                logger.error("Exception while popping message from queue, " + cassQueue.getName(), e);
            }
        }
    }

    private boolean doWorkWithTxMgmt() throws Throwable {
        TransactionStatus txStatus = txMgr.getTransaction(null);

        CassQMsg qMsg = null;
        try {
            qMsg = popper.pop();
            CassandraQueueTxMgr.setMessageOnThread(cassQueue, qMsg);
            boolean success = sendMsg(qMsg);
            txMgr.commit(txStatus);
            return success;
        }
        catch (Throwable e) {
            txMgr.rollback(txStatus);
            throw e;
        }

    }

    private boolean doWorkWithoutTxMgmt() throws Throwable {
        CassQMsg qMsg = null;
        try {
            qMsg = popper.pop();
            boolean success = sendMsg(qMsg);
            popper.commit(qMsg);
            return success;
        }
        catch (Throwable e) {
            if (null != qMsg) {
                popper.rollback(qMsg);
            }
            throw e;
        }

    }

    private boolean sendMsg(CassQMsg qMsg) {
        if (qMsg != null) {
            byte[] payload = qMsg.getMsgDesc().getPayload();
            MessageBuilder<byte[]> builder = MessageBuilder.withPayload(payload);
            messagingTemplate.send(outputChannel, builder.build());
            return true;
        }
        else {
            return false;
        }
    }

    public void setTxMgr(CassandraQueueTxMgr txMgr) {
        this.txMgr = txMgr;
    }
}
