package com.real.cassandra.queue.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.PopperImpl;
import com.real.cassandra.queue.PusherImpl;

/**
 * Spring channel adapter connecting a spring channel to a {@link CassQueueImpl}
 * .
 *
 * @see #push(String)
 * @see #pop()
 *
 * @author Todd Burruss
 */
public class CassandraQueueChannelAdapter {
    private static Logger logger = LoggerFactory.getLogger(CassandraQueueChannelAdapter.class);

    private CassQueueImpl cassQueue;
    private CassandraQueueTxMgr txMgr;
    private PopperImpl popper;
    private PusherImpl pusher;

    public CassandraQueueChannelAdapter(CassQueueImpl cassQueue, boolean pushOnly) {
        this.cassQueue = cassQueue;
        pusher = cassQueue.createPusher();
        if (!pushOnly) {
            popper = cassQueue.createPopper(true);
        }
    }

    /**
     * Called by channel adapter periodically to grab message from queue and
     * deliver via channel.
     *
     * @return {@link CassQMsg}
     */
    public String pop() {
        CassQMsg qMsg = null;
        try {
            qMsg = popper.pop();
            if (null == qMsg) {
                return null;
            }

            // check for tx mgmt
            if (null != txMgr) {
                CassandraQueueTxMgr.setMessageOnThread(cassQueue, qMsg);
            }
            else {
                // TODO BTB:could simply tell queue to not move to delivered and
                // skip this step for optimization
                popper.commit(qMsg);
            }

            return qMsg.getMsgData();
        }
        // propagate exception up so can be handled by tx mgmt code
        catch (Throwable e) {
            if (null == txMgr && null != qMsg) {
                try {
                    popper.rollback(qMsg);
                }
                catch (Exception e1) {
                    logger.error("exception while rolling back because of a different issue", e);
                }
            }

            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            else {
                throw new RuntimeException(e);
            }
        }
    }

    public void push(String data) {
        try {
            pusher.push(data);
        }
        catch (Throwable e) {
            logger.error("exception while pushing onto queue", e);
        }
    }

    public void setTxMgr(CassandraQueueTxMgr txMgr) {
        this.txMgr = txMgr;
    }
}
