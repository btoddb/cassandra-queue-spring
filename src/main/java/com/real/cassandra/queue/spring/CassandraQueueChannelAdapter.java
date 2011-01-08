package com.real.cassandra.queue.spring;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.PusherImpl;

/**
 * Spring channel adapter connecting a spring push channel to a {@link CassQueueImpl}. Supports creating multiple
 * pushers and iterating through them in a round-robin fashion.
 * 
 *
 * @see #push(String)
 *
 * @author Todd Burruss
 */
public class CassandraQueueChannelAdapter implements InitializingBean {
    private static Logger logger = LoggerFactory.getLogger(CassandraQueueChannelAdapter.class);

    private CassQueueImpl cassQueue;
    private PusherImpl[] pusherPool;

    AtomicInteger pusherIndex  = new AtomicInteger(0);

    public int numPushers = 1;

    public CassandraQueueChannelAdapter(CassQueueImpl cassQueue) {
        this.cassQueue = cassQueue;
    }

    @Override
    public void afterPropertiesSet() throws Exception {

        //create the pushers
        if(numPushers > 0) {
            pusherPool = new PusherImpl[numPushers];
            for(int x=0; x<numPushers; x++) {
                pusherPool[x] = cassQueue.createPusher();
            }
        }
    }

     public void push(byte[] data) {
        try {
            //select pusher
            int idx = getAndIncrement(pusherIndex, numPushers);
            PusherImpl pusher = pusherPool[idx];

            pusher.push(data);
        }
        catch (Throwable e) {
            logger.error("exception while pushing onto queue", e);
        }
    }

    public int getAndIncrement(AtomicInteger counter, int modulo) {
        for (;;) {
            int current = counter.get();
            int next = (current + 1) % modulo;
            if (counter.compareAndSet(current, next)) {
                return current;
            }
        }
    }

    public void setNumPushers(int numPushers) {
        this.numPushers = numPushers;
    }
}
