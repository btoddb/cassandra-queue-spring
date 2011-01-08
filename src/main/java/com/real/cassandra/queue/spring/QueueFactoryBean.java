package com.real.cassandra.queue.spring;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;

import com.real.cassandra.queue.CassQueueFactoryImpl;
import com.real.cassandra.queue.CassQueueImpl;

/**
 * Spring factory bean used to create {@link CassQueueImpl} type queues.
 * 
 * @author Todd Burruss
 */
public class QueueFactoryBean implements FactoryBean<CassQueueImpl>, DisposableBean {
    private String qName;
    private boolean distributed = false;
    private CassQueueFactoryImpl cqFactory;
    private int maxPushesPerPipe;
    private long maxPushTimeOfPipe;
    private long transactionTimeout;
    private CassQueueImpl cassQueue;

    public QueueFactoryBean(String qName, CassQueueFactoryImpl cqFactory) {
        this.qName = qName;
        this.cqFactory = cqFactory;
    }

    @Override
    public CassQueueImpl getObject() throws Exception {
        cassQueue =
                cqFactory.createInstance(qName, maxPushTimeOfPipe, maxPushesPerPipe, transactionTimeout, distributed);
        return cassQueue;
    }

    @Override
    public Class<?> getObjectType() {
        return CassQueueImpl.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setqName(String qName) {
        this.qName = qName;
    }

    public void setDistributed(boolean distributed) {
        this.distributed = distributed;
    }

    public void setMaxPushesPerPipe(int maxPushesPerPipe) {
        this.maxPushesPerPipe = maxPushesPerPipe;
    }

    public void setMaxPushTimeOfPipe(long maxPushTimeOfPipe) {
        this.maxPushTimeOfPipe = maxPushTimeOfPipe;
    }

    public void setTransactionTimeout(long transactionTimeout) {
        this.transactionTimeout = transactionTimeout;
    }

    @Override
    public void destroy() throws Exception {
        if (null != cassQueue) {
            cassQueue.shutdownAndWait();
        }
    }

}
