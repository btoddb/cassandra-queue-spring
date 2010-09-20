package com.real.cassandra.queue.spring;

import org.springframework.beans.factory.FactoryBean;

import com.real.cassandra.queue.CassQueueFactoryImpl;
import com.real.cassandra.queue.CassQueueImpl;

/**
 * Spring factory bean used to create {@link CassQueueImpl} type queues.
 * 
 * @author Todd Burruss
 */
public class QueueFactoryBean implements FactoryBean<CassQueueImpl> {
    private String qName;
    private boolean distributed = false;
    private CassQueueFactoryImpl cqFactory;
    private int maxPopWidth;
    private int maxPushesPerPipe;
    private long maxPushTimeOfPipe;
    private long popPipeRefreshDelay;

    public QueueFactoryBean(String qName, CassQueueFactoryImpl cqFactory) {
        this.qName = qName;
        this.cqFactory = cqFactory;
    }

    @Override
    public CassQueueImpl getObject() throws Exception {
        return cqFactory.createInstance(qName, maxPushTimeOfPipe, maxPushesPerPipe, maxPopWidth, popPipeRefreshDelay,
                distributed);
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

    public void setMaxPopWidth(int maxPopWidth) {
        this.maxPopWidth = maxPopWidth;
    }

    public void setMaxPushesPerPipe(int maxPushesPerPipe) {
        this.maxPushesPerPipe = maxPushesPerPipe;
    }

    public void setMaxPushTimeOfPipe(long maxPushTimeOfPipe) {
        this.maxPushTimeOfPipe = maxPushTimeOfPipe;
    }

    public void setPopPipeRefreshDelay(long popPipeRefreshDelay) {
        this.popPipeRefreshDelay = popPipeRefreshDelay;
    }

}
