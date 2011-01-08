package com.real.cassandra.queue.spring;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.integration.MessageChannel;
import org.springframework.integration.MessagingException;
import org.springframework.integration.endpoint.AbstractEndpoint;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.support.PeriodicTrigger;
import org.springframework.util.Assert;

import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.PopperImpl;

/**
 * Queue endpoint for populating messages in a channel via pool of pooling message poppers. It also supports stopping
 * and restarting the polling for cases such as when we want to pause the message polling.
 *
 * @author Andrew Ebaugh
 */
public class QueuePollingEndpoint extends AbstractEndpoint {

	private final ReentrantLock schedulingLock = new ReentrantLock();

    private volatile MessageChannel outputChannel;

    private volatile boolean initialized;    

	public static final int MAX_MESSAGES_UNBOUNDED = -1;

	private volatile Trigger trigger = new PeriodicTrigger(10);

	private volatile long maxMessagesPerPoll = MAX_MESSAGES_UNBOUNDED;

    private CassQueueImpl cassQueue;

    private CassandraQueueTxMgr txMgr;

    private Collection<PopperImpl> popperPool;
    private Collection<ScheduledFuture<Runnable>> runningTasks;

    private int numberOfPoppers = 1;

    private int threadsPerPopper = 1;

    public void setTrigger(Trigger trigger) {
        this.trigger = trigger;
    }

    public Trigger getTrigger() {
        return this.trigger;
    }

    /**
     * Set the maximum number of messages to receive for each poll.
     * A non-positive value indicates that polling should repeat as long
     * as non-null messages are being received and successfully sent.
     *
     * <p>The default is unbounded.
     *
     * @see #MAX_MESSAGES_UNBOUNDED
     */
    public void setMaxMessagesPerPoll(long maxMessagesPerPoll) {
        this.maxMessagesPerPoll = maxMessagesPerPoll;
    }

    public long getMaxMessagesPerPoll() {
        return this.maxMessagesPerPoll;
    }

    protected MessageChannel getOutputChannel() {
        return outputChannel;
    }

    public void setOutputChannel(MessageChannel outputChannel) {
        this.outputChannel = outputChannel;
    }

    protected CassQueueImpl getCassQueue() {
        return cassQueue;
    }

    public void setCassQueue(CassQueueImpl cassQueue) {
        this.cassQueue = cassQueue;
    }

    public void setNumberOfPoppers(Integer numberOfPoppers) {
        this.numberOfPoppers = numberOfPoppers;
    }

    public Integer getNumberOfPoppers() {
        return numberOfPoppers;
    }

    public int getThreadsPerPopper() {
        return threadsPerPopper;
    }

    public void setThreadsPerPopper(int threadsPerPopper) {
        this.threadsPerPopper = threadsPerPopper;
    }

    @Override
	protected void onInit() {
        schedulingLock.lock();
		try {
			if(this.initialized) {
				return;
			}

            Assert.notNull(this.cassQueue, "Configured cassandra queue is required");
            try {
                popperPool = new ArrayList<PopperImpl>(numberOfPoppers);
                for(int x=0; x<numberOfPoppers; x++) {
                    popperPool.add(cassQueue.createPopper());
                }

                this.initialized = true;

			}
			catch (Exception e) {
				throw new MessagingException("Failed to create Poller", e);
			}
        }
        finally {
            schedulingLock.unlock();
        }

    }


    @Override
	protected void doStart() {
        startPolling();
	}


    @SuppressWarnings("unchecked")
    public void startPolling() {
        if (!this.initialized) {
            this.onInit();
        }

        Assert.state(this.getTaskScheduler() != null, "Unable to start polling, no taskScheduler available");
        Assert.state(this.popperPool != null && this.popperPool.size() > 0, "Popper pool must be configured");

        schedulingLock.lock();
        try {
            runningTasks = new ArrayList<ScheduledFuture<Runnable>>(popperPool.size()*threadsPerPopper);

            for(PopperImpl popper : popperPool) {
                for(int x=0; x<threadsPerPopper; x++) {
                    runningTasks.add(getTaskScheduler().schedule(createPoller(popper), trigger));
                }
            }
        }
        finally {
            schedulingLock.unlock();
        }
    }

    public Boolean isPolling() {
        schedulingLock.lock();
        try {
            return this.initialized && !runningTasks.isEmpty();
        }
        finally {
            schedulingLock.unlock();
        }
    }


    public void pausePolling() {
        schedulingLock.lock();
        try {

            //cancel any pending tasks
            for(ScheduledFuture<Runnable> scheduled :  runningTasks) {
                if (scheduled != null && !scheduled.isDone()) {
                    scheduled.cancel(true);
                }
            }

            runningTasks.clear();
        }
        finally {
            schedulingLock.unlock();
        }
    }


	@Override
	protected void doStop() {
        schedulingLock.lock();
        try {

            pausePolling();
            this.popperPool.clear();
            this.initialized = false;
        }
        finally {
            schedulingLock.unlock();
        }
	}


	private Runnable createPoller(PopperImpl popper) {
        PopperPoller poller = new PopperPoller(popper, cassQueue, outputChannel, maxMessagesPerPoll);

        if(txMgr != null) {
            poller.setTxMgr(txMgr);
        }

        return poller;
	}


    public void setTxMgr(CassandraQueueTxMgr txMgr) {
        this.txMgr = txMgr;
    }
}
