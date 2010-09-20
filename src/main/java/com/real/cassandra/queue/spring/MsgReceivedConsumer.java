package com.real.cassandra.queue.spring;

import java.util.LinkedList;
import java.util.Queue;

public class MsgReceivedConsumer {
    private Queue<String> msgQueue = new LinkedList<String>();

    public void execute(String msg) {
        msgQueue.offer(msg);
    }

    public void clear() {
        msgQueue.clear();
    }

    public Queue<String> getMsgQueue() {
        return msgQueue;
    }
}
