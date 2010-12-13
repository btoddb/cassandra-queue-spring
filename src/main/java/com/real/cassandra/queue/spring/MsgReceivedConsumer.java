package com.real.cassandra.queue.spring;

import java.util.LinkedList;
import java.util.Queue;

public class MsgReceivedConsumer {
    private Queue<String> msgQueue = new LinkedList<String>();

    public void execute(byte[] msg) {
        msgQueue.offer(new String(msg));
    }

    public void clear() {
        msgQueue.clear();
    }

    public Queue<String> getMsgQueue() {
        return msgQueue;
    }
}
