package com.real.cassandra.queue.spring.mock;

import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.springframework.integration.Message;

/**
 * Mock message consumer which can optionally wait a random interval between queueing received messages.
 * 
 * @author Andrew Ebaugh
 */
public class MessageConsumer {

    Queue<Message<byte[]>> msgs = new ConcurrentLinkedQueue<Message<byte[]>>();
    Integer waitRandom = 0;

    public void setWaitRandom(Integer waitRandom) {
        this.waitRandom = waitRandom;
    }

    public void receiveEvent(Message<byte[]> msg) {
        msgs.add(msg);

        if(waitRandom > 0) {
            try {
                Thread.sleep(new Random().nextInt(waitRandom));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public Queue<Message<byte[]>> getMsgs() {
        return msgs;
    }
}
