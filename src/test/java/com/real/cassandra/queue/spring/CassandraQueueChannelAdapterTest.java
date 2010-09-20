package com.real.cassandra.queue.spring;

import static org.junit.Assert.assertEquals;

import java.util.Queue;

import javax.annotation.Resource;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.Message;
import org.springframework.integration.core.MessageBuilder;
import org.springframework.integration.core.MessageChannel;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.PusherImpl;

/**
 * Test the spring channel adapter, {@link CassandraQueueChannelAdapter}.
 * 
 * @author Todd Burruss
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
        "classpath:spring-cassandra-channels.xml", "classpath:spring-cassandra-queues.xml",
        "classpath:spring-config-properties.xml"

})
public class CassandraQueueChannelAdapterTest extends CassQueueSpringTestBase {
    @Resource(name = "testQueue")
    private CassQueueImpl cq;

    @Autowired
    private MsgReceivedConsumer msgReceivedConsumer;

    @Resource(name = "testPushChannel")
    private MessageChannel testPushChannel;

    // @Resource(name = "testPopChannel")
    // private MessageChannel testPopChannel;

    private PusherImpl pusher;

    // private PopperImpl popper;

    @Test
    public void testPush() throws Exception {
        int numEvents = 100;
        for (int i = 0; i < numEvents; i++) {
            final Message<String> eventMsg = MessageBuilder.withPayload("xxx_" + i).build();
            testPushChannel.send(eventMsg);
        }

        verifyAllPopped(numEvents);
    }

    /**
     * Test channel automatically pop'ing from queue and delivering event via
     * channel direct to consumer object.
     * 
     * @throws Exception
     */
    @Test
    public void testPop() throws Exception {
        int numEvents = 100;
        for (int i = 0; i < numEvents; i++) {
            pusher.push("xxx_" + i);
        }

        verifyAllPopped(numEvents);
    }

    // -----------------------

    private void verifyAllPopped(int numEvents) throws Exception {
        Thread.sleep(500);
        int lastNum = -1;
        for (;;) {
            int curNum = msgReceivedConsumer.getMsgQueue().size();
            if (curNum == lastNum) {
                break;
            }
            else if (0 <= curNum) {
                lastNum = curNum;
            }
            Thread.sleep(200);
        }

        Queue<String> msgQ = msgReceivedConsumer.getMsgQueue();
        System.out.println("msgQ = " + outputStringsAsCommaDelim(msgQ));
        assertEquals("Events didn't get on channel properly: " + outputStringsAsCommaDelim(msgQ), numEvents,
                msgQ.size());

        // testUtils.verifyWaitingQueue(0);
        // testUtils.verifyDeliveredQueue(0);
    }

    private String outputStringsAsCommaDelim(Queue<String> q) {
        StringBuilder sb = new StringBuilder();
        for (String str : q) {
            if (0 < sb.length()) {
                sb.append(", ");
            }
            sb.append(str);
        }
        return sb.toString();
    }

    @Before
    public void setupTest() throws Exception {
        cq.truncate();
        pusher = cq.createPusher();
        msgReceivedConsumer.clear();
    }

    @BeforeClass
    public static void setupCassandra() {
        try {
            startCassandraInstance();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
