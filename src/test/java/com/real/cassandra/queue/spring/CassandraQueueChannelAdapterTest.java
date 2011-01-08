package com.real.cassandra.queue.spring;

import java.util.Queue;

import javax.annotation.Resource;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.Message;
import org.springframework.integration.MessageChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.spring.mock.MessageConsumer;

import static org.junit.Assert.assertEquals;

/**
 * Test the spring channel adapter, {@link CassandraQueueChannelAdapter}.
 * 
 * @author Todd Burruss
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
        "classpath:spring-cassandra-channels.xml", "classpath:spring-cassandra-queues.xml"

})
@DirtiesContext
public class CassandraQueueChannelAdapterTest extends CassQueueSpringTestBase {
    @Resource(name = "testQueue")
    private CassQueueImpl cq;

    @Autowired
    private MessageConsumer msgReceivedConsumer;

    @Resource(name = "testPushChannel")
    private MessageChannel testPushChannel;
    
    @Resource(name = "cassQueueChannelAdapter")
    CassandraQueueChannelAdapter channelAdapter;

    @Test
    public void testPush() throws Exception {
        int numEvents = 100;
        for (int i = 0; i < numEvents; i++) {
            String msg = "xxx_" + i;
            final Message<byte[]> eventMsg = MessageBuilder.withPayload(msg.getBytes()).build();
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
            String msg = "xxx_" + i;
            channelAdapter.push(msg.getBytes());
        }

        verifyAllPopped(numEvents);
    }

    // -----------------------

    private void verifyAllPopped(int numEvents) throws Exception {
        Thread.sleep(1000);
        int lastNum = -1;
        for (;;) {
            int curNum = msgReceivedConsumer.getMsgs().size();
            if (curNum == lastNum) {
                break;
            }
            else if (0 <= curNum) {
                lastNum = curNum;
            }
            Thread.sleep(200);
        }

        Queue<Message<byte[]>> msgQ = msgReceivedConsumer.getMsgs();
        System.out.println("msgQ = " + outputStringsAsCommaDelim(msgQ));
        assertEquals("Events didn't get on channel properly: " + outputStringsAsCommaDelim(msgQ), numEvents,
                msgQ.size());

        // testUtils.verifyWaitingQueue(0);
        // testUtils.verifyDeliveredQueue(0);
    }

    private String outputStringsAsCommaDelim(Queue<Message<byte[]>> q) {
        StringBuilder sb = new StringBuilder();
        for (Message<byte[]> msg : q) {
            String str = new String(msg.getPayload());
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
        msgReceivedConsumer.getMsgs().clear();
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
