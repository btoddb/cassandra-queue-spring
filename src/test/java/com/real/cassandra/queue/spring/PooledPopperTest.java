package com.real.cassandra.queue.spring;

import java.util.Collection;
import java.util.UUID;

import org.junit.Assert;
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
import org.springframework.util.StringUtils;

import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.repository.QueueRepositoryImpl;
import com.real.cassandra.queue.spring.mock.MessageConsumer;

/**
 * @author Andrew Ebaugh
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:pooled-popper-test.xml"})
@DirtiesContext
public class PooledPopperTest extends CassQueueSpringTestBase {

    @Autowired
    private MessageChannel sendChannel;

    @Autowired
    private CassQueueImpl queue;

    @Autowired
    QueueRepositoryImpl qRepos;

    @Autowired
    MessageConsumer stageController;

    @Test
    public void testPushPop() throws Exception {
        int pushCount = 1000;
        UUID[] msgIds = new UUID[pushCount];

        int x = 0;
        for(; x<50; x++) {
            UUID msgId = UUID.randomUUID();
            msgIds[x] = msgId;
            final Message<byte[]> eventMsg = MessageBuilder.withPayload(msgId.toString().getBytes()).build();
            sendChannel.send(eventMsg);
        }

        //sleep to mix things up a little
//        Thread.sleep(50);

        for(; x<100; x++) {
            UUID msgId = UUID.randomUUID();
            msgIds[x] = msgId;
            final Message<byte[]> eventMsg = MessageBuilder.withPayload(msgId.toString().getBytes()).build();
            sendChannel.send(eventMsg);
        }

        //sleep to mix things up a little
//        Thread.sleep(200);

        for(; x<pushCount; x++) {
            UUID msgId = UUID.randomUUID();
            msgIds[x] = msgId;
            final Message<byte[]> eventMsg = MessageBuilder.withPayload(msgId.toString().getBytes()).build();
            sendChannel.send(eventMsg);
        }

        waitForAllMsgsPopped(queue, 10000);

        Collection<Message<byte[]>> recvdMsgs = stageController.getMsgs();
        String[] recvdIds = new String[recvdMsgs.size()];

        int y = 0;
        for(Message<byte[]> recvdMsg : recvdMsgs) {
            recvdIds[y++] = new String(recvdMsg.getPayload());
        }

        System.out.println("Received msg size: "+recvdIds.length);
        System.out.println("Pushed msgs: "+StringUtils.arrayToCommaDelimitedString(msgIds));
        System.out.println("Stage recvd: "+StringUtils.arrayToCommaDelimitedString(recvdIds));

        Assert.assertEquals("Should have popped all the messages",
                msgIds.length,
                recvdIds.length);

    }

    private void waitForAllMsgsPopped(CassQueueImpl cq, long waitTime) throws InterruptedException {
        long start = System.currentTimeMillis();
        while (waitTime > (System.currentTimeMillis() - start) &&
                qRepos.getCountOfWaitingMsgs(cq.getName(), 500).totalMsgCount != 0) {
            Thread.sleep(300);
        }

        System.out.println("waited for msg arrival = " + (System.currentTimeMillis() - start));
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

    @Before
    public void setupTest() {
        try {
            queue.truncate();

            //wait a random amount during message processing
            stageController.setWaitRandom(100);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}
