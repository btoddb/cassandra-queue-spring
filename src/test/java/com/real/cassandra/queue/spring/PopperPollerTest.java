package com.real.cassandra.queue.spring;

import static org.junit.Assert.assertEquals;

import javax.annotation.Resource;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.integration.MessageChannel;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.PlatformTransactionManager;

import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.PopperImpl;
import com.real.cassandra.queue.PusherImpl;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
        "classpath:spring-cassandra-channels.xml", "classpath:spring-cassandra-queues.xml", "classpath:spring-cassandra-txmgr.xml"

})
@Ignore
public class PopperPollerTest extends CassQueueSpringTestBase {

    @Resource(name = "testQueue")
    CassQueueImpl cq;
    
    @Resource(name = "testPopChannel")
    MessageChannel outputChannel;

    @Resource(name = "cassQueueTxMgr")
    PlatformTransactionManager txMgr;

    @Test
    @DirtiesContext
    public void testPopSuccess() {
        PusherImpl pusher = cq.createPusher();
        PopperImpl popper = cq.createPopper();
        long qDepth = cq.getQueueDepth();
        
        pusher.push("test".getBytes());

        assertEquals( qDepth+1, cq.getQueueDepth());
        
        PopperPoller pp = new PopperPoller(popper, cq, outputChannel, 1L);
        pp.run();
        
        assertEquals( qDepth, cq.getQueueDepth());
    }

    @Test
    @DirtiesContext
    public void testPopException() {
        PusherImpl pusher = cq.createPusher();
        PopperImpl popper = cq.createPopper();
        long qDepth = cq.getQueueDepth();
        
        pusher.push("test".getBytes());

        assertEquals( qDepth+1, cq.getQueueDepth());
        
        PopperPoller pp = new PopperPoller(popper, cq, outputChannel, 1L);
        pp.run();
        
        assertEquals( qDepth, cq.getQueueDepth());
    }

    // -----------------------

    @Before
    public void setupTest() throws Exception {
        cq.truncate();
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
