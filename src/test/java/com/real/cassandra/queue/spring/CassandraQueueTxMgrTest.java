package com.real.cassandra.queue.spring;

import static org.junit.Assert.assertEquals;

import javax.annotation.Resource;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionSystemException;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.PopperImpl;
import com.real.cassandra.queue.PusherImpl;

/**
 * Test the spring transaction manager for Cassandra Queue.
 * 
 * @author Todd Burruss
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
        "classpath:spring-cassandra-queues.xml", "classpath:spring-cassandra-txmgr.xml"

})
// this doesn't work for the exception testing classes below.  need to have it on each method??
//@DirtiesContext
public class CassandraQueueTxMgrTest extends CassQueueSpringTestBase {
    @Resource(name = "testQueue")
    CassQueueImpl cq;

    @Resource(name = "cassQueueTxMgr")
    PlatformTransactionManager txMgr;

    @Test
    @DirtiesContext
    public void testTxCommit() throws Exception {
        PusherImpl pusher = cq.createPusher();
        PopperImpl popper = cq.createPopper();

        long qDepth = cq.getQueueDepth();

        // push test msg
        pusher.push("test".getBytes());
        assertEquals(qDepth + 1, cq.getQueueDepth());

        // setup TX - probably some refactoring needed so don't need to call
        // CassandraQueueTxMgr.setMessageOnThread, but can't push spring
        // specific TX mgmt into cassandra-queue
        TransactionStatus txStatus = txMgr.getTransaction(null);
        CassQMsg qMsg = popper.pop();
        CassandraQueueTxMgr.setMessageOnThread(cq, qMsg);

        assertEquals(qDepth, cq.getQueueDepth());

        txMgr.commit(txStatus);
        assertEquals(qDepth, cq.getQueueDepth());
    }

    @Test
    @DirtiesContext
    public void testTxCommitNoMsg() throws Exception {
        // PusherImpl pusher = cq.createPusher();
        // PopperImpl popper = cq.createPopper();

        long qDepth = cq.getQueueDepth();

        // push test msg
        // pusher.push("test".getBytes());
        // assertEquals(qDepth + 1, cq.getQueueDepth());

        // setup TX - probably some refactoring needed so don't need to call
        // CassandraQueueTxMgr.setMessageOnThread, but can't push spring
        // specific TX mgmt into cassandra-queue
        TransactionStatus txStatus = txMgr.getTransaction(null);
        // CassQMsg qMsg = popper.pop();
        // CassandraQueueTxMgr.setMessageOnThread(cq, qMsg);

        // assertEquals(qDepth, cq.getQueueDepth());

        txMgr.commit(txStatus);
        assertEquals(qDepth, cq.getQueueDepth());
    }

    @Test
    @DirtiesContext
    public void testTxRollback() throws Exception {
        PusherImpl pusher = cq.createPusher();
        PopperImpl popper = cq.createPopper();

        long qDepth = cq.getQueueDepth();

        // push test msg
        pusher.push("test".getBytes());
        assertEquals(qDepth + 1, cq.getQueueDepth());

        // setup TX - probably some refactoring needed so don't need to call
        // CassandraQueueTxMgr.setMessageOnThread, but can't push spring
        // specific TX mgmt into cassandra-queue
        TransactionStatus txStatus = txMgr.getTransaction(null);
        CassQMsg qMsg = popper.pop();
        CassandraQueueTxMgr.setMessageOnThread(cq, qMsg);

        assertEquals(qDepth, cq.getQueueDepth());

        txMgr.rollback(txStatus);
        assertEquals(qDepth + 1, cq.getQueueDepth());
    }

    @Test
    @DirtiesContext
    public void testTxRollbackNoMsg() throws Exception {
        long qDepth = cq.getQueueDepth();

        TransactionStatus txStatus = txMgr.getTransaction(null);

        txMgr.rollback(txStatus);
        assertEquals(qDepth, cq.getQueueDepth());
    }

    @Test(expected = TransactionSystemException.class)
//    @Test
    @DirtiesContext
    public void testTxCommitWithException() throws Exception {
        PusherImpl pusher = cq.createPusher();
        PopperImpl popper = cq.createPopper();

        long qDepth = cq.getQueueDepth();

        // push test msg
        pusher.push("test".getBytes());
        Thread.sleep(1);
        assertEquals(qDepth + 1, cq.getQueueDepth());

        // setup TX - probably some refactoring needed so don't need to call
        // CassandraQueueTxMgr.setMessageOnThread, but can't push spring
        // specific TX mgmt into cassandra-queue
        TransactionStatus txStatus = txMgr.getTransaction(null);
        CassQMsg qMsg = popper.pop();
        CassandraQueueTxMgr.setMessageOnThread(cq, qMsg);

        assertEquals(qDepth, cq.getQueueDepth());

        // force a bad exception even though we have a good msg
        cq.drop();

        txMgr.commit(txStatus);
    }

  @Test(expected = TransactionSystemException.class)
//    @Test
    @DirtiesContext
    public void testTxRollbackWithException() throws Exception {
        PusherImpl pusher = cq.createPusher();
        PopperImpl popper = cq.createPopper();

        long qDepth = cq.getQueueDepth();

        // push test msg
        pusher.push("test".getBytes());
        assertEquals(qDepth + 1, cq.getQueueDepth());

        // setup TX - probably some refactoring needed so don't need to call
        // CassandraQueueTxMgr.setMessageOnThread, but can't push spring
        // specific TX mgmt into cassandra-queue
        TransactionStatus txStatus = txMgr.getTransaction(null);
        CassQMsg qMsg = popper.pop();
        CassandraQueueTxMgr.setMessageOnThread(cq, qMsg);

        assertEquals(qDepth, cq.getQueueDepth());

        // force a bad exception even though we have a good msg
        cq.drop();

        txMgr.rollback(txStatus);
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
