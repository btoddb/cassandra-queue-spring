package com.real.cassandra.queue.spring;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.apache.cassandra.contrib.utils.service.CassandraServiceDataCleaner;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassQueueSpringTestBase {
    private static Logger logger = LoggerFactory.getLogger(CassQueueSpringTestBase.class);

    private static boolean cassandraStarted = false;

    public static void startCassandraInstance() throws TTransportException, IOException, InterruptedException,
            SecurityException, IllegalArgumentException, NoSuchMethodException, IllegalAccessException,
            InvocationTargetException {
        if (cassandraStarted) {
            return;
        }

        File dd = new File("cassandra-data");
        try {
            FileUtils.deleteRecursive(dd);
        }
        catch (IOException e) {
            logger.debug("exception while deleting data directory, " + dd.getAbsolutePath(), e);
        }
        CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
        cleaner.prepare();
        EmbeddedCassandraService cassandra = new EmbeddedCassandraService();
        cassandra.init();

        cassandraStarted = true;

        Thread t = new Thread(cassandra);
        t.setName(cassandra.getClass().getSimpleName());
        t.setDaemon(true);
        t.start();
        Thread.sleep(1000);
    }

}
