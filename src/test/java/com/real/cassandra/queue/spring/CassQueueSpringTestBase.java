package com.real.cassandra.queue.spring;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.apache.cassandra.contrib.utils.service.CassandraServiceDataCleaner;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.transport.TTransportException;

public class CassQueueSpringTestBase {
    private static boolean cassandraStarted = false;

    public static void startCassandraInstance() throws TTransportException, IOException, InterruptedException,
            SecurityException, IllegalArgumentException, NoSuchMethodException, IllegalAccessException,
            InvocationTargetException {
        if (cassandraStarted) {
            return;
        }

        File dd = new File("cassandra-data");
        FileUtils.deleteDirectory(dd);

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
