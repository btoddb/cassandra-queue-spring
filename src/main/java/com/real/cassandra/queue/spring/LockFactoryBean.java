package com.real.cassandra.queue.spring;

import java.util.Properties;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;

import com.real.cassandra.queue.Descriptor;
import com.real.cassandra.queue.locks.CagesLockerImpl;
import com.real.cassandra.queue.locks.LocalLockerImpl;
import com.real.cassandra.queue.locks.Locker;

/**
 * Factory to create either a local or distributed locker instance. If the property "lockMode" is set to the value
 * "distributed", additional properties are used to create a Zookeeper cages write lock object. The alternative
 * is to create a "local" which is sufficient for synchronizing write operation within a single JVM.
 *
 * @author Andrew Ebaugh
 */
public class LockFactoryBean<M extends Descriptor> implements FactoryBean<Locker<M>>, InitializingBean {

    private final static String LOCK_MODE = "lockMode";

    private final static String LOCK_DIST = "distributed";
    private final static String LOCK_LOCAL = "local";    

    private final static String ZK_CONNECT_STRING = "zk.connectString";
    private final static String ZK_SESSION_TIMEOUT = "zk.sessionTimeout";
    private final static String ZK_MAX_CONNECT_ATTEMPTS = "zk.maxConnectionAttempts";


    private Locker<M> lockerInstance;
    private Properties lockProperties;
    private String lockPath;

    @Override
    public void afterPropertiesSet() throws Exception {
        if(lockProperties != null && !lockProperties.isEmpty() && LOCK_DIST.equals(lockProperties.getProperty(LOCK_MODE))) {
         lockerInstance = new CagesLockerImpl<M>(
                 getLockPath(),
                 lockProperties.getProperty(ZK_CONNECT_STRING),
                 Integer.valueOf(lockProperties.getProperty(ZK_SESSION_TIMEOUT)),
                 Integer.valueOf(lockProperties.getProperty(ZK_MAX_CONNECT_ATTEMPTS)));
        }
        else {
            lockerInstance = new LocalLockerImpl<M>();
        }
    }

    @Override
    public Locker<M> getObject() throws Exception {
        return lockerInstance;
    }

    @Override
    public Class<Locker> getObjectType() {
        return Locker.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Required
    public void setLockProperties(Properties lockProperties) {
        this.lockProperties = lockProperties;
    }

    @Required
    public void setLockPath(String lockPath) {
        this.lockPath = lockPath;
    }

    public String getLockPath() {
        return lockPath;
    }
}
