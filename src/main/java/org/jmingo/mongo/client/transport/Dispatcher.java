package org.jmingo.mongo.client.transport;

import org.jmingo.mongo.protocol.QueryMessage;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Class description.
 *
 * @author dmgcodevil
 */
public class Dispatcher implements Runnable {

    private WriteQueue writeQueue;
    private ReadQueue readQueue;
    private ExecutorService workers = Executors.newFixedThreadPool(1);
    private ConnectionPool connectionPool;

    public Dispatcher(WriteQueue writeQueue, ReadQueue readQueue, ConnectionPool connectionPool) {
        this.writeQueue = writeQueue;
        this.readQueue = readQueue;
        this.connectionPool = connectionPool;
    }

    @Override
    public void run() {
        while (connectionPool.getConnection().isOpen()) {
            QueryMessage queryMessage = writeQueue.getQueryMessage();
            if (queryMessage != null) {
                workers.submit(new Worker(connectionPool, queryMessage, readQueue));
            }
        }

        workers.shutdown();
        System.out.println("stop Dispatcher");
        try {
            workers.awaitTermination(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.exit(0);
    }
}
