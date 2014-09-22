package org.jmingo.mongo.client.transport;

import org.jmingo.mongo.protocol.QueryMessage;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Class description.
 *
 * @author dmgcodevil
 */
public class WriteQueue {

    private BlockingDeque<QueryMessage> queryMessages = new LinkedBlockingDeque<>(1000);

    public void put(QueryMessage queryMessage) {
        try {
            queryMessages.put(queryMessage);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public QueryMessage getQueryMessage() {
        try {
            return queryMessages.poll(200, TimeUnit.MICROSECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }
}
