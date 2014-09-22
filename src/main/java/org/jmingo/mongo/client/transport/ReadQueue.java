package org.jmingo.mongo.client.transport;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class description.
 *
 * @author dmgcodevil
 */
public class ReadQueue {

    private Map<Integer, Response> responseMap = new ConcurrentHashMap<>();


    public void put(Response response) {
        responseMap.put(response.getId(), response);
    }

    public synchronized Response get(Integer id) throws InterruptedException {
        while (!responseMap.containsKey(id)) {
            wait(100);
        }
        notify();
        return responseMap.get(id);
    }

}
