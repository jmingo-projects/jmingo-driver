package org.jmingo.mongo.client.mongo;


import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;

import java.net.UnknownHostException;

/**
 * Created by dmgcodevil on 19.09.2014.
 */
public class TestApp {
    public static void main(String[] args) throws UnknownHostException {
        MongoClient mongoClient = new MongoClient("localhost", MongoClientOptions.builder().connectTimeout(1000000000).build());
        DBCollection dbCollection = mongoClient.getDB("vehicle").getCollection("ModelYear");
        BasicDBObject query = new BasicDBObject("_id", 100520387);
        long startTime = System.currentTimeMillis();
        DBObject objectdb = dbCollection.find(query).next();
        System.out.println(objectdb.keySet());
        long endTime = System.currentTimeMillis();
        System.out.println("total execution time: " + (endTime - startTime));
    }
}
