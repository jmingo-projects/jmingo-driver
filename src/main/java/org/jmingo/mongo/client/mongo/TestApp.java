package org.jmingo.mongo.client.mongo;


import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * Created by dmgcodevil on 19.09.2014.
 */
public class TestApp {
    public static void main(String[] args) throws UnknownHostException {
        MongoClient mongoClient = new MongoClient("localhost", MongoClientOptions.builder().connectTimeout(1000000000).build());
        DBCollection dbCollection = mongoClient.getDB("driver_test").getCollection("test");
        BasicDBObject query = new BasicDBObject("_id", 1);
        query.put("number", 10);
        System.out.println(dbCollection.find(query, new BasicDBObject("text", 1)).next());
    }
}
