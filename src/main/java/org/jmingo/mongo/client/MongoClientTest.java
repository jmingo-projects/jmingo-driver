package org.jmingo.mongo.client;

import org.bson.BSONObject;
import org.jmingo.mongo.client.MongoClient;

import java.net.InetSocketAddress;

/**
 * Class description.
 *
 * @author dmgcodevil
 */
public class MongoClientTest {

    public static void main(String[] args) {
        MongoClient mongoClient = new MongoClient("vehicle", "ModelYear", new InetSocketAddress("localhost", 27017));
        System.out.println(getId(mongoClient.findOne(100520387L)));
        System.out.println(getId(mongoClient.findOne(100519541L)));
        System.out.println(getId(mongoClient.findOne(100503318L)));
        mongoClient.dispose();
    }

    private static long getId(BSONObject bsonObject) {
        return Long.parseLong(bsonObject.get("_id").toString());
    }
}
