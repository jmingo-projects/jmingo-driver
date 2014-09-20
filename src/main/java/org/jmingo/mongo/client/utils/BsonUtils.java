package org.jmingo.mongo.client.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.undercouch.bson4jackson.BsonFactory;
import de.undercouch.bson4jackson.BsonModule;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by Inspiron on 20.09.2014.
 */
public class BsonUtils {

    public static byte[] toBytesBson(int n) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        ObjectMapper om = new ObjectMapper(new BsonFactory());
        om.registerModule(new BsonModule());
        try {
            om.writeValue(baos, n);
        } catch (IOException e) {
            e.printStackTrace();
        }

        byte[] r = baos.toByteArray();
        return r;
    }
}
