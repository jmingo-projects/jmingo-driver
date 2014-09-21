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

    public static byte[] toBytesBson(long n) {
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

    public static int readInt( byte[] data , int offset ) {
        int x = 0;
        x |= ( 0xFF & data[offset+0] ) << 0;
        x |= ( 0xFF & data[offset+1] ) << 8;
        x |= ( 0xFF & data[offset+2] ) << 16;
        x |= ( 0xFF & data[offset+3] ) << 24;
        return x;
    }

    public static long readLong( byte[] data , int offset ) {
        long x = 0;
        x |= ( 0xFFL & data[offset+0] ) << 0;
        x |= ( 0xFFL & data[offset+1] ) << 8;
        x |= ( 0xFFL & data[offset+2] ) << 16;
        x |= ( 0xFFL & data[offset+3] ) << 24;
        x |= ( 0xFFL & data[offset+4] ) << 32;
        x |= ( 0xFFL & data[offset+5] ) << 40;
        x |= ( 0xFFL & data[offset+6] ) << 48;
        x |= ( 0xFFL & data[offset+7] ) << 56;
        return x;
    }
}
