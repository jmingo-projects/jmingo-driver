package org.jmingo.mongo.marshalling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Bytes;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import de.undercouch.bson4jackson.BsonFactory;
import de.undercouch.bson4jackson.BsonGenerator;
import de.undercouch.bson4jackson.BsonModule;
import de.undercouch.bson4jackson.BsonParser;
import org.bson.BSONDecoder;
import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;


/**
 *Implementation based on bson4jackson library.
 *
 * @author Raman_Pliashkou
 */
public class StreamBSONParser implements BSONParser<DBObject> {

    private final static ObjectMapper mapper;

    static {
        BsonFactory fac = new BsonFactory();
        fac.enable(BsonGenerator.Feature.ENABLE_STREAMING);
        fac.enable(BsonParser.Feature.HONOR_DOCUMENT_LENGTH);
        mapper = new ObjectMapper(fac);
        mapper.registerModule(new BsonModule());
    }


    @Override
    public DBObject parse(byte[] data) {
        ByteArrayInputStream bais = createInputStream(data);
        try {
            return mapper.readValue(bais, BasicDBObject.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private ByteArrayInputStream createInputStream(byte[] data) {
        return new ByteArrayInputStream(data);
    }
}
