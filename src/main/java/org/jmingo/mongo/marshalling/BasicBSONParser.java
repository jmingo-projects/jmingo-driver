package org.jmingo.mongo.marshalling;

import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;

/**
 * Implementation based on mongodb BasicBSONDecoder.
 *
 * @author Raman_Pliashkou
 */
public class BasicBSONParser implements BSONParser<BSONObject> {
    @Override
    public BSONObject parse(byte[] data) {
        return new BasicBSONDecoder().readObject(data);
    }
}
