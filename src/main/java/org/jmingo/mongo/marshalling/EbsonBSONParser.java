package org.jmingo.mongo.marshalling;

import com.github.kohanyirobert.ebson.BsonDocument;
import com.github.kohanyirobert.ebson.BsonDocuments;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Implementation based on EBSON library .
 *
 * @author Raman_Pliashkou
 */
public class EbsonBSONParser implements BSONParser<BsonDocument> {
    @Override
    public BsonDocument parse(byte[] data) {
        return BsonDocuments.readFrom(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN));
    }
}
