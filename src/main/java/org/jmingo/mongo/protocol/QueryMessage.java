package org.jmingo.mongo.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.undercouch.bson4jackson.BsonFactory;
import de.undercouch.bson4jackson.BsonModule;
import org.jmingo.mongo.client.utils.BsonUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

/*
The OP_QUERY message is used to query the database for documents in a collection. The format of the OP_QUERY message is:

struct OP_QUERY
{
        MsgHeader header;                 // standard message header
        int32     flags;                  // bit vector of query options.  See below for details.
        cstring   fullCollectionName ;    // "dbname.collectionname"
        int32     numberToSkip;           // number of documents to skip
        int32     numberToReturn;         // number of documents to return in the first OP_REPLY batch
        document  query;                  // query object.  See below for details.
        [ document  returnFieldsSelector; ] // Optional. Selector indicating the fields to return.  See below for details.
}
*/
public class QueryMessage {

    public static final int BUF_SIZE = 1024 * 16;
    private byte[] msg = new byte[BUF_SIZE];

    private MsgHeader msgHeader;
    private int flags = 0;
    private String fullCollectionName;
    private int numberToSkip = 0;
    private int numberToReturn = 0;
    private Map<String, Object> query = new LinkedHashMap<String, Object>();
    private Map<String, Object> returnFieldsSelector = new LinkedHashMap<String, Object>();

    public void setMsgHeader(MsgHeader msgHeader) {
        this.msgHeader = msgHeader;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public void setFullCollectionName(String fullCollectionName) {
        this.fullCollectionName = fullCollectionName;
    }

    public void setNumberToSkip(int numberToSkip) {
        this.numberToSkip = numberToSkip;
    }

    public void setNumberToReturn(int numberToReturn) {
        this.numberToReturn = numberToReturn;
    }

    public void setQuery(Map<String, Object> query) {
        this.query = query;
    }

    public void setFields(Map<String, Object> returnFieldsSelector) {
        this.returnFieldsSelector = returnFieldsSelector;
    }

    public String getFullCollectionName() {
        return fullCollectionName;
    }

    public MsgHeader getMsgHeader() {
        return msgHeader;
    }

    public byte[] getBytes() {

        byte[] flagsBytes = BsonUtils.toBytesBson(flags);
        byte[] colName = prepareCollectionName();
        byte[] skip = BsonUtils.toBytesBson(numberToSkip);
        byte[] returnBytes = BsonUtils.toBytesBson(numberToReturn);
        byte[] queryBytes = prepareQuery();
        byte[] fields = prepareFields();

        byte[] requestIDBytes = BsonUtils.toBytesBson(msgHeader.getRequestID());
        byte[] responseToBytes = BsonUtils.toBytesBson(msgHeader.getResponseTo());
        byte[] opCodeBytes = BsonUtils.toBytesBson(msgHeader.getOpCode());


        ByteBuffer byteBuffer = ByteBuffer.allocate(BUF_SIZE);

        int requestIDBytesSize = requestIDBytes.length;
        int responseToBytesSize = responseToBytes.length;
        int opCodeBytesSize = opCodeBytes.length;

        int flagsBytesSize = 4;
        int colNameSize = colName.length;
        int skipSize = 4;
        int returnBytesSize = 4;
        int queryBytesSize = queryBytes.length;
        int fieldsSize = fields.length;

        int messageLengthSelfSize = 4; // size of messageLength filed from msgHeader
        int messageLength =
                requestIDBytesSize + responseToBytesSize + opCodeBytesSize +
                        flagsBytesSize + colNameSize + skipSize + returnBytesSize + queryBytesSize + fieldsSize;

        messageLength = messageLength + messageLengthSelfSize;

        byteBuffer.put(BsonUtils.toBytesBson(messageLength));
        byteBuffer.put(requestIDBytes);
        byteBuffer.put(responseToBytes);
        byteBuffer.put(opCodeBytes);

        byteBuffer.put(flagsBytes);
        byteBuffer.put(colName);
        byteBuffer.put(skip);
        byteBuffer.put(returnBytes);
        byteBuffer.put(queryBytes);
        byteBuffer.put(fields);
        byteBuffer.rewind();

        byteBuffer.get(msg);


        /**
         * Example of query message in bytes.
         *
         * Query: {"_id" : 1, "number" : 10}
         * Fields: {"text": 1}
         */
        byte[] sigBytes = new byte[]{

                // START message header
                86, 0, 0, 0, // messageLength
                3, 0, 0, 0, // requestID
                0, 0, 0, 0, // responseTo
                -44, 7, 0, 0, // opCode
                // END message header

                // START Query message
                0, 0, 0, 0, // flags
                100, 114, 105, 118, 101, 114, 95, 116, 101, 115, 116, 46, 116, 101, 115, 116, // fullCollectionName
                0, 0, 0, 0, // numberToSkip
                0, 0, 0, 0, // numberToReturn
                0, 26, 0, 0, 0, 16, 95, 105, 100, 0, 1, 0, 0, 0, 16, 110, 117, 109, 98, 101, 114, 0, 10, 0, 0, 0, 0, // query
                15, 0, 0, 0, 16, 116, 101, 120, 116, 0, 1 // selected fields
                // END Query message
        };
        return msg;
    }


    private byte[] prepareQuery() {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        ObjectMapper om = new ObjectMapper(new BsonFactory());
        om.registerModule(new BsonModule());
        try {
            om.writeValue(baos, query);
        } catch (IOException e) {
            e.printStackTrace();
        }

        byte[] r = baos.toByteArray();
        byte[] full = new byte[r.length + 1];
        ByteBuffer buf = ByteBuffer.allocate(full.length);
        buf.put((byte) 0).put(r);
        buf.rewind();
        buf.get(full);

        return full;
    }

    private byte[] prepareFields() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        ObjectMapper om = new ObjectMapper(new BsonFactory());
        om.registerModule(new BsonModule());
        try {
            om.writeValue(baos, returnFieldsSelector);
        } catch (IOException e) {
            e.printStackTrace();
        }

        byte[] r = baos.toByteArray();
        return r;
    }


    private byte[] prepareCollectionName() {
        return fullCollectionName.getBytes();
    }
}
