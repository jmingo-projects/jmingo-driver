package org.jmingo.mongo.protocol;

import org.jmingo.mongo.client.utils.BsonUtils;

import java.nio.ByteBuffer;

/**
 * struct {
 * MsgHeader header;             // standard message header
 * int32     ZERO;               // 0 - reserved for future use
 * cstring   fullCollectionName; // "dbname.collectionname"
 * int32     numberToReturn;     // number of documents to return
 * int64     cursorID;           // cursorID from the OP_REPLY
 * }
 */
public class OpGetMore {

    public static final int BUF_SIZE = 1024 * 16;
    private byte[] msg = new byte[BUF_SIZE];
    private MsgHeader header;
    private int zero;
    private String fullCollectionName;
    private int numberToReturn;
    private long cursorID;

    public MsgHeader getHeader() {
        return header;
    }

    public void setHeader(MsgHeader header) {
        this.header = header;
    }

    public int getZero() {
        return zero;
    }

    public void setZero(int zero) {
        this.zero = zero;
    }

    public String getFullCollectionName() {
        return fullCollectionName;
    }

    public void setFullCollectionName(String fullCollectionName) {
        this.fullCollectionName = fullCollectionName;
    }

    public int getNumberToReturn() {
        return numberToReturn;
    }

    public void setNumberToReturn(int numberToReturn) {
        this.numberToReturn = numberToReturn;
    }

    public long getCursorID() {
        return cursorID;
    }

    public void setCursorID(long cursorID) {
        this.cursorID = cursorID;
    }

    public byte[] toBytes(){
        byte[] requestIDBytes = BsonUtils.toBytesBson(header.getRequestID());
        byte[] responseToBytes = BsonUtils.toBytesBson(header.getResponseTo());
        byte[] opCodeBytes = BsonUtils.toBytesBson(2005);

        byte[] zeroBytes = BsonUtils.toBytesBson(zero);
        byte[] colNameBytes = fullCollectionName.getBytes();
        byte[] numberToReturnBytes = BsonUtils.toBytesBson(numberToReturn);
        byte[] cursorIDnBytes = BsonUtils.toBytesBson(cursorID);

        int messageLength = MsgHeader.SIZE;
        messageLength+=4;
        messageLength+=fullCollectionName.length();
        messageLength+=4;
        messageLength+=8;
        byte[] messageLengthBytes = BsonUtils.toBytesBson(messageLength);
        ByteBuffer byteBuffer = ByteBuffer.allocate(BUF_SIZE);
        byteBuffer.get(messageLengthBytes);
        byteBuffer.get(requestIDBytes);
        byteBuffer.get(responseToBytes);
        byteBuffer.get(opCodeBytes);

        byteBuffer.get(zeroBytes);
        byteBuffer.get(colNameBytes);
        byteBuffer.get(numberToReturnBytes);
        byteBuffer.get(cursorIDnBytes);
        byteBuffer.rewind();
        byteBuffer.put(msg);
        return msg;

    }
}
