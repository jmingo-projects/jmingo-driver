package org.jmingo.mongo.protocol;


/*
In general, each message consists of a standard message header followed by request-specific data.
The standard message header is structured as follows:
struct MsgHeader {
        int32   messageLength; - total message size, including this
        int32   requestID;     - identifier for this message
        int32   responseTo;    - requestID from the original request (used in reponses from db)
        int32   opCode;        - request type - see table below
        }
*/

import org.jmingo.mongo.client.utils.BsonUtils;

public class MsgHeader {
    private int messageLength = 0;
    private int requestID = 0;
    private int responseTo = 0;
    private int opCode = 0;
    public static int SIZE = 16;

    public MsgHeader() {
    }

    public MsgHeader(byte[] data) {
        int read = 0;
        messageLength = BsonUtils.readInt(data, read);
        read += 4;
        requestID = BsonUtils.readInt(data, read);
        read += 4;
        responseTo = BsonUtils.readInt(data, read);
        read += 4;
        opCode = BsonUtils.readInt(data, read);
    }

    public int getMessageLength() {
        return messageLength;
    }

    public void setMessageLength(int messageLength) {
        this.messageLength = messageLength;
    }

    public int getRequestID() {
        return requestID;
    }

    public void setRequestID(int requestID) {
        this.requestID = requestID;
    }

    public int getResponseTo() {
        return responseTo;
    }

    public void setResponseTo(int responseTo) {
        this.responseTo = responseTo;
    }

    public int getOpCode() {
        return opCode;
    }

    public void setOpCode(int opCode) {
        this.opCode = opCode;
    }

    @Override
    public String toString() {
        return "MsgHeader{" +
                "messageLength=" + messageLength +
                ", requestID=" + requestID +
                ", responseTo=" + responseTo +
                ", opCode=" + opCode +
                '}';
    }
}
