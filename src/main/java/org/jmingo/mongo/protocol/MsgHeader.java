package org.jmingo.mongo.protocol;


/*
struct MsgHeader {
        int32   messageLength; - total message size, including this
        int32   requestID;     - identifier for this message
        int32   responseTo;    - requestID from the original request (used in reponses from db)
        int32   opCode;        - request type - see table below
        }
*/

public class MsgHeader {
    private int messageLength = 0;
    private int requestID = 0;
    private int responseTo = 0;
    private int opCode = 0;

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

}
