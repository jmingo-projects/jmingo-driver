package org.jmingo.mongo.protocol;

import com.google.common.primitives.Bytes;
import org.jmingo.mongo.client.utils.BsonUtils;

import java.nio.ByteBuffer;
import java.util.List;

/*
Database Response Messages.

The OP_REPLY message is sent by the database in response to an OP_QUERY or OP_GET_MORE message.
The format of an OP_REPLY message is:
struct {
        MsgHeader header;         // standard message header
        int32     responseFlags;  // bit vector - see details below
        int64     cursorID;       // cursor id if client needs to do get more's
        int32     startingFrom;   // where in the cursor this reply is starting
        int32     numberReturned; // number of documents in the reply
        document* documents;      // documents
        }
*/
public class OpReply {

    private final byte[] response;
    private MsgHeader msgHeader;
    private int responseFlags;
    private long cursorID;
    private int startingFrom;
    private int numberReturned;

    public static final int META_DATA_SIZE =
            16 // header
                    + 4 // responseFlags
                    + 8 // cursorID
                    + 4 // startingFrom
                    + 4; // numberReturned

    public OpReply(byte[] response) {
        this.response = response;
        readMetaData();
        //readDocuments();
    }

    private void readMetaData() {
        //todo: read header, responseFlags and etc.
        msgHeader = new MsgHeader(response);
        int read = MsgHeader.SIZE;
        responseFlags = BsonUtils.readInt(response, read);
        read += 4;
        cursorID = BsonUtils.readLong(response, read);
        read += 8;
        startingFrom = BsonUtils.readInt(response, read);
        read += 4;
        numberReturned = BsonUtils.readInt(response, read);

    }


    public void writeDocuments(List<Byte> fullDocument) {
        int offset = META_DATA_SIZE;
        byte[] documentsBytes = new byte[response.length - offset];
        ByteBuffer.wrap(response, offset, response.length - offset).get(documentsBytes);
        fullDocument.addAll(Bytes.asList(documentsBytes));
    }

    public MsgHeader getMsgHeader() {
        return msgHeader;
    }

    public int getResponseFlags() {
        return responseFlags;
    }

    public long getCursorID() {
        return cursorID;
    }

    public int getStartingFrom() {
        return startingFrom;
    }

    public int getNumberReturned() {
        return numberReturned;
    }

    @Override
    public String toString() {
        return "OpReply{" +
                ", msgHeader=" + msgHeader +
                ", responseFlags=" + responseFlags +
                ", cursorID=" + cursorID +
                ", startingFrom=" + startingFrom +
                ", numberReturned=" + numberReturned +
                '}';
    }
}
