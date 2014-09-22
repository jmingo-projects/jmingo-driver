package org.jmingo.mongo.client;

import org.jmingo.mongo.marshalling.BasicBSONParser;
import org.jmingo.mongo.marshalling.EbsonBSONParser;
import org.jmingo.mongo.marshalling.StreamBSONParser;
import org.jmingo.mongo.protocol.Flags;
import org.jmingo.mongo.protocol.MsgHeader;
import org.jmingo.mongo.protocol.OpCode;
import org.jmingo.mongo.protocol.QueryMessage;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;

/**
 * Created by dmgcodevil on 19.09.2014.
 */
public class MongoClient {

    public static void main(String[] args) throws UnknownHostException, InterruptedException {
        Thread.sleep(7000);
        MsgHeader msgHeader = new MsgHeader();
        msgHeader.setRequestID(3); //todo should be auto generated
        msgHeader.setOpCode(OpCode.OP_QUERY);
        QueryMessage queryMessage = new QueryMessage();
        queryMessage.setMsgHeader(msgHeader);
        queryMessage.setFlags(Flags.RESERVED);
        queryMessage.setFullCollectionName("driver_test.test");
        Map<String, Object> query = new LinkedHashMap<String, Object>();
        query.put("_id", 1);

        queryMessage.setQuery(query);
        Thread thread = new Thread(new SocketReader(queryMessage, new BasicBSONParser(),
                new InetSocketAddress("localhost", 27017)));
        thread.start();
    }
}
