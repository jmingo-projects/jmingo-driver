package org.jmingo.mongo.client;

import org.bson.BSONObject;
import org.jmingo.mongo.client.transport.ConnectionPool;
import org.jmingo.mongo.client.transport.Dispatcher;
import org.jmingo.mongo.client.transport.ReadQueue;
import org.jmingo.mongo.client.transport.Response;
import org.jmingo.mongo.client.transport.WriteQueue;
import org.jmingo.mongo.marshalling.BasicBSONParser;
import org.jmingo.mongo.protocol.Flags;
import org.jmingo.mongo.protocol.MsgHeader;
import org.jmingo.mongo.protocol.OpCode;
import org.jmingo.mongo.protocol.QueryMessage;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by dmgcodevil on 19.09.2014.
 */
public class MongoClient {

    private final String dbName;
    private final String collectionName;
    private final InetSocketAddress inetSocketAddress;

    private final Dispatcher dispatcher;
    private final Thread dispatcherThread;
    private final WriteQueue writeQueue;
    private final ReadQueue readQueue;
    private final ConnectionPool connectionPool;

    private final AtomicInteger requestIdGenerator = new AtomicInteger(0);


    private ExecutorService handlers = Executors.newFixedThreadPool(100);

    public MongoClient(String dbName, String collectionName, InetSocketAddress inetSocketAddress) {
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.inetSocketAddress = inetSocketAddress;

         connectionPool = new ConnectionPool();
        connectionPool.create(inetSocketAddress);
        writeQueue = new WriteQueue();
        readQueue = new ReadQueue();
        dispatcher = new Dispatcher(writeQueue, readQueue, connectionPool);
        dispatcherThread = new Thread(dispatcher);
        dispatcherThread.start();
    }


    public BSONObject findOne(Long id) {
        QueryMessage queryMessage = createFindOneMessage(id);
        writeQueue.put(queryMessage);
        Response response;
        try {
            response = handlers.submit(new ResponseHandler(readQueue, queryMessage.getMsgHeader().getRequestID())).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        return new BasicBSONParser().parse(response.getBuffer().array());
    }


    private static final class ResponseHandler implements Callable<Response> {
        private ReadQueue readQueue;
        private int responseId;

        private ResponseHandler(ReadQueue readQueue, int responseId) {
            this.readQueue = readQueue;
            this.responseId = responseId;
        }

        @Override
        public Response call() throws Exception {
            try {
                return readQueue.get(responseId);

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void dispose() {
        dispatcherThread.interrupt();
        connectionPool.close();

    }

    private QueryMessage createFindOneMessage(Long id) {
        MsgHeader msgHeader = new MsgHeader();
        msgHeader.setRequestID(requestIdGenerator.incrementAndGet());
        msgHeader.setOpCode(OpCode.OP_QUERY);
        QueryMessage queryMessage = new QueryMessage();
        queryMessage.setMsgHeader(msgHeader);
        queryMessage.setFlags(Flags.RESERVED);
        queryMessage.setFullCollectionName(dbName + "." + collectionName);
        Map<String, Object> query = new LinkedHashMap<String, Object>();
        query.put("_id", id);

        queryMessage.setQuery(query);
        return queryMessage;
    }
}
