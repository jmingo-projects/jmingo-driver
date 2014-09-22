package org.jmingo.mongo.client.transport;

import org.jmingo.mongo.protocol.OpReply;
import org.jmingo.mongo.protocol.QueryMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Iterator;

/**
 * Class description.
 *
 * @author dmgcodevil
 */
public class Worker implements Runnable {

    private ConnectionPool connectionPool;
    private QueryMessage queryMessage;
    private Selector selector;
    private ReadQueue readQueue;

    public Worker(ConnectionPool connectionPool, QueryMessage queryMessage, ReadQueue readQueue) {
        this.connectionPool = connectionPool;
        this.queryMessage = queryMessage;
        this.readQueue = readQueue;
    }

    @Override
    public void run() {

        selector = connectionPool.getConnection().getSelector();

        //System.out.println("start");
        while (!complete) {
            if (!connectionPool.getConnection().isOpen()) {
                System.out.println("connection has been closed");
                break;
            }
            try {
                selector.select(1000);

                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();

                    if (!key.isValid()) continue;

                    if (key.isWritable()) {
                        write(key);
                    }
                    if (key.isReadable()) {
                        read(key);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    private void close() {
        try {
            selector.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void read(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer readBuffer = ByteBuffer.allocate(64);

        //ByteBuffer readBuffer = ByteBuffer.allocate(1024);
        readBuffer.clear();
        int length;

        try {
            length = channel.read(readBuffer);
        } catch (IOException e) {
            System.out.println("Reading problem, closing connection");
            key.cancel();
            channel.close();
            return;
        }
        if (length == -1) {
            System.out.println("Nothing was read from server");
            channel.close();
            key.cancel();
            return;
        }


        readBuffer.flip();
        byte[] buff = new byte[length];
        readBuffer.get(buff, 0, length);

        if (opReply == null) {

            opReply = new OpReply(buff);
            //System.out.println(opReply);
            messageSize = opReply.getMsgHeader().getMessageLength() - OpReply.META_DATA_SIZE;
            initBuffer(messageSize);
            opReply.writeDocuments(responseBuffer);


        } else {
            responseBuffer.put(buff);
        }


        if (messageSize == responseBuffer.position()) {

            readQueue.put(new Response(opReply.getMsgHeader().getResponseTo(), responseBuffer, messageSize));
            //System.out.println("Finish");
            complete = true;
            channel.register(selector, SelectionKey.OP_WRITE);

        }


    }

    private final Charset UTF8_CHARSET = Charset.forName("UTF-8");

    String decodeUTF8(byte[] bytes) {
        return new String(bytes, UTF8_CHARSET);
    }

    private byte[] prepareData() {
        responseBuffer.flip();
        return responseBuffer.array();
    }


    private void write(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        byte[] msgBytes = queryMessage.getBytes();


        channel.write(ByteBuffer.wrap(msgBytes, 0, msgBytes[0]));

        // lets get ready to read.
        key.interestOps(SelectionKey.OP_READ);
    }

    private void write(SelectionKey key, byte[] msgBytes) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        channel.write(ByteBuffer.wrap(msgBytes, 0, msgBytes[0]));

        // lets get ready to read.
        key.interestOps(SelectionKey.OP_READ);
    }


    private void connect(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        if (channel.isConnectionPending()) {
            channel.finishConnect();
        }
        channel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_WRITE);
    }

    private void initBuffer(int cap) {
        if (responseBuffer == null) {
            responseBuffer = ByteBuffer.allocate(cap);
            responseBuffer.clear();
        }
    }

    private ByteBuffer responseBuffer;
    private volatile int messageSize = 0;
    private volatile boolean complete;
    private OpReply opReply;


}
