package org.jmingo.mongo.client;

import com.google.common.primitives.Bytes;
import org.bson.BSONDecoder;
import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;
import org.jmingo.mongo.protocol.OpGetMore;
import org.jmingo.mongo.protocol.OpReply;
import org.jmingo.mongo.protocol.QueryMessage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by dmgcodevil on 19.09.2014.
 */
public class SocketReader implements Runnable {

    private QueryMessage message;
    private Selector selector;
    private InetSocketAddress socketAddress;
    private OpReply opReply;
    private OpGetMore opGetMore;


    public SocketReader(QueryMessage message, InetSocketAddress socketAddress) {
        this.message = message;
        this.socketAddress = socketAddress;
    }

    @Override
    public void run() {
        SocketChannel channel;
        try {
            selector = Selector.open();
            channel = SocketChannel.open();
            channel.configureBlocking(false);

            channel.register(selector, SelectionKey.OP_CONNECT);
            channel.connect(socketAddress);

            while (!Thread.interrupted()) {

                selector.select(1000);

                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();

                    if (!key.isValid()) continue;

                    if (key.isConnectable()) {
                        System.out.println("I am connected to the server");
                        connect(key);
                    }
                    if (key.isWritable()) {
                        write(key);
                    }
                    if (key.isReadable()) {
                        read(key);
                    }
                }
            }
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } finally {
            close();
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
            System.out.println(opReply);
            opReply.writeDocuments(fullDocument);
            messageSize = opReply.getMsgHeader().getMessageLength() - OpReply.META_DATA_SIZE;
        } else {
            fullDocument.addAll(Bytes.asList(buff));

        }


        if (messageSize == fullDocument.size()) {

            ByteArrayInputStream bais = new ByteArrayInputStream(Bytes.toArray(fullDocument));

            BSONDecoder decoder = new BasicBSONDecoder();
            try {
                BSONObject documents = decoder.readObject(bais);
                System.out.println(documents);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }


    }

    private final Charset UTF8_CHARSET = Charset.forName("UTF-8");

    String decodeUTF8(byte[] bytes) {
        return new String(bytes, UTF8_CHARSET);
    }


    private void write(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        byte[] msgBytes = message.getBytes();
        if (opGetMore != null) {
            msgBytes = opGetMore.toBytes();
        }

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

    private List<Byte> fullDocument = new LinkedList<Byte>(); // todo replace with ByteBuffer
    private volatile int messageSize = 0;

    public static void main(String[] args) {
        System.out.println(Arrays.toString(new String(new char[]{'\0'}).getBytes()));
    }


}