package org.jmingo.mongo.client.transport;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * Class description.
 *
 * @author dmgcodevil
 */
public class Acceptor implements Runnable {

    private Selector selector;

    private int timeout = 1000;
    private int maxRetries = 20;
    private int retries = 0;
    private boolean connected;


    public Acceptor(Selector selector) {
        this.selector = selector;
    }

    @Override
    public void run() {
        try {
            while (!connected) {

                if (retries == maxRetries) {
                    throw new RuntimeException("cannot connect to the server");
                }

                selector.select(timeout);
                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();

                    if (!key.isValid()) continue;

                    if (key.isConnectable()) {

                        connect(key);
                    }
                }
                retries++;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private void connect(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        if (channel.isConnectionPending()) {
            channel.finishConnect();
        }
        channel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_WRITE);
        connected = true;
    }

}
