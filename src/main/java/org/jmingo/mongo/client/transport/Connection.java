package org.jmingo.mongo.client.transport;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Class description.
 *
 * @author dmgcodevil
 */
public class Connection {

    private ExecutorService service = Executors.newSingleThreadExecutor();

    private SocketAddress socketAddress;
    private Selector selector;
    private SocketChannel channel;

    public Connection(SocketAddress socketAddress) {
        this.socketAddress = socketAddress;
        try {
            this.selector = Selector.open();
            this.channel = SocketChannel.open();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public boolean isOpen() {
        return selector.isOpen();
    }

    public Selector getSelector() {
        return selector;
    }

    public SocketChannel getChannel() {

        return channel;
    }

}
