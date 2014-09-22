package org.jmingo.mongo.client.transport;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * Single connection pool for testing needs.
 *
 * @author dmgcodevil
 */
public class ConnectionPool {
    private Connection connection;

    public void create(SocketAddress socketAddress) {
        Connection connection = new Connection(socketAddress);
        submit(connection);
        open(socketAddress);
    }

    private void submit(Connection connection) {
        this.connection = connection;
    }

    public Connection getConnection() {
        if (connection == null) {
            throw new RuntimeException("no available connections");
        }
        return connection;
    }

    public void open(SocketAddress socketAddress) {
        try {
            final Selector selector = connection.getSelector();
            final SocketChannel channel = connection.getChannel();
            channel.configureBlocking(false);
            channel.register(selector, SelectionKey.OP_CONNECT);
            channel.connect(socketAddress);
            Thread accept = new Thread(new Acceptor(selector));
            accept.start();
            accept.join();
            System.out.println("connected to the server");
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            System.out.println("close connection");
            connection.getSelector().close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
