package org.jmingo.mongo.client.transport;

import java.nio.ByteBuffer;

/**
 * Class description.
 *
 * @author dmgcodevil
 */
public class Response {

    private final int id;
    private final ByteBuffer buffer;
    private final int size;

    public Response(int id, ByteBuffer buffer, int size) {
        this.id = id;
        this.buffer = ByteBuffer.wrap(buffer.array(), 0, size);
        this.size = size;
    }

    public int getId() {
        return id;
    }

    public ByteBuffer getBuffer() {
        return ByteBuffer.wrap(buffer.array());
    }

    public int getSize() {
        return size;
    }
}
