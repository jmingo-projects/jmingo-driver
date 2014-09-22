package org.jmingo.mongo.marshalling;

/**
 * Common interface for parsing.
 *
 * @author Raman_Pliashkou
 */
public interface BSONParser<T> {

    T parse(byte[] data);
}
