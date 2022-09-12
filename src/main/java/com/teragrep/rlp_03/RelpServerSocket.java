package com.teragrep.rlp_03;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class RelpServerSocket {
    /*
     * Tries to read incoming requests and changes state to WRITE if responses list
     * has been populated.
     */
    public abstract int processRead(int ops);

    /*
     * Tries to write ready responses into the socket.
     */
    public abstract int processWrite(int ops);

    /**
     * Reads incoming messages from the socketChannel into the given activeBuffer.
     *
     * @param activeBuffer The ByteBuffer to read messages into.
     * @return total read bytes.
     */
    abstract int read(ByteBuffer activeBuffer) throws IOException;

    /**
     * Writes the message in responseBuffer into the socketChannel.
     *
     * @param responseBuffer The ByteBuffer containing the response frame.
     * @return total bytes written.
     */
    abstract int write(ByteBuffer responseBuffer) throws IOException;

    public abstract void setSocketId(long socketId);

    public abstract long getSocketId();
}
