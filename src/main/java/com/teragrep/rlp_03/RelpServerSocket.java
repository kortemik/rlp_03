package com.teragrep.rlp_03;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

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

    abstract int read(ByteBuffer activeBuffer) throws IOException;

    abstract int write(ByteBuffer responseBuffer) throws IOException;

    public abstract void setSocketId(long socketId);

    public abstract long getSocketId();
}
