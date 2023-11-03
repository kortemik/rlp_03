package com.teragrep.rlp_03;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;

public class ConnectionEventCompletionService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionEventCompletionService.class);

    SelectionKey call(SelectionKey selectionKey) throws IOException {
        int readyOps = selectionKey.readyOps();

        RelpClientSocket clientRelpSocket = (RelpClientSocket) selectionKey.attachment();

        if (clientRelpSocket == null) {
            throw new RuntimeException("not here"); // FIXME
        }


        /*
        operations are toggled based on the return values of the socket
        meaning: the internal status of the parser.
        */
        int currentOps = selectionKey.interestOps();

        // writes become first
        if ((readyOps & SelectionKey.OP_WRITE) == SelectionKey.OP_WRITE) {
            currentOps = clientRelpSocket.processWrite(currentOps);
        }

        if ((readyOps & SelectionKey.OP_READ) == SelectionKey.OP_READ) {
            //LOGGER.trace("OP_READ @ " + finalThreadId);
            currentOps = clientRelpSocket.processRead(currentOps);
        }


        // FIXME zero ops ok
        if (currentOps != 0) {
            //LOGGER.trace("changing ops: " + currentOps);
            selectionKey.interestOps(currentOps);
        } else {
            // No operations indicates we are done with this one
            //LOGGER.trace("changing ops (closing): " + currentOps);
            try {
                // call close on socket so frameProcessor can cleanup
                clientRelpSocket.close();
            } catch (Exception e) {
                LOGGER.trace("clientRelpSocket.close(); threw", e);
            }
            selectionKey.attach(null);
            selectionKey.channel().close();
            selectionKey.cancel();
        }
        return selectionKey;
    }

}
