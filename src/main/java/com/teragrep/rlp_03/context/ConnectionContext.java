/*
 * Java Reliable Event Logging Protocol Library Server Implementation RLP-03
 * Copyright (C) 2021  Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */

package com.teragrep.rlp_03.context;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import com.teragrep.rlp_03.*;
import com.teragrep.rlp_03.context.channel.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;

/**
 * A per connection object that handles reading and writing messages from and to
 * the SocketChannel.
 */
public class ConnectionContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionContext.class);

    private InterestOps interestOps;

    private final ExecutorService executorService;
    final Socket socket;
    private final RelpRead relpRead;
    final RelpWrite relpWrite;




    private enum RelpState {
        NONE,
        READ,
        WRITE
    }

    private RelpState relpState = RelpState.NONE;

    public ConnectionContext(ExecutorService executorService, Socket socket, Supplier<FrameProcessor> frameProcessorSupplier) {
        this.interestOps = new InterestOpsStub();
        this.executorService = executorService;
        this.socket = socket;
        this.relpRead = new RelpRead(executorService, this, frameProcessorSupplier);
        this.relpWrite = new RelpWrite(this);
    }

    public void updateInterestOps(InterestOps interestOps) {
        this.interestOps = interestOps;
    }

    // TODO remove throw, catch it instead and log
    public void close() throws IOException {
        LOGGER.info("closing");
        //messageReader.close();
        interestOps.removeAll();
        // TODO close socket/channel
    }


    public void handleEvent(SelectionKey selectionKey) throws IOException {

        if (selectionKey.isReadable()) { // perhaps track read/write needs here per direction too
            LOGGER.info("handleEvent taking read");
            interestOps.remove(OP_READ);
            LOGGER.info("handleEvent submitting new runnable for read");
            executorService.submit(relpRead);
            LOGGER.info("handleEvent exiting read");
        }

        if (selectionKey.isWritable()) {
            LOGGER.info("handleEvent taking write");
            interestOps.remove(OP_WRITE);
            LOGGER.info("handleEvent submitting new runnable for write");
            executorService.submit(relpWrite);
            LOGGER.info("handleEvent exiting write");
        }
    }

    InterestOps interestOps() {
        return this.interestOps;
    }
}
