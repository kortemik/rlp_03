/*
 * Java Reliable Event Logging Protocol Library Server Implementation RLP-03
 * Copyright (C) 2021, 2024  Suomen Kanuuna Oy
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
package com.teragrep.rlp_03.client;

import com.teragrep.rlp_01.RelpFrameTX;
import com.teragrep.rlp_03.*;
import com.teragrep.rlp_03.config.Config;
import com.teragrep.rlp_03.context.ConnectionContext;
import com.teragrep.rlp_03.context.channel.PlainFactory;
import com.teragrep.rlp_03.context.channel.SocketFactory;
import com.teragrep.rlp_03.delegate.DefaultFrameDelegate;
import com.teragrep.rlp_03.delegate.FrameDelegate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ClientTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientTest.class);

    private final String hostname = "localhost";
    private Server server;
    private final int port = 23601;


    @BeforeAll
    public void init() {
        Config config = new Config(port, 1);
        ServerFactory serverFactory = new ServerFactory(config, () -> new DefaultFrameDelegate((frame) -> LOGGER.debug("server got <[{}]>", frame.relpFrame())));
        Assertions.assertAll(() -> {
            server = serverFactory.create();

            Thread serverThread = new Thread(server);
            serverThread.start();

            server.startup.waitForCompletion();
        });
    }

    @Test
    public void t() throws IOException, InterruptedException {
        ExecutorService executorService = Executors.newCachedThreadPool();
        SocketFactory socketFactory = new PlainFactory();


        ConnectContextFactory connectContextFactory = new ConnectContextFactory(
                executorService,
                socketFactory
        );

        // this is for returning ready connection
        TransferQueue<ConnectionContext> readyContexts = new LinkedTransferQueue<>();
        Consumer<ConnectionContext> connectionContextConsumer = connectionContext -> {
            LOGGER.debug("connectionContext ready");
            readyContexts.add(connectionContext);
        };


        // TODO perhaps <Integer, Future<Something>> ?
        // FIXME what should be Something, RelpFrame is immediately deallocated after FrameDelegate
        // TODO design better: Futures are not optimal for multi-complete/disruptor pattern
        HashMap<Integer, CompletableFuture<String>> pendingTransactions = new HashMap<>();

        FrameDelegate essentialClientDelegate = new FrameDelegate() {
            @Override
            public boolean accept(FrameContext frameContext) {
                LOGGER.debug("client got <[{}]>", frameContext.relpFrame());

                int txn = frameContext.relpFrame().txn().toInt();

                // TODO implement better handling for hint frames
                if (txn == 0) {
                    return true;
                }

                CompletableFuture<String> future = pendingTransactions.remove(txn);

                if (future == null) {
                    throw new IllegalStateException("txn not pending <[" + txn + "]>");
                }

                future.complete(frameContext.relpFrame().payload().toString());
                LOGGER.debug("completed transaction for <[{}]>", txn);

                frameContext.relpFrame().close();

                return true;
            }

            @Override
            public void close() throws Exception {
                LOGGER.debug("client FrameDelegate close");
            }

            @Override
            public boolean isStub() {
                return false;
            }
        };


        ConnectContext connectContext = connectContextFactory.create(
                new InetSocketAddress(port),
                essentialClientDelegate,
                connectionContextConsumer
        );
        connectContext.register(server.eventLoop);

        try (ConnectionContext connectionContext = readyContexts.take()) {
            Transmit transmit = new Transmit(connectionContext, pendingTransactions);

            CompletableFuture<String> open = transmit.transmit("open", "a hallo yo client".getBytes(StandardCharsets.UTF_8));
            CompletableFuture<String> syslog = transmit.transmit("syslog", "yonnes payload".getBytes(StandardCharsets.UTF_8));
            CompletableFuture<String> close = transmit.transmit("close", "".getBytes(StandardCharsets.UTF_8));

            try {
                String openResponse = open.get();
                LOGGER.debug("openResponse <[{}]>", openResponse);
                String syslogResponse = syslog.get();
                LOGGER.debug("syslogResponse <[{}]>", syslogResponse);
                Assertions.assertEquals("200 OK", syslogResponse);
                String closeResponse = close.get();
                LOGGER.debug("closeResponse <[{}]>", closeResponse);
            } catch (ExecutionException executionException) {
                throw new RuntimeException(executionException); // TODO
            }

            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        //throw new IllegalStateException("this is wip");
    }

    private class Transmit {

        private final ConnectionContext connectionContext;
        private final AtomicInteger txnCounter;
        HashMap<Integer, CompletableFuture<String>> pendingReplyTransactions;

        Transmit(ConnectionContext connectionContext, HashMap<Integer, CompletableFuture<String>> pendingReplyTransactions) {
            this.connectionContext = connectionContext;
            this.txnCounter = new AtomicInteger();
            this.pendingReplyTransactions = pendingReplyTransactions;
        }

        public CompletableFuture<String> transmit(String command, byte[] data) {
            RelpFrameTX relpFrameTX = new RelpFrameTX(command, data);
            int txn = txnCounter.incrementAndGet();
            relpFrameTX.setTransactionNumber(txn);
            if (pendingReplyTransactions.containsKey(txn)) {
                throw new IllegalStateException("already pending txn <" + txn + ">");
            }
            CompletableFuture<String> future = new CompletableFuture<>();
            pendingReplyTransactions.put(txn, future);
            connectionContext.relpWrite().accept(Collections.singletonList(relpFrameTX));

            return future;
        }
    }
}
