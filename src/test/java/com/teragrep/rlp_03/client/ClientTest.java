/*
 * Java Reliable Event Logging Protocol Library Server Implementation RLP-03
 * Copyright (C) 2021-2024 Suomen Kanuuna Oy
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

import com.teragrep.net_01.channel.context.ConnectContextFactory;
import com.teragrep.net_01.channel.socket.PlainFactory;
import com.teragrep.net_01.channel.socket.SocketFactory;
import com.teragrep.net_01.eventloop.EventLoopFactory;
import com.teragrep.rlp_03.frame.FrameDelegationClockFactory;
import com.teragrep.rlp_03.frame.RelpFrame;
import com.teragrep.rlp_03.frame.RelpFrameFactory;
import com.teragrep.rlp_03.frame.delegate.DefaultFrameDelegate;
import com.teragrep.net_01.eventloop.EventLoop;
import com.teragrep.net_01.server.ServerFactory;
import com.teragrep.rlp_03.frame.delegate.FrameContext;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ClientTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientTest.class);
    private EventLoop eventLoop;
    private Thread eventLoopThread;
    private final int port = 23601;
    private ExecutorService executorService;

    @BeforeAll
    public void init() {
        EventLoopFactory eventLoopFactory = new EventLoopFactory();
        Assertions.assertAll(() -> eventLoop = eventLoopFactory.create());

        eventLoopThread = new Thread(eventLoop, "SERVER_EL");
        eventLoopThread.start();

        AtomicLong bytes = new AtomicLong();
        AtomicLong approxGigas = new AtomicLong();
        Consumer<FrameContext> sharedConsumer = frameContext -> {
            long current = bytes.addAndGet(1);

            if (current % 1000000 == 0) {
                System.out.println("SERVER " + Thread.currentThread().getName() + " processed: " + current);
            }
        };

        executorService = ForkJoinPool.commonPool();
        ServerFactory serverFactory = new ServerFactory(
                eventLoop,
                executorService,
                new PlainFactory(),
                new FrameDelegationClockFactory(() -> new DefaultFrameDelegate(sharedConsumer))
        );

        Assertions.assertAll(() -> serverFactory.create(port));

        for (int i = 0; i < 25; i++) {
            EventLoop el = null;
            try {
                el = eventLoopFactory.create();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            new Thread(el, "SERVER_EL_" + i).start();

            ServerFactory sf2 = new ServerFactory(
                    el,
                    executorService,
                    new PlainFactory(),
                    new FrameDelegationClockFactory(() -> new DefaultFrameDelegate(sharedConsumer))
            );

            try {
                sf2.create(23602 + i);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @AfterAll
    public void cleanup() {
        eventLoop.stop();
        executorService.shutdown();
        Assertions.assertAll(eventLoopThread::join);
    }

    @Test
    public void testClient() throws IOException {

        // client takes resources from this pool
        ExecutorService executorService = Executors.newCachedThreadPool();

        // client connection type
        SocketFactory socketFactory = new PlainFactory();

        ConnectContextFactory connectContextFactory = new ConnectContextFactory(executorService, socketFactory);
        RelpClientFactory relpClientFactory = new RelpClientFactory(connectContextFactory, eventLoop);

        RelpFrameFactory relpFrameFactory = new RelpFrameFactory();

        try (
                RelpClient relpClient = relpClientFactory.open(new InetSocketAddress("localhost", port)).get(1, TimeUnit.SECONDS)
        ) {

            // send open
            CompletableFuture<RelpFrame> open = relpClient
                    .transmit(relpFrameFactory.create("open", "a hallo yo client"));

            // send syslog
            CompletableFuture<RelpFrame> syslog = relpClient
                    .transmit(relpFrameFactory.create("syslog", "yonnes payload"));

            // send close
            CompletableFuture<RelpFrame> close = relpClient.transmit(relpFrameFactory.create("close", ""));

            // test open response
            try (RelpFrame openResponse = open.get()) {
                LOGGER.debug("openResponse <[{}]>", openResponse);
                Assertions.assertEquals("rsp", openResponse.command().toString());
                Pattern pattern = Pattern
                        .compile(
                                "200 OK\\nrelp_version=0\\nrelp_software=rlp_03,[0-9]+.[0-9]+.[0-9]+(-[a-zA-Z0-9]+)?,https://teragrep.com\\ncommands=syslog\\n"
                        );
                Matcher matcher = pattern.matcher(openResponse.payload().toString());
                Assertions.assertTrue(matcher.find());
            } // close the openResponse frame, free resources

            // test syslog response
            try (RelpFrame syslogResponse = syslog.get()) {
                Assertions.assertEquals("rsp", syslogResponse.command().toString());
                LOGGER.debug("syslogResponse <[{}]>", syslogResponse);
                Assertions.assertEquals("200 OK", syslogResponse.payload().toString());
            } // close the syslogResponse frame, free resources

            // test close response
            try (RelpFrame closeResponse = close.get()) {
                Assertions.assertEquals("rsp", closeResponse.command().toString());
                LOGGER.debug("closeResponse <[{}]>", closeResponse);
                Assertions.assertEquals("", closeResponse.payload().toString());
            } // close the closeResponse frame, free resources
        }
        catch (InterruptedException | ExecutionException | TimeoutException exception) {
            throw new RuntimeException(exception);
        }
    }

    @Test
    public void testMultiTheadedClientUse() throws InterruptedException, IOException {
        EventLoopFactory clientEventLoopFactory = new EventLoopFactory();
        EventLoop eventLoop1 = clientEventLoopFactory.create();
        new Thread(eventLoop1, "CLIENT_EL1").start();
        EventLoop eventLoop2 = clientEventLoopFactory.create();
        new Thread(eventLoop2, "CLIENT_EL2").start();
        EventLoop eventLoop3 = clientEventLoopFactory.create();
        new Thread(eventLoop3, "CLIENT_EL3").start();
        EventLoop eventLoop4 = clientEventLoopFactory.create();
        new Thread(eventLoop4, "CLIENT_EL4").start();
        EventLoop eventLoop5 = clientEventLoopFactory.create();
        new Thread(eventLoop5, "CLIENT_EL5").start();
        EventLoop eventLoop6 = clientEventLoopFactory.create();
        new Thread(eventLoop6, "CLIENT_EL6").start();
        EventLoop eventLoop7 = clientEventLoopFactory.create();
        new Thread(eventLoop7, "CLIENT_EL7").start();
        EventLoop eventLoop8 = clientEventLoopFactory.create();
        new Thread(eventLoop8, "CLIENT_EL8").start();

        Thread client1 = new Thread(new MTSharedClient(eventLoop1, port, ForkJoinPool.commonPool()));
        Thread client2 = new Thread(new MTSharedClient(eventLoop2, port, ForkJoinPool.commonPool()));
        Thread client3 = new Thread(new MTSharedClient(eventLoop3, port, ForkJoinPool.commonPool()));
        Thread client4 = new Thread(new MTSharedClient(eventLoop4, port, ForkJoinPool.commonPool()));
        Thread client5 = new Thread(new MTSharedClient(eventLoop5, port, ForkJoinPool.commonPool()));
        Thread client6 = new Thread(new MTSharedClient(eventLoop6, port, ForkJoinPool.commonPool()));
        Thread client7 = new Thread(new MTSharedClient(eventLoop7, port, ForkJoinPool.commonPool()));
        Thread client8 = new Thread(new MTSharedClient(eventLoop8, port, ForkJoinPool.commonPool()));

        client1.start();
        client2.start();
        client3.start();
        client4.start();
        client5.start();
        client6.start();
        client7.start();
        client8.start();

        client1.join();
        client2.join();
        client3.join();
        client4.join();
        client5.join();
        client6.join();
        client7.join();
        client8.join();
    }

    private static class MTSharedClient implements Runnable {

        private final EventLoop eventLoop;
        private final int port;
        private final ExecutorService executorService;

        MTSharedClient(EventLoop eventLoop, int port, ExecutorService executorService) {
            this.eventLoop = eventLoop;
            this.port = port;
            this.executorService = executorService;
        }

        @Override
        public void run() {
            // client connection type
            SocketFactory socketFactory = new PlainFactory();

            ConnectContextFactory connectContextFactory = new ConnectContextFactory(executorService, socketFactory);
            RelpClientFactory relpClientFactory = new RelpClientFactory(connectContextFactory, eventLoop);

            RelpFrameFactory relpFrameFactory = new RelpFrameFactory();
            try (
                    RelpClient relpClient = relpClientFactory.open(new InetSocketAddress("localhost", port)).get(1, TimeUnit.SECONDS)
            ) {

                CompletableFuture<RelpFrame> open = relpClient
                        .transmit(relpFrameFactory.create("open", "a hallo multi-yo client"));

                // test open response
                try (RelpFrame openResponse = open.get()) {
                    LOGGER.debug("openResponse <[{}]>", openResponse);
                    Assertions.assertEquals("rsp", openResponse.command().toString());
                    Pattern pattern = Pattern
                            .compile(
                                    "200 OK\\nrelp_version=0\\nrelp_software=rlp_03,[0-9]+.[0-9]+.[0-9]+(-[a-zA-Z0-9]+)?,https://teragrep.com\\ncommands=syslog\\n"
                            );
                    Matcher matcher = pattern.matcher(openResponse.payload().toString());
                    Assertions.assertTrue(matcher.find());
                } // close the openResponse frame, free resources

                Runnable runnable = () -> {
                    while (true) {
                        // send syslog
                        CompletableFuture<RelpFrame> syslog = relpClient
                                .transmit(relpFrameFactory.create("syslog", "multi-client payloads here"));

                        try {
                            int a = syslog.get().txn().toInt();
                            if (a % 100000 == 0) {
                                System.out.println("from " + Thread.currentThread().getName() + " a: " + a);
                            }
                        }
                        catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        catch (ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }

                };

                Thread thread1 = new Thread(runnable);
                Thread thread2 = new Thread(runnable);
                Thread thread3 = new Thread(runnable);
                Thread thread4 = new Thread(runnable);
                Thread thread5 = new Thread(runnable);
                Thread thread6 = new Thread(runnable);
                Thread thread7 = new Thread(runnable);
                Thread thread8 = new Thread(runnable);

                thread1.start();
                thread2.start();
                thread3.start();
                thread4.start();
                thread5.start();
                thread6.start();
                thread7.start();
                thread8.start();

                thread1.join();
                thread2.join();
                thread3.join();
                thread4.join();
                thread5.join();
                thread6.join();
                thread7.join();
                thread8.join();

                // send close
                CompletableFuture<RelpFrame> close = relpClient.transmit(relpFrameFactory.create("close", ""));

                // test close response
                try (RelpFrame closeResponse = close.get()) {
                    Assertions.assertEquals("rsp", closeResponse.command().toString());
                    LOGGER.debug("closeResponse <[{}]>", closeResponse);
                    Assertions.assertEquals("", closeResponse.payload().toString());
                } // close the closeResponse frame, free resources
            }
            catch (InterruptedException | ExecutionException | TimeoutException exception) {
                throw new RuntimeException(exception);
            }
        }

    }
}
