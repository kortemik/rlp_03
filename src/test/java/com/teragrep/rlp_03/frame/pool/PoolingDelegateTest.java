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
package com.teragrep.rlp_03.frame.pool;

import com.teragrep.rlp_01.RelpCommand;
import com.teragrep.net_01.eventloop.EventLoop;
import com.teragrep.net_01.eventloop.EventLoopFactory;
import com.teragrep.rlp_03.frame.FrameDelegationClockFactory;
import com.teragrep.rlp_03.frame.delegate.FrameContext;
import com.teragrep.rlp_03.frame.delegate.pool.FrameDelegatePool;
import com.teragrep.rlp_03.frame.delegate.pool.PoolDelegate;
import com.teragrep.net_01.server.ServerFactory;
import com.teragrep.net_01.channel.socket.PlainFactory;
import com.teragrep.rlp_03.frame.delegate.EventDelegate;
import com.teragrep.rlp_03.frame.delegate.FrameDelegate;
import com.teragrep.rlp_03.frame.delegate.SequencingDelegate;
import com.teragrep.rlp_03.frame.delegate.event.RelpEvent;
import com.teragrep.rlp_03.frame.delegate.event.RelpEventClose;
import com.teragrep.rlp_03.frame.delegate.event.RelpEventOpen;
import com.teragrep.rlp_03.frame.delegate.event.RelpEventSyslog;
import com.teragrep.rlp_03.readme.ExampleRelpClient;
import com.teragrep.rlp_03.version.CachedVersion;
import com.teragrep.rlp_03.version.Version;
import com.teragrep.rlp_03.version.VersionImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class PoolingDelegateTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(PoolingDelegateTest.class);

    @Test
    public void testPoolingDelegate() {

        int sends = 8;

        AtomicLong frameDelegates = new AtomicLong();
        AtomicLong frameCount = new AtomicLong();
        CountDownLatch countDownLatch = new CountDownLatch(sends);

        // consumer for syslog command processing
        Consumer<FrameContext> frameContextConsumer = frameContext -> {
            LOGGER.debug("got frameContext with relpFrame <{}>", frameContext.relpFrame());
            frameCount.incrementAndGet();
            // make main thread run faster than this, so we get more pooled ones
            countDownLatch.countDown();

            while (true) {
                try {
                    Thread.sleep(200L);
                    break;
                }
                catch (InterruptedException ignored) {

                }
            }
        };

        Version version = new VersionImpl();
        CachedVersion cachedVersion = new CachedVersion(version.version());

        // supplier for pooled delegates
        Supplier<FrameDelegate> frameDelegateSupplier = () -> {
            Map<String, RelpEvent> relpEventMap = new HashMap<>();
            relpEventMap.put(RelpCommand.CLOSE, new RelpEventClose());
            relpEventMap.put(RelpCommand.OPEN, new RelpEventOpen(cachedVersion));
            relpEventMap.put(RelpCommand.SYSLOG, new RelpEventSyslog(frameContextConsumer));

            frameDelegates.incrementAndGet();
            LOGGER.debug("supplying eventDelegate");
            return new EventDelegate(relpEventMap);
        };

        try (FrameDelegatePool frameDelegatePool = new FrameDelegatePool(frameDelegateSupplier)) {

            // return always the same pool
            PoolDelegate poolDelegate = new PoolDelegate(frameDelegatePool);

            // each connection however needs Sequencing for txn so returning new SequencingDelegate decorator
            Supplier<FrameDelegate> poolSupplier = () -> {
                LOGGER.debug("return new SequencingDelegate");
                return new SequencingDelegate(poolDelegate);
            };

            int port = 11601;

            EventLoopFactory eventLoopFactory = new EventLoopFactory();
            EventLoop eventLoop = eventLoopFactory.create();
            Thread eventLoopThread = new Thread(eventLoop);
            eventLoopThread.start();

            ExecutorService executorService = Executors.newFixedThreadPool(8);
            ServerFactory serverFactory = new ServerFactory(
                    eventLoop,
                    executorService,
                    new PlainFactory(),
                    new FrameDelegationClockFactory(poolSupplier)
            );
            serverFactory.create(port);

            // send frames
            List<Thread> sendThreads = new LinkedList<>();
            for (int i = 0; i < sends; i++) {
                sendThreads.add(deferredSend(port, String.valueOf(i)));
            }

            countDownLatch.await();

            for (Thread thread : sendThreads) {
                thread.join();
            }

            eventLoop.stop();
            executorService.shutdown();
            Assertions.assertAll(eventLoopThread::join);

            Assertions.assertEquals(sends, frameDelegates.get());
            Assertions.assertEquals(sends, frameCount.get());

        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    Thread deferredSend(int port, String payload) {
        Runnable runnable = () -> {
            new ExampleRelpClient(port).send(payload);
        };

        Thread thread = new Thread(runnable);
        thread.start();
        return thread;
    }
}
