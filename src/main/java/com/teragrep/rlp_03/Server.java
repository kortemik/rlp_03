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

package com.teragrep.rlp_03;

import com.teragrep.rlp_03.config.Config;
import com.teragrep.rlp_03.config.TLSConfig;
import com.teragrep.rlp_03.context.FrameProcessorPool;
import com.teragrep.rlp_03.context.channel.PlainFactory;
import com.teragrep.rlp_03.context.channel.SocketFactory;
import com.teragrep.rlp_03.context.channel.TLSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * A class that starts the server connection to the client. Fires up a new thread
 * for the Socket Processor.
 */
public class Server implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);
    final Config config;
    final TLSConfig tlsConfig;

    private final FrameProcessorPool frameProcessorPool;

    public final ThreadPoolExecutor executorService;

    private final AtomicBoolean stop;

    public final Status startup;


    public Server(Config config, FrameProcessor frameProcessor) {
        this(config, () -> frameProcessor);
    }

    public Server(Config config, Supplier<FrameProcessor> frameProcessorSupplier) {
        this(config, new TLSConfig(), frameProcessorSupplier);
    }

    public Server(
            Config config,
            TLSConfig tlsConfig,
            FrameProcessor frameProcessor
    ) {
        this(config, tlsConfig, () -> frameProcessor);
    }

    public Server(
            Config config,
            TLSConfig tlsConfig,
            Supplier<FrameProcessor> frameProcessorSupplier
    ) {
        this.config = config;
        this.tlsConfig = tlsConfig;
        this.frameProcessorPool = new FrameProcessorPool(frameProcessorSupplier);
        this.executorService = new ThreadPoolExecutor(config.numberOfThreads, config.numberOfThreads, Long.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        this.stop = new AtomicBoolean();
        this.startup = new Status();
    }

    public void stop() throws InterruptedException {
        LOGGER.debug("stopping");
        stop.set(true);
        frameProcessorPool.close();
    }

    @Override
    public void run() {
        config.validate();

        SocketFactory socketFactory;
        if (tlsConfig.useTls) {
            socketFactory = new TLSFactory(tlsConfig.getSslContext(), tlsConfig.getSslEngineFunction());
        }
        else {
            socketFactory = new PlainFactory();
        }

        try (SocketPoll socketPoll = new SocketPoll(config.port, executorService, socketFactory, frameProcessorPool)) {

            startup.complete(); // indicate successful startup
            LOGGER.debug("Started");
            while (!stop.get()) {
                socketPoll.poll();
            }
        }
        catch (IOException ioException) {
            throw new UncheckedIOException(ioException);
        }
        LOGGER.debug("Stopped");
    }
}