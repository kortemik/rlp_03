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
package com.teragrep.rlp_03.frame.delegate.pool;

import com.teragrep.rlp_03.frame.delegate.FrameContext;
import com.teragrep.rlp_03.frame.delegate.FrameDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PoolDelegate implements FrameDelegate {

    private static final Logger LOGGER = LoggerFactory.getLogger(PoolDelegate.class);
    private final FrameDelegatePool frameDelegatePool;

    public PoolDelegate(FrameDelegatePool frameDelegatePool) {
        this.frameDelegatePool = frameDelegatePool;
    }

    @Override
    public boolean accept(FrameContext frameContext) {
        boolean rv = false;
        FrameDelegate frameDelegate = frameDelegatePool.take();

        if (!frameDelegate.isStub()) {
            rv = frameDelegate.accept(frameContext); // this thread goes there
            frameDelegatePool.offer(frameDelegate);
        }
        else {
            // TODO should this be IllegalState or should it just '0 serverclose 0' ?
            LOGGER
                    .warn(
                            "PoolingDelegate closing, rejecting frame and closing connection for PeerAddress <{}> PeerPort <{}>",
                            frameContext.establishedContext().socket().getTransportInfo().getPeerAddress(),
                            frameContext.establishedContext().socket().getTransportInfo().getPeerPort()
                    );
            frameContext.establishedContext().close();
        }
        return rv;
    }

    @Override
    public void close() {
        // ignored because each connection will call this, use frameDelegatePool.close() to close the pool
        LOGGER.debug("close");
    }

    @Override
    public boolean isStub() {
        return false;
    }

}
