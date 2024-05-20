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
package com.teragrep.rlp_03.frame;

import com.teragrep.rlp_03.frame.fragment.Fragment;
import com.teragrep.rlp_03.frame.fragment.FragmentStub;
import com.teragrep.rlp_03.frame.fragment.clocks.*;

import java.nio.ByteBuffer;

public class FrameClock {

    private static final RelpFrameStub relpFrameStub = new RelpFrameStub();
    private static final FragmentStub fragmentStub = new FragmentStub();

    private final TransactionClock transactionClock;
    private final CommandClock commandClock;
    private final PayloadLengthClock payloadLengthClock;
    private final PayloadClock payloadClock;
    private final EndOfTransferClock endOfTransferClock;

    private Fragment txn;
    private Fragment command;
    private Fragment payloadLength;
    private int cachedPayloadLength;
    private Fragment payload;
    private Fragment endOfTransfer;

    public FrameClock() {
        this.transactionClock = new TransactionClock();
        this.commandClock = new CommandClock();
        this.payloadLengthClock = new PayloadLengthClock();
        this.payloadClock = new PayloadClock();
        this.endOfTransferClock = new EndOfTransferClock();

        reset();
    }

    private void reset() {
        txn = fragmentStub;
        command = fragmentStub;
        payloadLength = fragmentStub;
        cachedPayloadLength = Integer.MIN_VALUE;
        payload = fragmentStub;
        endOfTransfer = fragmentStub;
    }

    public synchronized RelpFrame submit(ByteBuffer input) {
        RelpFrame relpFrame = relpFrameStub;

        while (input.hasRemaining()) {
            if (txn.isStub()) {
                txn = transactionClock.submit(input);
            }
            else if (command.isStub()) {
                command = commandClock.submit(input);
            }
            else if (payloadLength.isStub()) {
                payloadLength = payloadLengthClock.submit(input);
                if (!payloadLength.isStub()) {
                    cachedPayloadLength = payloadLength.toInt();
                }
            }
            else if (payload.isStub()) {
                payload = payloadClock.submit(input, cachedPayloadLength);
            }
            else if (endOfTransfer.isStub()) {
                endOfTransfer = endOfTransferClock.submit(input);
                if (!endOfTransfer.isStub()) {
                    relpFrame = new RelpFrameImpl(txn, command, payloadLength, payload, endOfTransfer);
                    reset();
                    break;
                }
            }
            else {
                throw new IllegalStateException("FrameClock not in phase");
            }
        }

        return relpFrame;
    }
}
