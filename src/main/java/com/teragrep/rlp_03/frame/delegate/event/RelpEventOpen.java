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
package com.teragrep.rlp_03.frame.delegate.event;

import com.teragrep.rlp_03.frame.RelpFrame;
import com.teragrep.rlp_03.frame.RelpFrameImpl;
import com.teragrep.rlp_03.frame.delegate.FrameContext;
import com.teragrep.rlp_03.frame.fragment.Fragment;
import com.teragrep.rlp_03.frame.fragment.FragmentImpl;
import com.teragrep.rlp_03.frame.fragment.FragmentStub;
import com.teragrep.rlp_03.frame.function.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class RelpEventOpen extends RelpEvent {

    private final RelpFrame responseFrameTemplate;

    RelpEventOpen() {
        Fragment txn = new FragmentStub();

        String cmd = "rsp";
        byte[] cmdBytes = cmd.getBytes(StandardCharsets.US_ASCII);
        ByteBuffer cmdBB = ByteBuffer.allocateDirect(cmdBytes.length);
        cmdBB.put(cmdBytes);

        Fragment command = new FragmentImpl(new CommandFunction());
        command.accept(cmdBB);

        String content = "200 OK\nrelp_version=0\nrelp_software=RLP-01,1.0.1,https://teragrep.com\ncommands=syslog\n";
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        ByteBuffer contentBB = ByteBuffer.allocateDirect(contentBytes.length);
        contentBB.put(contentBytes);


        String contentLength = String.valueOf(contentBytes.length);
        byte[] contentLengthBytes = contentLength.getBytes(StandardCharsets.US_ASCII);
        ByteBuffer contentLenthBB = ByteBuffer.allocateDirect(contentLengthBytes.length);
        contentLenthBB.put(contentBytes);

        Fragment payloadLength = new FragmentImpl(new PayloadLengthFunction());
        payloadLength.accept(contentLenthBB);

        Fragment payload = new FragmentImpl(new PayloadFunction(contentBytes.length));
        payload.accept(contentBB);

        String end = "\n";
        byte[] endBytes = end.getBytes(StandardCharsets.US_ASCII);
        ByteBuffer endBB = ByteBuffer.allocateDirect(endBytes.length);
        endBB.put(endBytes);

        Fragment endOfTransfer = new FragmentImpl(new EndOfTransferFunction());
        endOfTransfer.accept(endBB);

        this.responseFrameTemplate = new RelpFrameImpl(txn, command, payloadLength, payload, endOfTransfer);
    }


    @Override
    public void accept(FrameContext frameContext) {
        try {

            RelpFrame frame = new RelpFrameImpl(frameContext.relpFrame().txn(), responseFrameTemplate.command(),
                    responseFrameTemplate.payloadLength(), responseFrameTemplate.payload(),
                    responseFrameTemplate.endOfTransfer()
            );

            frameContext.establishedContext().relpWrite().accept(Collections.singletonList(frame));
        }
        finally {
            frameContext.relpFrame().close();
        }
    }
}
