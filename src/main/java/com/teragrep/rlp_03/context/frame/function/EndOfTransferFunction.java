package com.teragrep.rlp_03.context.frame.function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.function.BiFunction;

public class EndOfTransferFunction implements BiFunction<ByteBuffer, LinkedList<ByteBuffer>, Boolean> {

    //private static final Logger LOGGER = LoggerFactory.getLogger(EndOfTransferFunction.class);

    @Override
    public Boolean apply(ByteBuffer input, LinkedList<ByteBuffer> bufferSliceList) {
        // LOGGER.info("apply with input <{}> bufferSliceList.size() <{}>", input, bufferSliceList.size());
        boolean rv = false;
        if (input.hasRemaining()) {
            byte b = input.get();
            // LOGGER.info("read byte b <{}>", new String(new byte[]{b}, StandardCharsets.UTF_8));

            if (b == '\n') {
                // RelpFrame always ends with a newline byte.

                // adjust limit so that bufferSlice contains only this data (\n)
                ByteBuffer bufferSlice = (ByteBuffer) input.duplicate().limit(input.position()).rewind();
                bufferSliceList.add(bufferSlice);
                rv = true;
            } else {
                throw new IllegalStateException("RelpFrame EndOfTransfer \\n missing");
            }
        }

        return rv;
    }
}