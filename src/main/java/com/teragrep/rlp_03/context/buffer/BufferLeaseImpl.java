package com.teragrep.rlp_03.context.buffer;

import java.nio.ByteBuffer;
import java.util.concurrent.Phaser;

public class BufferLeaseImpl implements BufferLease {
    protected BufferContainer bufferContainer;
    private final Phaser phaser;

    public BufferLeaseImpl(BufferContainer bc) {
        this.bufferContainer = bc;
        this.phaser = new ClearingPhaser(1); // registered = 1
    }

    @Override
    public BufferContainer bufferContainer() {
        return this.bufferContainer;
    }

    @Override
    public long id() {
        return this.bufferContainer.id();
    }

    @Override
    public long refs() {
        // initial number of registered parties is 1
        return phaser.getRegisteredParties();
    }

    @Override
    public ByteBuffer buffer() {
        return this.bufferContainer.buffer();
    }

    @Override
    public void addRef() {
        int a = this.phaser.register();
        if (a < 0) {
            throw new IllegalStateException("paksis");
        }
    }

    @Override
    public void removeRef() {
        int a = phaser.arriveAndDeregister();
        if (a < 0) {
            throw new IllegalStateException("poksis");
        }
    }

    @Override
    public boolean isRefCountZero() {
        return phaser.isTerminated();
    }

    @Override
    public boolean isStub() {
        return this.bufferContainer.isStub();
    }

    @Override
    public synchronized boolean attemptRelease() {
        int a = phaser.arriveAndDeregister();
        if (a < 0) {
            throw new IllegalStateException("poks");
        }
        return phaser.isTerminated();
    }

    private class ClearingPhaser extends Phaser {
        public ClearingPhaser(int i) {
            super(i);
        }

        @Override
        protected boolean onAdvance(int phase, int registeredParties) {
            boolean rv = false;
            if (registeredParties == 0) {
                buffer().clear();
                rv = true;
            }
            return rv;
        }
    }

}
