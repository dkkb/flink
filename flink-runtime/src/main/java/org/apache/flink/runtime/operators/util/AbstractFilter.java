package org.apache.flink.runtime.operators.util;

import org.apache.flink.core.memory.MemorySegment;

public abstract class AbstractFilter {
    private BitSet bitSet;

    public void setBitsLocation(MemorySegment memorySegment, int offset) {
        this.bitSet.setMemorySegment(memorySegment, offset);
    }

    public abstract void addHash(int hash32);
}
