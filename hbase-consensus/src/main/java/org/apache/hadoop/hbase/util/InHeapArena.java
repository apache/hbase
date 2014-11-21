package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.io.hfile.bucket.CacheFullException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A basic Arena Class which handles a chunk of contiguous memory using
 * a Bucket Allocator.
 */
public class InHeapArena implements Arena {

  public static final AtomicInteger numOps = new AtomicInteger();
  public static final AtomicInteger failedOps = new AtomicInteger();
  public static final AtomicInteger freedOps = new AtomicInteger();

  public static final Logger LOG = LoggerFactory
    .getLogger(InHeapArena.class);

  private final byte[] buffer;
  private BucketAllocator allocator;

  public InHeapArena(int[] bucketSizes, int capacity) {
    try {
      allocator = new BucketAllocator(bucketSizes, capacity);
    } catch (BucketAllocatorException e) {
      LOG.error("Cannot initialize allocator with size " + capacity +
        ". Reason", e);
    }
    buffer = new byte[capacity];
  }

  @Override
  public MemoryBuffer allocateByteBuffer(int size) throws
    CacheFullException, BucketAllocatorException {
    int offset;

    try {
      offset = (int) allocator.allocateBlock(size);
      numOps.incrementAndGet();
    } catch (CacheFullException | BucketAllocatorException e) {
      failedOps.incrementAndGet();
      if (LOG.isTraceEnabled()) {
        LOG.trace("Cannot allocate memory buffer of size " + size, e);
      }
      throw e;
    }

    final ByteBuffer chunk = ByteBuffer.wrap(buffer, offset, size);
    return new MemoryBuffer(chunk, offset, chunk.remaining());
  }

  @Override
  public void freeByteBuffer(final MemoryBuffer buffer) {
    if (buffer.getOffset() != MemoryBuffer.UNDEFINED_OFFSET) {
      allocator.freeBlock(buffer.getOffset());
      freedOps.incrementAndGet();
    }
  }

  public static int getNumOpsAndReset() {
    return numOps.getAndSet(0);
  }

  public static int getFailedOpsAndReset() {
    return failedOps.getAndSet(0);
  }

  public static int getFreedOpsAndReset() {
    return freedOps.getAndSet(0);
  }

  public BucketAllocator getAllocator() {
    return allocator;
  }
}
