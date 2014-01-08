package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.hbase.regionserver.metrics.SchemaConfigured;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * A representation of a raw {@link HFileBlock}, used to pass a small amount
 * of metadata around with the raw byte array. This is primarily useful in the
 * context of making policy decisions and updating metrics when caching raw
 * block data in L2 and L3 caches.
 */
public class RawHFileBlock extends SchemaConfigured implements Cacheable {
  private final BlockType type;
  private final byte[] data;

  public static final long RAW_HFILE_BLOCK_OVERHEAD =
          SCHEMA_CONFIGURED_UNALIGNED_HEAP_SIZE +
          // type and data references.
          2 * ClassSize.REFERENCE;

  public RawHFileBlock(BlockType type, byte[] data) {
    this.type = type;
    this.data = data;
  }

  public RawHFileBlock(final HFileBlock block) {
    this(block.getBlockType(), block.getBufferWithHeader().array());
  }

  @Override
  public BlockType getBlockType() {
    return type;
  }

  public byte[] getData() {
    return data;
  }

  @Override
  public long heapSize() {
    return ClassSize.align(RAW_HFILE_BLOCK_OVERHEAD);
  }
}
