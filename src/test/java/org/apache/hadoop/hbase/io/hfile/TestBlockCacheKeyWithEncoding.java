package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.junit.Test;

public class TestBlockCacheKeyWithEncoding {

  @Test
  public void testBlockCachedWithNullTypeFoundWithEncoding() {
    // Assume we read and cache an index block with "null" passed to readBlock
    BlockCacheKey key1 =
        new BlockCacheKey("t35tf1l3n4m3", 55555, DataBlockEncoding.PREFIX, null);
    // The next time we request the same block as an index block we should find it in the block
    // cache
    BlockCacheKey key2 =
        new BlockCacheKey("t35tf1l3n4m3", 55555, DataBlockEncoding.PREFIX,
            BlockType.LEAF_INDEX);
    assertEquals("missmatch in hashCode", key1.hashCode(), key2.hashCode());
    assertEquals("missmatch in object.equals", key1.hashCode(), key2.hashCode());
  }
}
