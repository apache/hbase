package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.io.hfile.bucket.CacheFullException;

public interface Arena {

  MemoryBuffer allocateByteBuffer(int size) throws
    CacheFullException, BucketAllocatorException;

  void freeByteBuffer(final MemoryBuffer buffer);

}
