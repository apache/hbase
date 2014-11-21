package org.apache.hadoop.hbase.consensus;

import junit.framework.Assert;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.consensus.log.RandomAccessLog;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Arena;
import org.apache.hadoop.hbase.util.BucketAllocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.InHeapArena;
import org.apache.hadoop.hbase.util.MemoryBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TestRandomAccessLog {

  File file;

  private final Arena arena = new InHeapArena(BucketAllocator.DEFAULT_BUCKETS,
    HConstants.ARENA_CAPACITY_DEFAULT);

  @Before
  public void setUp() throws Exception {
    file = new File("testBasics");
  }

  @After
  public void tearDown() throws Exception {
    if (file != null) {
      file.delete();
    }
  }

  @Test
  public void testBasics() throws IOException {
    RandomAccessLog log = new RandomAccessLog(file, false);

    Assert.assertTrue(log.getCreationTime() !=
      RandomAccessLog.UNKNOWN_CREATION_TIME);

    final int term = 1;
    final int startIndex = 1;
    final int middleIndex = 500;
    final int endIndex = 1000;
    final String readSessionKey = "test";

    for (int i = startIndex; i <= endIndex; i++) {
      WALEdit edit = new WALEdit();
      edit.add(new KeyValue(Bytes.toBytes("test" + i), System.currentTimeMillis()));
      log.append(term, i, WALEdit.serializeToByteBuffer(Arrays.asList(edit),
              1234567890L, Compression.Algorithm.NONE));
    }

    Assert.assertEquals(term, log.getCurrentTerm());
    Assert.assertEquals(startIndex, log.getInitialIndex());
    Assert.assertEquals(endIndex, log.getLastIndex());
    Assert.assertEquals(endIndex, log.getTxnCount());

    log.truncate(middleIndex + 1);
    Assert.assertEquals(term, log.getCurrentTerm());
    Assert.assertEquals(startIndex, log.getInitialIndex());
    Assert.assertEquals(middleIndex, log.getLastIndex());
    Assert.assertEquals(middleIndex, log.getTxnCount());

    log.finalizeForWrite();

    RandomAccessLog log2 = new RandomAccessLog(file, false);
    log2.rebuild(readSessionKey);
    Assert.assertEquals(log.getCurrentTerm(), log2.getCurrentTerm());
    Assert.assertEquals(log.getInitialIndex(), log2.getInitialIndex());
    Assert.assertEquals(log.getLastIndex(), log2.getLastIndex());
    Assert.assertEquals(log.getTxnCount(), log2.getTxnCount());


    for (int i = startIndex; i <= middleIndex; i++) {
      MemoryBuffer buffer = log2.getTransaction(term, i, readSessionKey, arena);
      List<WALEdit> txns = WALEdit.deserializeFromByteBuffer(buffer.getBuffer());
      Assert.assertEquals(1, txns.size());
      Assert.assertEquals(1, txns.get(0).getKeyValues().size());
      byte[] row = txns.get(0).getKeyValues().get(0).getRow();
      Assert.assertEquals(0, Bytes.compareTo(Bytes.toBytes("test" + i), row));
      arena.freeByteBuffer(buffer);
    }
  }
}
