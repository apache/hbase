package org.apache.hadoop.hbase.consensus;

import junit.framework.Assert;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.consensus.log.LogWriter;
import org.apache.hadoop.hbase.consensus.log.RandomAccessLog;
import org.apache.hadoop.hbase.consensus.log.ReadOnlyLog;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Arena;
import org.apache.hadoop.hbase.util.BucketAllocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.InHeapArena;
import org.apache.hadoop.hbase.util.MemoryBuffer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestReadOnlyLog {
  private static final Logger LOG = LoggerFactory.getLogger(
          TestReadOnlyLog.class);
  private static final int TOTAL_COMMIT = 100;
  private static final long INITIAL_TERM = 1;
  private static final long INITIAL_INDEX = 1;
  private static final KeyValue.KVComparator comparator = new KeyValue.KVComparator();
  private static final int CONCURRENT_READER_CNT = 10;
  private final AtomicInteger SUCCESS_CNT = new AtomicInteger(0);

  private static Random random;
  private static File file;
  private static ReadOnlyLog readOnlyLog;

  private final Arena arena = new InHeapArena(BucketAllocator.DEFAULT_BUCKETS,
    HConstants.ARENA_CAPACITY_DEFAULT);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    random = new Random();
    file = new File("TestReadOnlyLog_" + INITIAL_INDEX + "_" + INITIAL_INDEX);
    file.createNewFile();
    readOnlyLog = new ReadOnlyLog(file, INITIAL_TERM, INITIAL_INDEX);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    readOnlyLog.closeAndDelete();
  }

  @Test(timeout=50000)
  public void testConcurrentReader() throws Exception {
    prepareLogFile();

    Assert.assertTrue(readOnlyLog.getCreationTime() !=
      RandomAccessLog.UNKNOWN_CREATION_TIME);

    ExecutorService service = Executors.newFixedThreadPool(CONCURRENT_READER_CNT);

    for (int i = 0 ; i < CONCURRENT_READER_CNT; i++) {
      final String sessionKey = Integer.toString(i);
      service.submit(new Runnable() {
        @Override
        public void run() {
          readLogFile(sessionKey);
        }
      });
    }

    service.shutdown();
    try {
      service.awaitTermination(50000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {}

    Assert.assertEquals(CONCURRENT_READER_CNT, SUCCESS_CNT.get());

  }

  private void readLogFile(String sessionKey) {
    // Choose a random read point
    long readPointer = (long)random.nextInt(TOTAL_COMMIT);

    try {
      for (long i = readPointer; i < TOTAL_COMMIT; i++) {
        MemoryBuffer buffer = readOnlyLog.getTransaction(INITIAL_TERM, i, sessionKey, arena);

        // Read the commit entry
        List<WALEdit> txns = WALEdit.deserializeFromByteBuffer(buffer.getBuffer());

        WALEdit edit = txns.get(0);
        KeyValue kv = edit.getKeyValues().get(0);
        KeyValue expectedKV = new KeyValue(Bytes.toBytes(i), i);

        // Verify the commit entry
        Assert.assertEquals(1, txns.size());
        Assert.assertEquals(1, edit.size());
        Assert.assertEquals(0, comparator.compare(expectedKV, kv));
        arena.freeByteBuffer(buffer);
      }

      // Increase the success cnt
      SUCCESS_CNT.incrementAndGet();
      LOG.info("Reader #" + sessionKey + " has read and verified all the commits from " +
        + readPointer + " to " + TOTAL_COMMIT);

    } catch (Exception e) {
      // Fail the unit test if any exception caught
      LOG.error("Unexpected exception: ", e);
      Assert.fail("Unexpected exception: " + e);
    }
  }

  private void prepareLogFile() throws IOException {
    LogWriter writer = new LogWriter(new RandomAccessFile(file, "rw"), false);

    // Generate the header
    final long initialIndex = 0;
    final long term = 1;
    writer.writeFileHeader(term, initialIndex);

    // Write the numTXNs to the log file
    List<WALEdit> txns;
    WALEdit edit;
    for (long i = initialIndex; i < TOTAL_COMMIT; i++) {
      edit = new WALEdit();
      edit.add(new KeyValue(Bytes.toBytes(i), i));
      txns = Arrays.asList(edit);
      writer.append(i, WALEdit.serializeToByteBuffer(txns, 1234567890L,
              Compression.Algorithm.NONE));
    }

    // Close the writer
    writer.close();
  }
}
