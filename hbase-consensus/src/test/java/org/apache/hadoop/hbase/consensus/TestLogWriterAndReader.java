package org.apache.hadoop.hbase.consensus;

import junit.framework.Assert;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.consensus.log.LogReader;
import org.apache.hadoop.hbase.consensus.log.LogWriter;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class TestLogWriterAndReader {
  private static final Logger LOG = LoggerFactory.getLogger(
          TestLogWriterAndReader.class);

  private final Arena arena = new InHeapArena(BucketAllocator.DEFAULT_BUCKETS,
    HConstants.ARENA_CAPACITY_DEFAULT);

  @Test
  public void testSequentialWriteAndRead() throws IOException {
    final int numTXNs = 100;
    final KeyValue.KVComparator comparator = new KeyValue.KVComparator();

    // Initialize the writer
    File file = new File("testSequentialWriteAndRead");
    RandomAccessFile raf = new RandomAccessFile(file, "rw");
    LogWriter writer = new LogWriter(raf, false);

    // Generate the header
    final long initialIndex = 0;
    final long term = 1;
    writer.writeFileHeader(term, initialIndex);

    // Write the numTXNs to the log file
    long curIndex, filePosition;
    List<WALEdit> txns;
    WALEdit edit;
    for (curIndex = initialIndex; curIndex < numTXNs; curIndex++) {
      edit = new WALEdit();
      edit.add(new KeyValue(Bytes.toBytes(curIndex), curIndex));
      txns = Arrays.asList(edit);
      writer.append(curIndex, WALEdit.serializeToByteBuffer(txns, 1234567890L,
              Compression.Algorithm.NONE));

      // Test the truncate for every 10 entries;
      if (curIndex % 10 == 0) {
        // Write some dummy data to be truncated
        filePosition = writer.getCurrentPosition();

        edit = new WALEdit();
        edit.add(new KeyValue(Bytes.toBytes("ToBeTruncated"), System.currentTimeMillis()));
        txns = Arrays.asList(edit);

        long tmpIndex = curIndex + 1;
        long tmpOffset = writer.append(tmpIndex,
                WALEdit.serializeToByteBuffer(txns, 1234567890L,
                        Compression.Algorithm.NONE));

        Assert.assertEquals(filePosition, tmpOffset);
        writer.truncate(tmpOffset);
        Assert.assertEquals(tmpOffset, raf.getChannel().size());

        LOG.info("Truncate the log at the offset of " + tmpOffset + " for the index " + tmpIndex);
      }
    }

    // Close the writer
    writer.close();

    // Initialize the reader
    LogReader reader = new LogReader(file);
    reader.initialize();

    // Verify the header data
    Assert.assertEquals(term, reader.getCurrentTerm());
    Assert.assertEquals(initialIndex, reader.getInitialIndex());

    // Read the transactions and verify
    for (long i = initialIndex; i < numTXNs; i++) {
      MemoryBuffer buffer = reader.seekAndRead(i, arena);
      txns = WALEdit.deserializeFromByteBuffer(buffer.getBuffer());
      edit = txns.get(0);
      KeyValue kv = edit.getKeyValues().get(0);
      KeyValue expectedKV = new KeyValue(Bytes.toBytes(i), i);

      Assert.assertEquals(1, txns.size());
      Assert.assertEquals(1, edit.size());
      Assert.assertEquals(0, comparator.compare(expectedKV, kv));
      arena.freeByteBuffer(buffer);
    }

    LOG.info("Verified all " + numTXNs + " entries from the log file: " + file.getAbsoluteFile());
    file.delete();
  }
}
