package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.metrics.PercentileMetric;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Histogram;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public abstract class AbstractWAL {
  public static final byte [] METAFAMILY = Bytes.toBytes("METAFAMILY");
  public static final byte [] METAROW = Bytes.toBytes("METAROW");

  // For measuring size of each transaction
  protected static Histogram writeSize = new Histogram(
    PercentileMetric.HISTOGRAM_NUM_BUCKETS_DEFAULT,
    PercentileMetric.HISTOGRAM_MINVALUE_DEFAULT,
    PercentileMetric.HISTOGRAM_MAXVALUE_DEFAULT);

  // For measure the sync time for each HLog.append operation;
  protected static Histogram syncTime = new Histogram(
    PercentileMetric.HISTOGRAM_NUM_BUCKETS_DEFAULT,
    PercentileMetric.HISTOGRAM_MINVALUE_DEFAULT,
    PercentileMetric.HISTOGRAM_MAXVALUE_DEFAULT);

  // For measuring the internal group commit time
  protected static Histogram gsyncTime = new Histogram(
    PercentileMetric.HISTOGRAM_NUM_BUCKETS_DEFAULT,
    PercentileMetric.HISTOGRAM_MINVALUE_DEFAULT,
    PercentileMetric.HISTOGRAM_MAXVALUE_DEFAULT);

  public abstract long append(HRegionInfo info, byte [] tableName, WALEdit edits,
                     final long now)
    throws IOException, ExecutionException, InterruptedException;

  public abstract long startMemStoreFlush(final byte[] regionName);
  public abstract void completeMemStoreFlush(final byte[] regionName, final byte[] tableName,
                                    final long logSeqId, final boolean isMetaRegion);
  public abstract void abortMemStoreFlush(byte[] regionName);
  public abstract long startMemStoreFlush(final byte[] regionName,
                                 long firstSeqIdInStoresToFlush,
                                 long firstSeqIdInStoresNotToFlush);

  public abstract long obtainNextSequenceNumber()
    throws IOException, ExecutionException, InterruptedException;
  public abstract long getSequenceNumber();
  public abstract void initSequenceNumber(long seqid)
    throws IOException, ExecutionException, InterruptedException;

  public abstract void close() throws IOException;
  public abstract void closeAndDelete() throws IOException;
  public abstract String getPath();

  public static Histogram getWriteSizeHistogram() {
    return writeSize;
  }

  public static Histogram getSyncTimeHistogram() {
    return syncTime;
  }

  public static Histogram getGSyncTimeHistogram() {
    return gsyncTime;
  }

  public abstract long getLastCommittedIndex();

}
