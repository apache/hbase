package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.RawComparator;

public class CompoundBloomFilterBase implements BloomFilterBase {

  /**
   * At read time, the total number of chunks. At write time, the number of
   * chunks created so far. The first chunk has an ID of 0, and the current
   * chunk has the ID of numChunks - 1.
   */
  protected int numChunks;

  /**
   * The Bloom filter version. There used to be a DynamicByteBloomFilter which
   * had version 2.
   */
  public static final int VERSION = 3;

  /** Target error rate for configuring the filter and for information */
  protected float errorRate;

  /** The total number of keys in all chunks */
  protected long totalKeyCount;
  protected long totalByteSize;
  protected long totalMaxKeys;

  /** Hash function type to use, as defined in {@link Hash} */
  protected int hashType;
  
  /** Comparator used to compare Bloom filter keys */
  protected RawComparator<byte[]> comparator;

  @Override
  public long getMaxKeys() {
    return totalMaxKeys;
  }

  @Override
  public long getKeyCount() {
    return totalKeyCount;
  }

  @Override
  public long getByteSize() {
    return totalByteSize;
  }

  private static final byte[] DUMMY = new byte[0];

  /**
   * Prepare an ordered pair of row and qualifier to be compared using
   * {@link KeyValue.KeyComparator}. This is only used for row-column Bloom
   * filters.
   */
  @Override
  public byte[] createBloomKey(byte[] row, int roffset, int rlength,
      byte[] qualifier, int qoffset, int qlength) {
    if (qualifier == null)
      qualifier = DUMMY;

    // Make sure this does not specify a timestamp so that the default maximum
    // (most recent) timestamp is used.
    KeyValue kv = KeyValue.createFirstOnRow(row, roffset, rlength, DUMMY, 0, 0,
        qualifier, qoffset, qlength);
    return kv.getKey();
  }

  @Override
  public RawComparator<byte[]> getComparator() {
    return comparator;
  }

}
