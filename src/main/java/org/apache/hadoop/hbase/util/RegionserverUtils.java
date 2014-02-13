package org.apache.hadoop.hbase.util;

import java.util.List;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram;
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram.Bucket;

import com.google.common.base.Preconditions;

public class RegionserverUtils {
  /**
   * Adjusting the startRow of startBucket to region's startRow
   * and endRow of endBucket to region's endRow.
   * Modifies the current list
   * @param buckets
   * @return
   */
  public static List<Bucket> adjustHistogramBoundariesToRegionBoundaries(
      List<Bucket> buckets, byte[] startKey, byte[] endKey) {
    int size = buckets.size();
    Preconditions.checkArgument(size > 1);
    Bucket startBucket = buckets.get(0);
    Bucket endBucket = buckets.get(size - 1);
    buckets.set(0, new HFileHistogram.Bucket.Builder(startBucket)
      .setStartRow(startKey).create());
    buckets.set(size - 1, new HFileHistogram.Bucket.Builder(endBucket)
      .setEndRow(endKey).create());
    return buckets;
  }
}
