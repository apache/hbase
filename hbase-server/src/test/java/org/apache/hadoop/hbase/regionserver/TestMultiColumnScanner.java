/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.BlockCacheFactory;
import org.apache.hadoop.hbase.util.BloomFilterUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests optimized scanning of multiple columns. <br>
 * We separated the big test into several sub-class UT, because When in ROWCOL bloom type, we will
 * test the row-col bloom filter frequently for saving HDFS seek once we switch from one column to
 * another in our UT. It's cpu time consuming (~45s for each case), so moved the ROWCOL case into a
 * separated LargeTests to avoid timeout failure. <br>
 * <br>
 * To be clear: In TestMultiColumnScanner, we will flush 10 (NUM_FLUSHES=10) HFiles here, and the
 * table will put ~1000 cells (rows=20, ts=6, qualifiers=8, total=20*6*8 ~ 1000) . Each full table
 * scan will check the ROWCOL bloom filter 20 (rows)* 8 (column) * 10 (hfiles)= 1600 times, beside
 * it will scan the full table 6*2^8=1536 times, so finally will have 1600*1536=2457600 bloom filter
 * testing. (See HBASE-21520)
 */
public abstract class TestMultiColumnScanner {

  private static final Logger LOG = LoggerFactory.getLogger(TestMultiColumnScanner.class);

  private static final String TABLE_NAME =
      TestMultiColumnScanner.class.getSimpleName();

  static final int MAX_VERSIONS = 50;

  private static final String FAMILY = "CF";
  private static final byte[] FAMILY_BYTES = Bytes.toBytes(FAMILY);

  /**
   * The size of the column qualifier set used. Increasing this parameter
   * exponentially increases test time.
   */
  private static final int NUM_COLUMNS = 8;

  private static final int MAX_COLUMN_BIT_MASK = 1 << NUM_COLUMNS - 1;
  private static final int NUM_FLUSHES = 10;
  private static final int NUM_ROWS = 20;

  /** A large value of type long for use as a timestamp */
  private static final long BIG_LONG = 9111222333444555666L;

  /**
   * Timestamps to test with. Cannot use {@link Long#MAX_VALUE} here, because
   * it will be replaced by an timestamp auto-generated based on the time.
   */
  private static final long[] TIMESTAMPS = new long[] { 1, 3, 5,
      Integer.MAX_VALUE, BIG_LONG, Long.MAX_VALUE - 1 };

  /** The probability that a column is skipped in a store file. */
  private static final double COLUMN_SKIP_IN_STORE_FILE_PROB = 0.7;

  /** The probability to delete a row/column pair */
  private static final double DELETE_PROBABILITY = 0.02;

  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @Parameter(0)
  public Compression.Algorithm comprAlgo;

  @Parameter(1)
  public BloomType bloomType;

  @Parameter(2)
  public DataBlockEncoding dataBlockEncoding;

  // Some static sanity-checking.
  static {
    assertTrue(BIG_LONG > 0.9 * Long.MAX_VALUE); // Guard against typos.

    // Ensure TIMESTAMPS are sorted.
    for (int i = 0; i < TIMESTAMPS.length - 1; ++i)
      assertTrue(TIMESTAMPS[i] < TIMESTAMPS[i + 1]);
  }

  public static Collection<Object[]> generateParams(Compression.Algorithm algo,
      boolean useDataBlockEncoding) {
    List<Object[]> parameters = new ArrayList<>();
    for (BloomType bloomType : BloomType.values()) {
      DataBlockEncoding dataBlockEncoding =
          useDataBlockEncoding ? DataBlockEncoding.PREFIX : DataBlockEncoding.NONE;
      parameters.add(new Object[] { algo, bloomType, dataBlockEncoding });
    }
    return parameters;
  }

  @Test
  public void testMultiColumnScanner() throws IOException {
    TEST_UTIL.getConfiguration().setInt(BloomFilterUtil.PREFIX_LENGTH_KEY, 10);
    HRegion region = TEST_UTIL.createTestRegion(TABLE_NAME,
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILY_BYTES).setCompressionType(comprAlgo)
            .setBloomFilterType(bloomType).setMaxVersions(MAX_VERSIONS)
            .setDataBlockEncoding(dataBlockEncoding).build(),
        BlockCacheFactory.createBlockCache(TEST_UTIL.getConfiguration()));
    List<String> rows = sequentialStrings("row", NUM_ROWS);
    List<String> qualifiers = sequentialStrings("qual", NUM_COLUMNS);
    List<KeyValue> kvs = new ArrayList<>();
    Set<String> keySet = new HashSet<>();

    // A map from <row>_<qualifier> to the most recent delete timestamp for
    // that column.
    Map<String, Long> lastDelTimeMap = new HashMap<>();

    Random rand = ThreadLocalRandom.current();
    for (int iFlush = 0; iFlush < NUM_FLUSHES; ++iFlush) {
      for (String qual : qualifiers) {
        // This is where we decide to include or not include this column into
        // this store file, regardless of row and timestamp.
        if (rand.nextDouble() < COLUMN_SKIP_IN_STORE_FILE_PROB)
          continue;

        byte[] qualBytes = Bytes.toBytes(qual);
        for (String row : rows) {
          Put p = new Put(Bytes.toBytes(row));
          for (long ts : TIMESTAMPS) {
            String value = createValue(row, qual, ts);
            KeyValue kv = KeyValueTestUtil.create(row, FAMILY, qual, ts,
                value);
            assertEquals(kv.getTimestamp(), ts);
            p.add(kv);
            String keyAsString = kv.toString();
            if (!keySet.contains(keyAsString)) {
              keySet.add(keyAsString);
              kvs.add(kv);
            }
          }
          region.put(p);

          Delete d = new Delete(Bytes.toBytes(row));
          boolean deletedSomething = false;
          for (long ts : TIMESTAMPS)
            if (rand.nextDouble() < DELETE_PROBABILITY) {
              d.addColumns(FAMILY_BYTES, qualBytes, ts);
              String rowAndQual = row + "_" + qual;
              Long whenDeleted = lastDelTimeMap.get(rowAndQual);
              lastDelTimeMap.put(rowAndQual, whenDeleted == null ? ts
                  : Math.max(ts, whenDeleted));
              deletedSomething = true;
            }
          if (deletedSomething)
            region.delete(d);
        }
      }
      region.flush(true);
    }

    Collections.sort(kvs, CellComparatorImpl.COMPARATOR);
    for (int maxVersions = 1; maxVersions <= TIMESTAMPS.length; ++maxVersions) {
      for (int columnBitMask = 1; columnBitMask <= MAX_COLUMN_BIT_MASK; ++columnBitMask) {
        Scan scan = new Scan();
        scan.readVersions(maxVersions);
        Set<String> qualSet = new TreeSet<>();
        {
          int columnMaskTmp = columnBitMask;
          for (String qual : qualifiers) {
            if ((columnMaskTmp & 1) != 0) {
              scan.addColumn(FAMILY_BYTES, Bytes.toBytes(qual));
              qualSet.add(qual);
            }
            columnMaskTmp >>= 1;
          }
          assertEquals(0, columnMaskTmp);
        }

        InternalScanner scanner = region.getScanner(scan);
        List<Cell> results = new ArrayList<>();

        int kvPos = 0;
        int numResults = 0;
        String queryInfo = "columns queried: " + qualSet + " (columnBitMask="
            + columnBitMask + "), maxVersions=" + maxVersions;

        while (scanner.next(results) || results.size() > 0) {
          for (Cell kv : results) {
            while (kvPos < kvs.size()
                && !matchesQuery(kvs.get(kvPos), qualSet, maxVersions,
                    lastDelTimeMap)) {
              ++kvPos;
            }
            String rowQual = getRowQualStr(kv);
            String deleteInfo = "";
            Long lastDelTS = lastDelTimeMap.get(rowQual);
            if (lastDelTS != null) {
              deleteInfo = "; last timestamp when row/column " + rowQual
                  + " was deleted: " + lastDelTS;
            }
            assertTrue("Scanner returned additional key/value: " + kv + ", "
                + queryInfo + deleteInfo + ";", kvPos < kvs.size());
            assertTrue("Scanner returned wrong key/value; " + queryInfo + deleteInfo + ";",
              PrivateCellUtil.equalsIgnoreMvccVersion(kvs.get(kvPos), (kv)));
            ++kvPos;
            ++numResults;
          }
          results.clear();
        }
        for (; kvPos < kvs.size(); ++kvPos) {
          KeyValue remainingKV = kvs.get(kvPos);
          assertFalse("Matching column not returned by scanner: "
              + remainingKV + ", " + queryInfo + ", results returned: "
              + numResults, matchesQuery(remainingKV, qualSet, maxVersions,
              lastDelTimeMap));
        }
      }
    }
    assertTrue("This test is supposed to delete at least some row/column " +
        "pairs", lastDelTimeMap.size() > 0);
    LOG.info("Number of row/col pairs deleted at least once: " +
       lastDelTimeMap.size());
    HBaseTestingUtil.closeRegionAndWAL(region);
  }

  private static String getRowQualStr(Cell kv) {
    String rowStr = Bytes.toString(CellUtil.cloneRow(kv));
    String qualStr = Bytes.toString(CellUtil.cloneQualifier(kv));
    return rowStr + "_" + qualStr;
  }

  private static boolean matchesQuery(KeyValue kv, Set<String> qualSet,
      int maxVersions, Map<String, Long> lastDelTimeMap) {
    Long lastDelTS = lastDelTimeMap.get(getRowQualStr(kv));
    long ts = kv.getTimestamp();
    return qualSet.contains(qualStr(kv))
        && ts >= TIMESTAMPS[TIMESTAMPS.length - maxVersions]
        && (lastDelTS == null || ts > lastDelTS);
  }

  private static String qualStr(KeyValue kv) {
    return Bytes.toString(kv.getQualifierArray(), kv.getQualifierOffset(),
        kv.getQualifierLength());
  }

  static String createValue(String row, String qual, long ts) {
    return "value_for_" + row + "_" + qual + "_" + ts;
  }

  private static List<String> sequentialStrings(String prefix, int n) {
    List<String> lst = new ArrayList<>();
    for (int i = 0; i < n; ++i) {
      StringBuilder sb = new StringBuilder();
      sb.append(prefix + i);

      // Make column length depend on i.
      int iBitShifted = i;
      while (iBitShifted != 0) {
        sb.append((iBitShifted & 1) == 0 ? 'a' : 'b');
        iBitShifted >>= 1;
      }

      lst.add(sb.toString());
    }
    return lst;
  }
}

