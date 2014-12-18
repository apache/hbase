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
package org.apache.hadoop.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This Integration Test verifies acid guarantees across column families by frequently writing
 * values to rows with multiple column families and concurrently reading entire rows that expect all
 * column families.
 */
@Category(IntegrationTests.class)
public class IntegrationTestAcidGuarantees extends IntegrationTestBase {

  protected static final Log LOG = LogFactory.getLog(IntegrationTestAcidGuarantees.class);

  private static final int SERVER_COUNT = 1; // number of slaves for the smallest cluster
  public static final TableName TABLE_NAME =
          TableName.valueOf(IntegrationTestAcidGuarantees.class.getSimpleName());
  public static final byte [] FAMILY_A = Bytes.toBytes("A");
  public static final byte [] FAMILY_B = Bytes.toBytes("B");
  public static final byte [] FAMILY_C = Bytes.toBytes("C");
  public static final byte[][] FAMILIES = new byte[][] {
          FAMILY_A, FAMILY_B, FAMILY_C };

  public static int NUM_COLS_TO_CHECK = 50;

  protected IntegrationTestingUtility util;

  // **** extends IntegrationTestBase

  @Override
  public int runTestFromCommandLine() throws Exception {
    Configuration c = getConf();
    int millis = c.getInt("millis", 5000);
    int numWriters = c.getInt("numWriters", 50);
    int numGetters = c.getInt("numGetters", 2);
    int numScanners = c.getInt("numScanners", 2);
    int numUniqueRows = c.getInt("numUniqueRows", 3);
    runTestAtomicity(millis, numWriters, numGetters, numScanners, numUniqueRows, true);
    return 0;
  }

  @Override
  public void setUpCluster() throws Exception {
    // Set small flush size for minicluster so we exercise reseeking scanners
    util = getTestingUtil(getConf());
    util.initializeCluster(SERVER_COUNT);
    conf = getConf();
    conf.set(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, String.valueOf(128*1024));
    // prevent aggressive region split
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
            ConstantSizeRegionSplitPolicy.class.getName());
    this.setConf(util.getConfiguration());
  }

  @Override
  public TableName getTablename() {
    return TABLE_NAME;
  }

  @Override
  protected Set<String> getColumnFamilies() {
    return Sets.newHashSet(String.valueOf(FAMILY_A), String.valueOf(FAMILY_B),
            String.valueOf(FAMILY_C));
  }

  // **** core

  /**
   * Thread that does random full-row writes into a table.
   */
  public static class AtomicityWriter extends MultithreadedTestUtil.RepeatingTestThread {
    Random rand = new Random();
    byte data[] = new byte[10];
    byte targetRows[][];
    byte targetFamilies[][];
    Table table;
    AtomicLong numWritten = new AtomicLong();

    public AtomicityWriter(MultithreadedTestUtil.TestContext ctx, byte targetRows[][],
                           byte targetFamilies[][]) throws IOException {
      super(ctx);
      this.targetRows = targetRows;
      this.targetFamilies = targetFamilies;
      table = new HTable(ctx.getConf(), TABLE_NAME);
    }
    public void doAnAction() throws Exception {
      // Pick a random row to write into
      byte[] targetRow = targetRows[rand.nextInt(targetRows.length)];
      Put p = new Put(targetRow);
      rand.nextBytes(data);

      for (byte[] family : targetFamilies) {
        for (int i = 0; i < NUM_COLS_TO_CHECK; i++) {
          byte qualifier[] = Bytes.toBytes("col" + i);
          p.add(family, qualifier, data);
        }
      }
      table.put(p);
      numWritten.getAndIncrement();
    }
  }

  /**
   * Thread that does single-row reads in a table, looking for partially
   * completed rows.
   */
  public static class AtomicGetReader extends MultithreadedTestUtil.RepeatingTestThread {
    byte targetRow[];
    byte targetFamilies[][];
    Table table;
    int numVerified = 0;
    AtomicLong numRead = new AtomicLong();

    public AtomicGetReader(MultithreadedTestUtil.TestContext ctx, byte targetRow[],
                           byte targetFamilies[][]) throws IOException {
      super(ctx);
      this.targetRow = targetRow;
      this.targetFamilies = targetFamilies;
      table = new HTable(ctx.getConf(), TABLE_NAME);
    }

    public void doAnAction() throws Exception {
      Get g = new Get(targetRow);
      Result res = table.get(g);
      byte[] gotValue = null;
      if (res.getRow() == null) {
        // Trying to verify but we didn't find the row - the writing
        // thread probably just hasn't started writing yet, so we can
        // ignore this action
        return;
      }

      for (byte[] family : targetFamilies) {
        for (int i = 0; i < NUM_COLS_TO_CHECK; i++) {
          byte qualifier[] = Bytes.toBytes("col" + i);
          byte thisValue[] = res.getValue(family, qualifier);
          if (gotValue != null && !Bytes.equals(gotValue, thisValue)) {
            gotFailure(gotValue, res);
          }
          numVerified++;
          gotValue = thisValue;
        }
      }
      numRead.getAndIncrement();
    }

    private void gotFailure(byte[] expected, Result res) {
      StringBuilder msg = new StringBuilder();
      msg.append("Failed after ").append(numVerified).append("!");
      msg.append("Expected=").append(Bytes.toStringBinary(expected));
      msg.append("Got:\n");
      for (Cell kv : res.listCells()) {
        msg.append(kv.toString());
        msg.append(" val= ");
        msg.append(Bytes.toStringBinary(CellUtil.cloneValue(kv)));
        msg.append("\n");
      }
      throw new RuntimeException(msg.toString());
    }
  }

  /**
   * Thread that does full scans of the table looking for any partially completed
   * rows.
   */
  public static class AtomicScanReader extends MultithreadedTestUtil.RepeatingTestThread {
    byte targetFamilies[][];
    Table table;
    AtomicLong numScans = new AtomicLong();
    AtomicLong numRowsScanned = new AtomicLong();

    public AtomicScanReader(MultithreadedTestUtil.TestContext ctx,
                            byte targetFamilies[][]) throws IOException {
      super(ctx);
      this.targetFamilies = targetFamilies;
      table = new HTable(ctx.getConf(), TABLE_NAME);
    }

    public void doAnAction() throws Exception {
      Scan s = new Scan();
      for (byte[] family : targetFamilies) {
        s.addFamily(family);
      }
      ResultScanner scanner = table.getScanner(s);

      for (Result res : scanner) {
        byte[] gotValue = null;

        for (byte[] family : targetFamilies) {
          for (int i = 0; i < NUM_COLS_TO_CHECK; i++) {
            byte qualifier[] = Bytes.toBytes("col" + i);
            byte thisValue[] = res.getValue(family, qualifier);
            if (gotValue != null && !Bytes.equals(gotValue, thisValue)) {
              gotFailure(gotValue, res);
            }
            gotValue = thisValue;
          }
        }
        numRowsScanned.getAndIncrement();
      }
      numScans.getAndIncrement();
    }

    private void gotFailure(byte[] expected, Result res) {
      StringBuilder msg = new StringBuilder();
      msg.append("Failed after ").append(numRowsScanned).append("!");
      msg.append("Expected=").append(Bytes.toStringBinary(expected));
      msg.append("Got:\n");
      for (Cell kv : res.listCells()) {
        msg.append(kv.toString());
        msg.append(" val= ");
        msg.append(Bytes.toStringBinary(CellUtil.cloneValue(kv)));
        msg.append("\n");
      }
      throw new RuntimeException(msg.toString());
    }
  }

  public void runTestAtomicity(long millisToRun,
                               int numWriters,
                               int numGetters,
                               int numScanners,
                               int numUniqueRows) throws Exception {
    runTestAtomicity(millisToRun, numWriters, numGetters, numScanners, numUniqueRows, false);
  }

  private void createTableIfMissing()
          throws IOException {
    try {
      util.createTable(TABLE_NAME, FAMILIES);
    } catch (TableExistsException tee) {
    }
  }

  public void runTestAtomicity(long millisToRun,
                               int numWriters,
                               int numGetters,
                               int numScanners,
                               int numUniqueRows,
                               final boolean systemTest) throws Exception {
    createTableIfMissing();
    MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext(util.getConfiguration());

    byte rows[][] = new byte[numUniqueRows][];
    for (int i = 0; i < numUniqueRows; i++) {
      rows[i] = Bytes.toBytes("test_row_" + i);
    }

    List<AtomicityWriter> writers = Lists.newArrayList();
    for (int i = 0; i < numWriters; i++) {
      AtomicityWriter writer = new AtomicityWriter(
              ctx, rows, FAMILIES);
      writers.add(writer);
      ctx.addThread(writer);
    }
    // Add a flusher
    ctx.addThread(new MultithreadedTestUtil.RepeatingTestThread(ctx) {
      Admin admin = util.getHBaseAdmin();
      public void doAnAction() throws Exception {
        try {
          admin.flush(TABLE_NAME);
        } catch(IOException ioe) {
          LOG.warn("Ignoring exception while flushing: " + StringUtils.stringifyException(ioe));
        }
        // Flushing has been a source of ACID violations previously (see HBASE-2856), so ideally,
        // we would flush as often as possible.  On a running cluster, this isn't practical:
        // (1) we will cause a lot of load due to all the flushing and compacting
        // (2) we cannot change the flushing/compacting related Configuration options to try to
        // alleviate this
        // (3) it is an unrealistic workload, since no one would actually flush that often.
        // Therefore, let's flush every minute to have more flushes than usual, but not overload
        // the running cluster.
        if (systemTest) Thread.sleep(60000);
      }
    });

    List<AtomicGetReader> getters = Lists.newArrayList();
    for (int i = 0; i < numGetters; i++) {
      AtomicGetReader getter = new AtomicGetReader(
              ctx, rows[i % numUniqueRows], FAMILIES);
      getters.add(getter);
      ctx.addThread(getter);
    }

    List<AtomicScanReader> scanners = Lists.newArrayList();
    for (int i = 0; i < numScanners; i++) {
      AtomicScanReader scanner = new AtomicScanReader(ctx, FAMILIES);
      scanners.add(scanner);
      ctx.addThread(scanner);
    }

    ctx.startThreads();
    ctx.waitFor(millisToRun);
    ctx.stop();

    LOG.info("Finished test. Writers:");
    for (AtomicityWriter writer : writers) {
      LOG.info("  wrote " + writer.numWritten.get());
    }
    LOG.info("Readers:");
    for (AtomicGetReader reader : getters) {
      LOG.info("  read " + reader.numRead.get());
    }
    LOG.info("Scanners:");
    for (AtomicScanReader scanner : scanners) {
      LOG.info("  scanned " + scanner.numScans.get());
      LOG.info("  verified " + scanner.numRowsScanned.get() + " rows");
    }
  }

  @Test
  public void testGetAtomicity() throws Exception {
    runTestAtomicity(20000, 5, 5, 0, 3);
  }

  @Test
  public void testScanAtomicity() throws Exception {
    runTestAtomicity(20000, 5, 0, 5, 3);
  }

  @Test
  public void testMixedAtomicity() throws Exception {
    runTestAtomicity(20000, 5, 2, 2, 3);
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf, new IntegrationTestAcidGuarantees(), args);
    System.exit(ret);
  }


}


