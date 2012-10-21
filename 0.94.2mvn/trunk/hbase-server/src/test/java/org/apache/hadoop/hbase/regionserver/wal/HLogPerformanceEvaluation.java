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

package org.apache.hadoop.hbase.regionserver.wal;

import java.util.Map;
import java.util.List;
import java.util.Random;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.classification.InterfaceAudience;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.regionserver.wal.HLogFactory;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

/**
 * This class runs performance benchmarks for {@link HLog}.
 * See usage for this tool by running:
 * <code>$ hbase org.apache.hadoop.hbase.regionserver.wal.HLogPerformanceEvaluation -h</code>
 */
@InterfaceAudience.Private
public final class HLogPerformanceEvaluation extends Configured implements Tool {
  static final Log LOG = LogFactory.getLog(HLogPerformanceEvaluation.class.getName());

  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  static final String TABLE_NAME = "HLogPerformanceEvaluation";
  static final String QUALIFIER_PREFIX = "q";
  static final String FAMILY_PREFIX = "cf";

  private int numQualifiers = 1;
  private int valueSize = 512;
  private int keySize = 16;

  /**
   * Perform HLog.append() of Put object, for the number of iterations requested.
   * Keys and Vaues are generated randomly, the number of column familes,
   * qualifiers and key/value size is tunable by the user.
   */
  class HLogPutBenchmark implements Runnable {
    private final long numIterations;
    private final int numFamilies;
    private final boolean noSync;
    private final HRegion region;
    private final HTableDescriptor htd;

    HLogPutBenchmark(final HRegion region, final HTableDescriptor htd,
        final long numIterations, final boolean noSync) {
      this.numIterations = numIterations;
      this.noSync = noSync;
      this.numFamilies = htd.getColumnFamilies().length;
      this.region = region;
      this.htd = htd;
    }

    public void run() {
      byte[] key = new byte[keySize];
      byte[] value = new byte[valueSize];
      Random rand = new Random(Thread.currentThread().getId());
      HLog hlog = region.getLog();

      try {
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numIterations; ++i) {
          Put put = setupPut(rand, key, value, numFamilies);
          long now = System.currentTimeMillis();
          WALEdit walEdit = new WALEdit();
          addFamilyMapToWALEdit(put.getFamilyMap(), walEdit);
          HRegionInfo hri = region.getRegionInfo();
          if (this.noSync) {
            hlog.appendNoSync(hri, hri.getTableName(), walEdit,
                              HConstants.DEFAULT_CLUSTER_ID, now, htd);
          } else {
            hlog.append(hri, hri.getTableName(), walEdit, now, htd);
          }
        }
        long totalTime = (System.currentTimeMillis() - startTime);
        logBenchmarkResult(Thread.currentThread().getName(), numIterations, totalTime);
      } catch (Exception e) {
        LOG.error(getClass().getSimpleName() + " Thread failed", e);
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Path rootRegionDir = null;
    int numThreads = 1;
    long numIterations = 10000;
    int numFamilies = 1;
    boolean noSync = false;
    boolean verify = false;
    boolean verbose = false;
    long roll = Long.MAX_VALUE;
    // Process command line args
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      try {
        if (cmd.equals("-threads")) {
          numThreads = Integer.parseInt(args[++i]);
        } else if (cmd.equals("-iterations")) {
          numIterations = Long.parseLong(args[++i]);
        } else if (cmd.equals("-path")) {
          rootRegionDir = new Path(args[++i]);
        } else if (cmd.equals("-families")) {
          numFamilies = Integer.parseInt(args[++i]);
        } else if (cmd.equals("-qualifiers")) {
          numQualifiers = Integer.parseInt(args[++i]);
        } else if (cmd.equals("-keySize")) {
          keySize = Integer.parseInt(args[++i]);
        } else if (cmd.equals("-valueSize")) {
          valueSize = Integer.parseInt(args[++i]);
        } else if (cmd.equals("-nosync")) {
          noSync = true;
        } else if (cmd.equals("-verify")) {
          verify = true;
        } else if (cmd.equals("-verbose")) {
          verbose = true;
        } else if (cmd.equals("-roll")) {
          roll = Long.parseLong(args[++i]);
        } else if (cmd.equals("-h")) {
          printUsageAndExit();
        } else if (cmd.equals("--help")) {
          printUsageAndExit();
        } else {
          System.err.println("UNEXPECTED: " + cmd);
          printUsageAndExit();
        }
      } catch (Exception e) {
        printUsageAndExit();
      }
    }

    // Run HLog Performance Evaluation
    FileSystem fs = FileSystem.get(getConf());
    LOG.info("" + fs);
    try {
      if (rootRegionDir == null) {
        rootRegionDir = TEST_UTIL.getDataTestDir("HLogPerformanceEvaluation");
      }
      rootRegionDir = rootRegionDir.makeQualified(fs);
      cleanRegionRootDir(fs, rootRegionDir);
      // Initialize Table Descriptor
      HTableDescriptor htd = createHTableDescriptor(numFamilies);
      final long whenToRoll = roll;
      HLog hlog = new FSHLog(fs, rootRegionDir, "wals", getConf()) {
        int appends = 0;
        protected void doWrite(HRegionInfo info, HLogKey logKey, WALEdit logEdit,
            HTableDescriptor htd)
        throws IOException {
          this.appends++;
          if (this.appends % whenToRoll == 0) {
            LOG.info("Rolling after " + appends + " edits");
            rollWriter();
          }
          super.doWrite(info, logKey, logEdit, htd);
        };
      };
      hlog.rollWriter();
      HRegion region = null;
      try {
        region = openRegion(fs, rootRegionDir, htd, hlog);
        long putTime = runBenchmark(new HLogPutBenchmark(region, htd, numIterations, noSync), numThreads);
        logBenchmarkResult("Summary: threads=" + numThreads + ", iterations=" + numIterations,
          numIterations * numThreads, putTime);
        if (region != null) {
          closeRegion(region);
          region = null;
        }
        if (verify) {
          Path dir = ((FSHLog) hlog).getDir();
          long editCount = 0;
          for (FileStatus fss: fs.listStatus(dir)) {
            editCount += verify(fss.getPath(), verbose);
          }
          long expected = numIterations * numThreads;
          if (editCount != expected) {
            throw new IllegalStateException("Counted=" + editCount + ", expected=" + expected);
          }
        }
      } finally {
        if (region != null) closeRegion(region);
        // Remove the root dir for this test region
        cleanRegionRootDir(fs, rootRegionDir);
      }
    } finally {
      fs.close();
    }

    return(0);
  }

  private static HTableDescriptor createHTableDescriptor(final int numFamilies) {
    HTableDescriptor htd = new HTableDescriptor(TABLE_NAME);
    for (int i = 0; i < numFamilies; ++i) {
      HColumnDescriptor colDef = new HColumnDescriptor(FAMILY_PREFIX + i);
      htd.addFamily(colDef);
    }
    return htd;
  }

  /**
   * Verify the content of the WAL file.
   * Verify that sequenceids are ascending and that the file has expected number
   * of edits.
   * @param wal
   * @return Count of edits.
   * @throws IOException
   */
  private long verify(final Path wal, final boolean verbose) throws IOException {
    HLog.Reader reader = HLogFactory.createReader(wal.getFileSystem(getConf()), 
        wal, getConf());
    long previousSeqid = -1;
    long count = 0;
    try {
      while (true) {
        Entry e = reader.next();
        if (e == null) break;
        count++;
        long seqid = e.getKey().getLogSeqNum();
        if (verbose) LOG.info("seqid=" + seqid);
        if (previousSeqid >= seqid) {
          throw new IllegalStateException("wal=" + wal.getName() +
            ", previousSeqid=" + previousSeqid + ", seqid=" + seqid);
        }
        previousSeqid = seqid;
      }
    } finally {
      reader.close();
    }
    return count;
  }

  private static void logBenchmarkResult(String testName, long numTests, long totalTime) {
    float tsec = totalTime / 1000.0f;
    LOG.info(String.format("%s took %.3fs %.3fops/s", testName, tsec, numTests / tsec));
  }

  private void printUsageAndExit() {
    System.err.printf("Usage: bin/hbase %s [options]\n", getClass().getName());
    System.err.println(" where [options] are:");
    System.err.println("  -h|-help         Show this help and exit.");
    System.err.println("  -threads <N>     Number of threads writing on the WAL.");
    System.err.println("  -iterations <N>  Number of iterations per thread.");
    System.err.println("  -path <PATH>     Path where region's root directory is created.");
    System.err.println("  -families <N>    Number of column families to write.");
    System.err.println("  -qualifiers <N>  Number of qualifiers to write.");
    System.err.println("  -keySize <N>     Row key size in byte.");
    System.err.println("  -valueSize <N>   Row/Col value size in byte.");
    System.err.println("  -nosync          Append without syncing");
    System.err.println("  -verify          Verify edits written in sequence");
    System.err.println("  -verbose         Output extra info; e.g. all edit seq ids when verifying");
    System.err.println("  -roll <N>        Roll the way every N appends");
    System.err.println("");
    System.err.println("Examples:");
    System.err.println("");
    System.err.println(" To run 100 threads on hdfs with log rolling every 10k edits and verification afterward do:");
    System.err.println(" $ ./bin/hbase org.apache.hadoop.hbase.regionserver.wal.HLogPerformanceEvaluation \\");
    System.err.println("    -conf ./core-site.xml -path hdfs://example.org:7000/tmp -threads 100 -roll 10000 -verify");
    System.exit(1);
  }

  private HRegion openRegion(final FileSystem fs, final Path dir, final HTableDescriptor htd, final HLog hlog)
  throws IOException {
    // Initialize HRegion
    HRegionInfo regionInfo = new HRegionInfo(htd.getName());
    return HRegion.createHRegion(regionInfo, dir, getConf(), htd, hlog);
  }

  private void closeRegion(final HRegion region) throws IOException {
    if (region != null) {
      region.close();
      HLog wal = region.getLog();
      if (wal != null) wal.close();
    }
  }

  private void cleanRegionRootDir(final FileSystem fs, final Path dir) throws IOException {
    if (fs.exists(dir)) {
      fs.delete(dir, true);
    }
  }

  private Put setupPut(Random rand, byte[] key, byte[] value, final int numFamilies) {
    rand.nextBytes(key);
    Put put = new Put(key);
    for (int cf = 0; cf < numFamilies; ++cf) {
      for (int q = 0; q < numQualifiers; ++q) {
        rand.nextBytes(value);
        put.add(Bytes.toBytes(FAMILY_PREFIX + cf), Bytes.toBytes(QUALIFIER_PREFIX + q), value);
      }
    }
    return put;
  }

  private void addFamilyMapToWALEdit(Map<byte[], List<KeyValue>> familyMap, WALEdit walEdit) {
    for (List<KeyValue> edits : familyMap.values()) {
      for (KeyValue kv : edits) {
        walEdit.add(kv);
      }
    }
  }

  private long runBenchmark(Runnable runnable, final int numThreads) throws InterruptedException {
    Thread[] threads = new Thread[numThreads];
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < numThreads; ++i) {
      threads[i] = new Thread(runnable);
      threads[i].start();
    }
    for (Thread t : threads) t.join();
    long endTime = System.currentTimeMillis();
    return(endTime - startTime);
  }

  /**
   * The guts of the {@link #main} method.
   * Call this method to avoid the {@link #main(String[])} System.exit.
   * @param args
   * @return errCode
   * @throws Exception 
   */
  static int innerMain(final String [] args) throws Exception {
    return ToolRunner.run(HBaseConfiguration.create(), new HLogPerformanceEvaluation(), args);
  }

  public static void main(String[] args) throws Exception {
     System.exit(innerMain(args));
  }
}
