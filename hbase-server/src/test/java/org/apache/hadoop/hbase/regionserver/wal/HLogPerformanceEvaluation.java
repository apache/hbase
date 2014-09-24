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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.ConsoleReporter;

/**
 * This class runs performance benchmarks for {@link HLog}.
 * See usage for this tool by running:
 * <code>$ hbase org.apache.hadoop.hbase.regionserver.wal.HLogPerformanceEvaluation -h</code>
 */
@InterfaceAudience.Private
public final class HLogPerformanceEvaluation extends Configured implements Tool {
  static final Log LOG = LogFactory.getLog(HLogPerformanceEvaluation.class.getName());
  private final MetricsRegistry metrics = new MetricsRegistry();
  private final Meter syncMeter =
    metrics.newMeter(HLogPerformanceEvaluation.class, "syncMeter", "syncs", TimeUnit.MILLISECONDS);
  private final Meter appendMeter =
    metrics.newMeter(HLogPerformanceEvaluation.class, "append", "bytes", TimeUnit.MILLISECONDS);

  private HBaseTestingUtility TEST_UTIL;

  static final String TABLE_NAME = "HLogPerformanceEvaluation";
  static final String QUALIFIER_PREFIX = "q";
  static final String FAMILY_PREFIX = "cf";

  private int numQualifiers = 1;
  private int valueSize = 512;
  private int keySize = 16;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    TEST_UTIL = new HBaseTestingUtility(conf);
  }

  /**
   * Perform HLog.append() of Put object, for the number of iterations requested.
   * Keys and Vaues are generated randomly, the number of column families,
   * qualifiers and key/value size is tunable by the user.
   */
  class HLogPutBenchmark implements Runnable {
    private final long numIterations;
    private final int numFamilies;
    private final boolean noSync;
    private final HRegion region;
    private final int syncInterval;
    private final HTableDescriptor htd;

    HLogPutBenchmark(final HRegion region, final HTableDescriptor htd,
        final long numIterations, final boolean noSync, final int syncInterval) {
      this.numIterations = numIterations;
      this.noSync = noSync;
      this.syncInterval = syncInterval;
      this.numFamilies = htd.getColumnFamilies().length;
      this.region = region;
      this.htd = htd;
    }

    @Override
    public void run() {
      byte[] key = new byte[keySize];
      byte[] value = new byte[valueSize];
      Random rand = new Random(Thread.currentThread().getId());
      HLog hlog = region.getLog();
      ArrayList<UUID> clusters = new ArrayList<UUID>();
      long nonce = HConstants.NO_NONCE;

      try {
        long startTime = System.currentTimeMillis();
        int lastSync = 0;
        for (int i = 0; i < numIterations; ++i) {
          Put put = setupPut(rand, key, value, numFamilies);
          long now = System.currentTimeMillis();
          WALEdit walEdit = new WALEdit();
          addFamilyMapToWALEdit(put.getFamilyCellMap(), walEdit);
          HRegionInfo hri = region.getRegionInfo();
          hlog.appendNoSync(hri, hri.getTable(), walEdit, clusters, now, htd,
            region.getSequenceId(), true, nonce, nonce);
          if (!this.noSync) {
            if (++lastSync >= this.syncInterval) {
              hlog.sync();
              lastSync = 0;
            }
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
    long numIterations = 1000000;
    int numFamilies = 1;
    int syncInterval = 0;
    boolean noSync = false;
    boolean verify = false;
    boolean verbose = false;
    boolean cleanup = true;
    boolean noclosefs = false;
    long roll = Long.MAX_VALUE;
    boolean compress = false;
    String cipher = null;
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
        } else if (cmd.equals("-syncInterval")) {
          syncInterval = Integer.parseInt(args[++i]);
        } else if (cmd.equals("-nosync")) {
          noSync = true;
        } else if (cmd.equals("-verify")) {
          verify = true;
        } else if (cmd.equals("-verbose")) {
          verbose = true;
        } else if (cmd.equals("-nocleanup")) {
          cleanup = false;
        } else if (cmd.equals("-noclosefs")) {
          noclosefs = true;
        } else if (cmd.equals("-roll")) {
          roll = Long.parseLong(args[++i]);
        } else if (cmd.equals("-compress")) {
          compress = true;
        } else if (cmd.equals("-encryption")) {
          cipher = args[++i];
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

    if (compress) {
      Configuration conf = getConf();
      conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);
    }

    if (cipher != null) {
      // Set up HLog for encryption
      Configuration conf = getConf();
      conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
      conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
      conf.setClass("hbase.regionserver.hlog.reader.impl", SecureProtobufLogReader.class,
        HLog.Reader.class);
      conf.setClass("hbase.regionserver.hlog.writer.impl", SecureProtobufLogWriter.class,
        HLog.Writer.class);
      conf.setBoolean(HConstants.ENABLE_WAL_ENCRYPTION, true);
      conf.set(HConstants.CRYPTO_WAL_ALGORITHM_CONF_KEY, cipher);
    }

    // Run HLog Performance Evaluation
    // First set the fs from configs.  In case we are on hadoop1
    FSUtils.setFsDefault(getConf(), FSUtils.getRootDir(getConf()));
    FileSystem fs = FileSystem.get(getConf());
    LOG.info("FileSystem: " + fs);
    try {
      if (rootRegionDir == null) {
        rootRegionDir = TEST_UTIL.getDataTestDirOnTestFS("HLogPerformanceEvaluation");
      }
      rootRegionDir = rootRegionDir.makeQualified(fs);
      cleanRegionRootDir(fs, rootRegionDir);
      // Initialize Table Descriptor
      HTableDescriptor htd = createHTableDescriptor(numFamilies);
      final long whenToRoll = roll;
      HLog hlog = new FSHLog(fs, rootRegionDir, "wals", getConf()) {
        int appends = 0;
        @Override
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

        @Override
        public void postSync() {
          super.postSync();
          syncMeter.mark();
        }

        @Override
        public void postAppend(List<Entry> entries) {
          super.postAppend(entries);
          int size = 0;
          for (Entry e: entries) size += e.getEdit().heapSize();
          appendMeter.mark(size);
        }
      };
      hlog.rollWriter();
      HRegion region = null;
      try {
        region = openRegion(fs, rootRegionDir, htd, hlog);
        ConsoleReporter.enable(this.metrics, 1, TimeUnit.SECONDS);
        long putTime =
          runBenchmark(new HLogPutBenchmark(region, htd, numIterations, noSync, syncInterval),
            numThreads);
        logBenchmarkResult("Summary: threads=" + numThreads + ", iterations=" + numIterations +
          ", syncInterval=" + syncInterval, numIterations * numThreads, putTime);
        
        if (region != null) {
          closeRegion(region);
          region = null;
        }
        if (verify) {
          Path dir = ((FSHLog) hlog).getDir();
          long editCount = 0;
          FileStatus [] fsss = fs.listStatus(dir);
          if (fsss.length == 0) throw new IllegalStateException("No WAL found");
          for (FileStatus fss: fsss) {
            Path p = fss.getPath();
            if (!fs.exists(p)) throw new IllegalStateException(p.toString());
            editCount += verify(p, verbose);
          }
          long expected = numIterations * numThreads;
          if (editCount != expected) {
            throw new IllegalStateException("Counted=" + editCount + ", expected=" + expected);
          }
        }
      } finally {
        if (region != null) closeRegion(region);
        // Remove the root dir for this test region
        if (cleanup) cleanRegionRootDir(fs, rootRegionDir);
      }
    } finally {
      // We may be called inside a test that wants to keep on using the fs.
      if (!noclosefs) fs.close();
    }

    return(0);
  }

  private static HTableDescriptor createHTableDescriptor(final int numFamilies) {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
    for (int i = 0; i < numFamilies; ++i) {
      HColumnDescriptor colDef = new HColumnDescriptor(FAMILY_PREFIX + i);
      htd.addFamily(colDef);
    }
    return htd;
  }

  /**
   * Verify the content of the WAL file.
   * Verify that the file has expected number of edits.
   * @param wal
   * @return Count of edits.
   * @throws IOException
   */
  private long verify(final Path wal, final boolean verbose) throws IOException {
    HLog.Reader reader = HLogFactory.createReader(wal.getFileSystem(getConf()), wal, getConf());
    long count = 0;
    Map<String, Long> sequenceIds = new HashMap<String, Long>();
    try {
      while (true) {
        Entry e = reader.next();
        if (e == null) {
          LOG.debug("Read count=" + count + " from " + wal);
          break;
        }
        count++;
        long seqid = e.getKey().getLogSeqNum();
        if (sequenceIds.containsKey(Bytes.toString(e.getKey().getEncodedRegionName()))) {
          // sequenceIds should be increasing for every regions
          if (sequenceIds.get(Bytes.toString(e.getKey().getEncodedRegionName())) >= seqid) {
            throw new IllegalStateException("wal = " + wal.getName() + ", " + "previous seqid = "
                + sequenceIds.get(Bytes.toString(e.getKey().getEncodedRegionName()))
                + ", current seqid = " + seqid);
          }
        }
        // update the sequence Id.
        sequenceIds.put(Bytes.toString(e.getKey().getEncodedRegionName()), seqid);
        if (verbose) LOG.info("seqid=" + seqid);
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
    System.err.println("  -nocleanup       Do NOT remove test data when done.");
    System.err.println("  -noclosefs       Do NOT close the filesystem when done.");
    System.err.println("  -nosync          Append without syncing");
    System.err.println("  -syncInterval <N> Append N edits and then sync. Default=0, i.e. sync every edit.");
    System.err.println("  -verify          Verify edits written in sequence");
    System.err.println("  -verbose         Output extra info; e.g. all edit seq ids when verifying");
    System.err.println("  -roll <N>        Roll the way every N appends");
    System.err.println("  -encryption <A>  Encrypt the WAL with algorithm A, e.g. AES");
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
    HRegionInfo regionInfo = new HRegionInfo(htd.getTableName());
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

  private void addFamilyMapToWALEdit(Map<byte[], List<Cell>> familyMap,
      WALEdit walEdit) {
    for (List<Cell> edits : familyMap.values()) {
      for (Cell cell : edits) {
        KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
        walEdit.add(kv);
      }
    }
  }

  private long runBenchmark(Runnable runnable, final int numThreads) throws InterruptedException {
    Thread[] threads = new Thread[numThreads];
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < numThreads; ++i) {
      threads[i] = new Thread(runnable, "t" + i);
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
  static int innerMain(final Configuration c, final String [] args) throws Exception {
    return ToolRunner.run(c, new HLogPerformanceEvaluation(), args);
  }

  public static void main(String[] args) throws Exception {
     System.exit(innerMain(HBaseConfiguration.create(), args));
  }
}
