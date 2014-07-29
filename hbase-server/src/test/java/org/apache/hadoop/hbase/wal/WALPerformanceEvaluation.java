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

package org.apache.hadoop.hbase.wal;

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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.trace.SpanReceiverHost;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.htrace.Sampler;
import org.htrace.Trace;
import org.htrace.TraceScope;
import org.htrace.impl.ProbabilitySampler;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.ConsoleReporter;

// imports for things that haven't moved from regionserver.wal yet.
import org.apache.hadoop.hbase.regionserver.wal.SecureProtobufLogReader;
import org.apache.hadoop.hbase.regionserver.wal.SecureProtobufLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

/**
 * This class runs performance benchmarks for {@link WAL}.
 * See usage for this tool by running:
 * <code>$ hbase org.apache.hadoop.hbase.wal.WALPerformanceEvaluation -h</code>
 */
@InterfaceAudience.Private
public final class WALPerformanceEvaluation extends Configured implements Tool {
  static final Log LOG = LogFactory.getLog(WALPerformanceEvaluation.class.getName());
  private final MetricsRegistry metrics = new MetricsRegistry();
  private final Meter syncMeter =
    metrics.newMeter(WALPerformanceEvaluation.class, "syncMeter", "syncs", TimeUnit.MILLISECONDS);
  private final Histogram syncHistogram =
    metrics.newHistogram(WALPerformanceEvaluation.class, "syncHistogram", "nanos-between-syncs",
      true);
  private final Histogram syncCountHistogram =
      metrics.newHistogram(WALPerformanceEvaluation.class, "syncCountHistogram", "countPerSync",
        true);
  private final Meter appendMeter =
    metrics.newMeter(WALPerformanceEvaluation.class, "appendMeter", "bytes",
      TimeUnit.MILLISECONDS);
  private final Histogram latencyHistogram =
    metrics.newHistogram(WALPerformanceEvaluation.class, "latencyHistogram", "nanos", true);

  private HBaseTestingUtility TEST_UTIL;

  static final String TABLE_NAME = "WALPerformanceEvaluation";
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
   * Perform WAL.append() of Put object, for the number of iterations requested.
   * Keys and Vaues are generated randomly, the number of column families,
   * qualifiers and key/value size is tunable by the user.
   */
  class WALPutBenchmark implements Runnable {
    private final long numIterations;
    private final int numFamilies;
    private final boolean noSync;
    private final HRegion region;
    private final int syncInterval;
    private final HTableDescriptor htd;
    private final Sampler loopSampler;

    WALPutBenchmark(final HRegion region, final HTableDescriptor htd,
        final long numIterations, final boolean noSync, final int syncInterval,
        final double traceFreq) {
      this.numIterations = numIterations;
      this.noSync = noSync;
      this.syncInterval = syncInterval;
      this.numFamilies = htd.getColumnFamilies().length;
      this.region = region;
      this.htd = htd;
      String spanReceivers = getConf().get("hbase.trace.spanreceiver.classes");
      if (spanReceivers == null || spanReceivers.isEmpty()) {
        loopSampler = Sampler.NEVER;
      } else {
        if (traceFreq <= 0.0) {
          LOG.warn("Tracing enabled but traceFreq=0.");
          loopSampler = Sampler.NEVER;
        } else if (traceFreq >= 1.0) {
          loopSampler = Sampler.ALWAYS;
          if (numIterations > 1000) {
            LOG.warn("Full tracing of all iterations will produce a lot of data. Be sure your"
              + " SpanReciever can keep up.");
          }
        } else {
          loopSampler = new ProbabilitySampler(traceFreq);
        }
      }
    }

    @Override
    public void run() {
      byte[] key = new byte[keySize];
      byte[] value = new byte[valueSize];
      Random rand = new Random(Thread.currentThread().getId());
      WAL wal = region.getWAL();

      TraceScope threadScope =
        Trace.startSpan("WALPerfEval." + Thread.currentThread().getName());
      try {
        long startTime = System.currentTimeMillis();
        int lastSync = 0;
        for (int i = 0; i < numIterations; ++i) {
          assert Trace.currentSpan() == threadScope.getSpan() : "Span leak detected.";
          TraceScope loopScope = Trace.startSpan("runLoopIter" + i, loopSampler);
          try {
            long now = System.nanoTime();
            Put put = setupPut(rand, key, value, numFamilies);
            WALEdit walEdit = new WALEdit();
            addFamilyMapToWALEdit(put.getFamilyCellMap(), walEdit);
            HRegionInfo hri = region.getRegionInfo();
            final WALKey logkey = new WALKey(hri.getEncodedNameAsBytes(), hri.getTable(), now);
            wal.append(htd, hri, logkey, walEdit, region.getSequenceId(), true, null);
            if (!this.noSync) {
              if (++lastSync >= this.syncInterval) {
                wal.sync();
                lastSync = 0;
              }
            }
            latencyHistogram.update(System.nanoTime() - now);
          } finally {
            loopScope.close();
          }
        }
        long totalTime = (System.currentTimeMillis() - startTime);
        logBenchmarkResult(Thread.currentThread().getName(), numIterations, totalTime);
      } catch (Exception e) {
        LOG.error(getClass().getSimpleName() + " Thread failed", e);
      } finally {
        threadScope.close();
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
    int numRegions = 1;
    String spanReceivers = getConf().get("hbase.trace.spanreceiver.classes");
    boolean trace = spanReceivers != null && !spanReceivers.isEmpty();
    double traceFreq = 1.0;
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
        } else if (cmd.equals("-regions")) {
          numRegions = Integer.parseInt(args[++i]);
        } else if (cmd.equals("-traceFreq")) {
          traceFreq = Double.parseDouble(args[++i]);
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
      // Set up WAL for encryption
      Configuration conf = getConf();
      conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
      conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
      conf.setClass("hbase.regionserver.hlog.reader.impl", SecureProtobufLogReader.class,
        WAL.Reader.class);
      conf.setClass("hbase.regionserver.hlog.writer.impl", SecureProtobufLogWriter.class,
        Writer.class);
      conf.setBoolean(HConstants.ENABLE_WAL_ENCRYPTION, true);
      conf.set(HConstants.CRYPTO_WAL_ALGORITHM_CONF_KEY, cipher);
    }

    if (numThreads < numRegions) {
      LOG.warn("Number of threads is less than the number of regions; some regions will sit idle.");
    }

    // Internal config. goes off number of threads; if more threads than handlers, stuff breaks.
    // In regionserver, number of handlers == number of threads.
    getConf().setInt(HConstants.REGION_SERVER_HANDLER_COUNT, numThreads);

    // Run WAL Performance Evaluation
    // First set the fs from configs.  In case we are on hadoop1
    FSUtils.setFsDefault(getConf(), FSUtils.getRootDir(getConf()));
    FileSystem fs = FileSystem.get(getConf());
    LOG.info("FileSystem: " + fs);

    SpanReceiverHost receiverHost = trace ? SpanReceiverHost.getInstance(getConf()) : null;
    TraceScope scope = Trace.startSpan("WALPerfEval", trace ? Sampler.ALWAYS : Sampler.NEVER);

    try {
      if (rootRegionDir == null) {
        rootRegionDir = TEST_UTIL.getDataTestDirOnTestFS("WALPerformanceEvaluation");
      }
      rootRegionDir = rootRegionDir.makeQualified(fs);
      cleanRegionRootDir(fs, rootRegionDir);
      FSUtils.setRootDir(getConf(), rootRegionDir);
      final WALFactory wals = new WALFactory(getConf(), null, "wals");
      final HRegion[] regions = new HRegion[numRegions];
      final Runnable[] benchmarks = new Runnable[numRegions];

      try {
        for(int i = 0; i < numRegions; i++) {
          // Initialize Table Descriptor
          // a table per desired region means we can avoid carving up the key space
          final HTableDescriptor htd = createHTableDescriptor(i, numFamilies);
          regions[i] = openRegion(fs, rootRegionDir, htd, wals, roll);
          benchmarks[i] = Trace.wrap(new WALPutBenchmark(regions[i], htd, numIterations, noSync,
              syncInterval, traceFreq));
        }
        ConsoleReporter.enable(this.metrics, 30, TimeUnit.SECONDS);
        long putTime = runBenchmark(benchmarks, numThreads);
        logBenchmarkResult("Summary: threads=" + numThreads + ", iterations=" + numIterations +
          ", syncInterval=" + syncInterval, numIterations * numThreads, putTime);
        
        for (int i = 0; i < numRegions; i++) {
          if (regions[i] != null) {
            closeRegion(regions[i]);
            regions[i] = null;
          }
        }
        if (verify) {
          Path dir = new Path(FSUtils.getRootDir(getConf()),
              DefaultWALProvider.getWALDirectoryName("wals"));
          long editCount = 0;
          FileStatus [] fsss = fs.listStatus(dir);
          if (fsss.length == 0) throw new IllegalStateException("No WAL found");
          for (FileStatus fss: fsss) {
            Path p = fss.getPath();
            if (!fs.exists(p)) throw new IllegalStateException(p.toString());
            editCount += verify(wals, p, verbose);
          }
          long expected = numIterations * numThreads;
          if (editCount != expected) {
            throw new IllegalStateException("Counted=" + editCount + ", expected=" + expected);
          }
        }
      } finally {
        for (int i = 0; i < numRegions; i++) {
          if (regions[i] != null) {
            closeRegion(regions[i]);
          }
        }
        wals.shutdown();
        // Remove the root dir for this test region
        if (cleanup) cleanRegionRootDir(fs, rootRegionDir);
      }
    } finally {
      // We may be called inside a test that wants to keep on using the fs.
      if (!noclosefs) fs.close();
      scope.close();
      if (receiverHost != null) receiverHost.closeReceivers();
    }

    return(0);
  }

  private static HTableDescriptor createHTableDescriptor(final int regionNum,
      final int numFamilies) {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(TABLE_NAME + ":" + regionNum));
    for (int i = 0; i < numFamilies; ++i) {
      HColumnDescriptor colDef = new HColumnDescriptor(FAMILY_PREFIX + i);
      htd.addFamily(colDef);
    }
    return htd;
  }

  /**
   * Verify the content of the WAL file.
   * Verify that the file has expected number of edits.
   * @param wals may not be null
   * @param wal
   * @return Count of edits.
   * @throws IOException
   */
  private long verify(final WALFactory wals, final Path wal, final boolean verbose)
      throws IOException {
    WAL.Reader reader = wals.createReader(wal.getFileSystem(getConf()), wal);
    long count = 0;
    Map<String, Long> sequenceIds = new HashMap<String, Long>();
    try {
      while (true) {
        WAL.Entry e = reader.next();
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
    System.err.println("  -regions <N>     Number of regions to open in the WAL. Default: 1");
    System.err.println("  -iterations <N>  Number of iterations per thread.");
    System.err.println("  -path <PATH>     Path where region's root directory is created.");
    System.err.println("  -families <N>    Number of column families to write.");
    System.err.println("  -qualifiers <N>  Number of qualifiers to write.");
    System.err.println("  -keySize <N>     Row key size in byte.");
    System.err.println("  -valueSize <N>   Row/Col value size in byte.");
    System.err.println("  -nocleanup       Do NOT remove test data when done.");
    System.err.println("  -noclosefs       Do NOT close the filesystem when done.");
    System.err.println("  -nosync          Append without syncing");
    System.err.println("  -syncInterval <N> Append N edits and then sync. " +
      "Default=0, i.e. sync every edit.");
    System.err.println("  -verify          Verify edits written in sequence");
    System.err.println("  -verbose         Output extra info; " +
      "e.g. all edit seq ids when verifying");
    System.err.println("  -roll <N>        Roll the way every N appends");
    System.err.println("  -encryption <A>  Encrypt the WAL with algorithm A, e.g. AES");
    System.err.println("  -traceFreq <N>   Rate of trace sampling. Default: 1.0, " +
      "only respected when tracing is enabled, ie -Dhbase.trace.spanreceiver.classes=...");
    System.err.println("");
    System.err.println("Examples:");
    System.err.println("");
    System.err.println(" To run 100 threads on hdfs with log rolling every 10k edits and " +
      "verification afterward do:");
    System.err.println(" $ ./bin/hbase org.apache.hadoop.hbase.wal." +
      "WALPerformanceEvaluation \\");
    System.err.println("    -conf ./core-site.xml -path hdfs://example.org:7000/tmp " +
      "-threads 100 -roll 10000 -verify");
    System.exit(1);
  }

  private HRegion openRegion(final FileSystem fs, final Path dir, final HTableDescriptor htd,
      final WALFactory wals, final long whenToRoll) throws IOException {
    // Initialize HRegion
    HRegionInfo regionInfo = new HRegionInfo(htd.getTableName());
    // Initialize WAL
    final WAL wal = wals.getWAL(regionInfo.getEncodedNameAsBytes());
    wal.registerWALActionsListener(new WALActionsListener.Base() {
      private int appends = 0;

      @Override
      public void visitLogEntryBeforeWrite(HTableDescriptor htd, WALKey logKey,
          WALEdit logEdit) {
        this.appends++;
        if (this.appends % whenToRoll == 0) {
          LOG.info("Rolling after " + appends + " edits");
          // We used to do explicit call to rollWriter but changed it to a request
          // to avoid dead lock (there are less threads going on in this class than
          // in the regionserver -- regionserver does not have the issue).
          // TODO I think this means no rolling actually happens; the request relies on there
          // being a LogRoller.
          DefaultWALProvider.requestLogRoll(wal);
        }
      }

      @Override
      public void postSync(final long timeInNanos, final int handlerSyncs) {
        syncMeter.mark();
        syncHistogram.update(timeInNanos);
        syncCountHistogram.update(handlerSyncs);
      }

      @Override
      public void postAppend(final long size, final long elapsedTime) {
        appendMeter.mark(size);
      }
    });
    wal.rollWriter();
     
    return HRegion.createHRegion(regionInfo, dir, getConf(), htd, wal);
  }

  private void closeRegion(final HRegion region) throws IOException {
    if (region != null) {
      region.close();
      WAL wal = region.getWAL();
      if (wal != null) {
        wal.shutdown();
      }
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
        walEdit.add(cell);
      }
    }
  }

  private long runBenchmark(Runnable[] runnable, final int numThreads) throws InterruptedException {
    Thread[] threads = new Thread[numThreads];
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < numThreads; ++i) {
      threads[i] = new Thread(runnable[i%runnable.length], "t" + i + ",r" + (i%runnable.length));
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
    return ToolRunner.run(c, new WALPerformanceEvaluation(), args);
  }

  public static void main(String[] args) throws Exception {
     System.exit(innerMain(HBaseConfiguration.create(), args));
  }
}
