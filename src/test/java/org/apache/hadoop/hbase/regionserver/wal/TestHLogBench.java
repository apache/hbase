/**
 *
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
import java.util.Random;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.hbase.*;
import org.apache.log4j.Level;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.ipc.HBaseRPC;

import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestHLogBench extends Configured implements Tool {

  static final Log LOG = LogFactory.getLog(TestHLogBench.class);
  private static final Random r = new Random();

  private static final byte [] FAMILY = Bytes.toBytes("hlogbenchFamily");

  // accumulate time here
  private static int totalTime = 0;
  private static Object lock = new Object();

  // the file system where to create the Hlog file
  protected FileSystem fs;

  // the number of threads and the number of iterations per thread
  private int numThreads = 300;
  private int numIterationsPerThread = 10000;

  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private Path regionRootDir =TEST_UTIL.getDataTestDir("TestHLogBench") ;

  private boolean appendNoSync = false;

  public TestHLogBench() {
    this(null);
  }

  private TestHLogBench(Configuration conf) {
    super(conf);
    fs = null;
  }

  /**
   * Initialize file system object
   */
  public void init() throws IOException {
    getConf().setQuietMode(true);
    if (this.fs == null) {
     this.fs = FileSystem.get(getConf());
    }
  }

  /**
   * Close down file system
   */
  public void close() throws IOException {
    if (fs != null) {
      fs.close();
      fs = null;
    }
  }

  /**
   * The main run method of TestHLogBench
   */
  public int run(String argv[]) throws Exception {

    int exitCode = -1;
    int i = 0;

    // verify that we have enough command line parameters
    if (argv.length < 4) {
      printUsage("");
      return exitCode;
    }

    // initialize LogBench
    try {
      init();
    } catch (HBaseRPC.VersionMismatch v) {
      LOG.warn("Version Mismatch between client and server" +
               "... command aborted.");
      return exitCode;
    } catch (IOException e) {
      LOG.warn("Bad connection to FS. command aborted.");
      return exitCode;
    }

    try {
      for (; i < argv.length; i++) {
        if ("-numThreads".equals(argv[i])) {
          i++;
          this.numThreads = Integer.parseInt(argv[i]);
        } else if ("-numIterationsPerThread".equals(argv[i])) {
          i++;
          this.numIterationsPerThread = Integer.parseInt(argv[i]);
        } else if ("-path".equals(argv[i])) {
          // get an absolute path using the default file system
          i++;
          this.regionRootDir = new Path(argv[i]);
          this.regionRootDir = regionRootDir.makeQualified(this.fs);
        } else if ("-nosync".equals(argv[i])) {
          this.appendNoSync = true;
        } else {
          printUsage(argv[i]);
          return exitCode;
        }
      }
    } catch (NumberFormatException nfe) {
      LOG.warn("Illegal numThreads or numIterationsPerThread, " +
               " a positive integer expected");
      throw nfe;
    }
    go();
    return 0;
  }

  private void go() throws IOException, InterruptedException {

    long start = System.currentTimeMillis();
    log("Running TestHLogBench with " + numThreads + " threads each doing " +
        numIterationsPerThread + " HLog appends " +
        (appendNoSync ? "nosync" : "sync") +
        " at rootDir " + regionRootDir);

    // Mock an HRegion
    byte [] tableName = Bytes.toBytes("table");
    byte [][] familyNames = new byte [][] { FAMILY };
    HTableDescriptor htd = new HTableDescriptor();
    htd.addFamily(new HColumnDescriptor(Bytes.toBytes("f1")));
    HRegion region = mockRegion(tableName, familyNames, regionRootDir);
    HLog hlog = region.getLog();

    // Spin up N threads to each perform M log operations
    LogWriter [] incrementors = new LogWriter[numThreads];
    for (int i=0; i<numThreads; i++) {
      incrementors[i] = new LogWriter(region, tableName, hlog, i, 
                                      numIterationsPerThread, 
                                      appendNoSync);
      incrementors[i].start();
    }

    // Wait for threads to finish
    for (int i=0; i<numThreads; i++) {
      //log("Waiting for #" + i + " to finish");
      incrementors[i].join();
    }

    // Output statistics
    long totalOps = numThreads * numIterationsPerThread;
    log("Operations per second " + ((totalOps * 1000L)/totalTime));
    log("Average latency in ms " + ((totalTime * 1000L)/totalOps));
  }

  /**
   * Displays format of commands.
   */
  private static void printUsage(String cmd) {
    String prefix = "Usage: java " + TestHLogBench.class.getSimpleName();
    System.err.println(prefix + cmd + 
                       " [-numThreads <number>] " +
                       " [-numIterationsPerThread <number>] " +
                       " [-path <path where region's root directory is created>]" +
                       " [-nosync]");
  }

  /**
   * A thread that writes data to an HLog
   */
  public static class LogWriter extends Thread {

    private final HRegion region;
    private final int threadNumber;
    private final int numIncrements;
    private final HLog hlog;
    private boolean appendNoSync;
    private byte[] tableName;

    private int count;

    public LogWriter(HRegion region, byte[] tableName,
        HLog log, int threadNumber,
        int numIncrements, boolean appendNoSync) {
      this.region = region;
      this.threadNumber = threadNumber;
      this.numIncrements = numIncrements;
      this.hlog = log;
      this.count = 0;
      this.appendNoSync = appendNoSync;
      this.tableName = tableName;
      setDaemon(true);
      //log("LogWriter[" + threadNumber + "] instantiated");
    }

    @Override
    public void run() {
      long now = System.currentTimeMillis();
      byte [] key = Bytes.toBytes("thisisakey");
      KeyValue kv = new KeyValue(key, now);
      WALEdit walEdit = new WALEdit();
      walEdit.add(kv);
      HRegionInfo hri = region.getRegionInfo();
      HTableDescriptor htd = new HTableDescriptor();
      htd.addFamily(new HColumnDescriptor(Bytes.toBytes("f1")));
      boolean isMetaRegion = false;
      long start = System.currentTimeMillis();
      for (int i=0; i<numIncrements; i++) {
        try {
          if (appendNoSync) {
            hlog.appendNoSync(hri, tableName, walEdit, 
                              HConstants.DEFAULT_CLUSTER_ID, now, htd);
          } else {
            hlog.append(hri, tableName, walEdit, now, htd);
          }
        } catch (IOException e) {
          log("Fatal exception: " + e);
          e.printStackTrace();
        }
        count++;
      }
      long tot = System.currentTimeMillis() - start;
      synchronized (lock) {
        totalTime += tot;   // update global statistics
      }

    }
  }

  private static void log(String string) {
    LOG.info(string);
  }

  private byte[][] makeBytes(int numArrays, int arraySize) {
    byte [][] bytes = new byte[numArrays][];
    for (int i=0; i<numArrays; i++) {
      bytes[i] = new byte[arraySize];
      r.nextBytes(bytes[i]);
    }
    return bytes;
  }

  /**
   * Create a dummy region
   */
  private HRegion mockRegion(byte[] tableName, byte[][] familyNames,
                             Path rootDir) throws IOException {

    HBaseTestingUtility htu = new HBaseTestingUtility();
    Configuration conf = htu.getConfiguration();
    conf.setBoolean("hbase.rs.cacheblocksonwrite", true);
    conf.setBoolean("hbase.hregion.use.incrementnew", true);
    conf.setBoolean("dfs.support.append", true);
    FileSystem fs = FileSystem.get(conf);
    int numQualifiers = 10;
    byte [][] qualifiers = new byte [numQualifiers][];
    for (int i=0; i<numQualifiers; i++) qualifiers[i] = Bytes.toBytes("qf" + i);
    int numRows = 10;
    byte [][] rows = new byte [numRows][];
    for (int i=0; i<numRows; i++) rows[i] = Bytes.toBytes("r" + i);

    // switch off debug message from Region server
    ((Log4JLogger)HRegion.LOG).getLogger().setLevel(Level.WARN);

    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (byte [] family : familyNames)
      htd.addFamily(new HColumnDescriptor(family));

    HRegionInfo hri = new HRegionInfo(tableName, Bytes.toBytes(0L), 
                                      Bytes.toBytes(0xffffffffL));
    if (fs.exists(rootDir)) {
      if (!fs.delete(rootDir, true)) {
        throw new IOException("Failed delete of " + rootDir);
      }
    }
    return HRegion.createHRegion(hri, rootDir, conf, htd);
  }

  @Test
  public void testLogPerformance() throws Exception {
    TestHLogBench bench = new TestHLogBench();
    int res;
    String[] argv = new String[7];
    argv[0] = "-numThreads";
    argv[1] = Integer.toString(100);
    argv[2] = "-numIterationsPerThread";
    argv[3] = Integer.toString(1000);
    argv[4] = "-path";
    argv[5] = TEST_UTIL.getDataTestDir() + "/HlogPerformance";
    argv[6] = "-nosync";
    try {
      res = ToolRunner.run(bench, argv);
    } finally {
      bench.close();
    }
  }

  public static void main(String[] argv) throws Exception {
    TestHLogBench bench = new TestHLogBench();
    int res;
    try {
      res = ToolRunner.run(bench, argv);
    } finally {
      bench.close();
    }
    System.exit(res);
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

