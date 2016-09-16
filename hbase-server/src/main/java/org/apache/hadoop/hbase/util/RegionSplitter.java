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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.NoServerForRegionException;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.fs.RegionStorage;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * The {@link RegionSplitter} class provides several utilities to help in the
 * administration lifecycle for developers who choose to manually split regions
 * instead of having HBase handle that automatically. The most useful utilities
 * are:
 * <p>
 * <ul>
 * <li>Create a table with a specified number of pre-split regions
 * <li>Execute a rolling split of all regions on an existing table
 * </ul>
 * <p>
 * Both operations can be safely done on a live server.
 * <p>
 * <b>Question:</b> How do I turn off automatic splitting? <br>
 * <b>Answer:</b> Automatic splitting is determined by the configuration value
 * <i>HConstants.HREGION_MAX_FILESIZE</i>. It is not recommended that you set this
 * to Long.MAX_VALUE in case you forget about manual splits. A suggested setting
 * is 100GB, which would result in &gt; 1hr major compactions if reached.
 * <p>
 * <b>Question:</b> Why did the original authors decide to manually split? <br>
 * <b>Answer:</b> Specific workload characteristics of our use case allowed us
 * to benefit from a manual split system.
 * <p>
 * <ul>
 * <li>Data (~1k) that would grow instead of being replaced
 * <li>Data growth was roughly uniform across all regions
 * <li>OLTP workload. Data loss is a big deal.
 * </ul>
 * <p>
 * <b>Question:</b> Why is manual splitting good for this workload? <br>
 * <b>Answer:</b> Although automated splitting is not a bad option, there are
 * benefits to manual splitting.
 * <p>
 * <ul>
 * <li>With growing amounts of data, splits will continually be needed. Since
 * you always know exactly what regions you have, long-term debugging and
 * profiling is much easier with manual splits. It is hard to trace the logs to
 * understand region level problems if it keeps splitting and getting renamed.
 * <li>Data offlining bugs + unknown number of split regions == oh crap! If an
 * WAL or StoreFile was mistakenly unprocessed by HBase due to a weird bug and
 * you notice it a day or so later, you can be assured that the regions
 * specified in these files are the same as the current regions and you have
 * less headaches trying to restore/replay your data.
 * <li>You can finely tune your compaction algorithm. With roughly uniform data
 * growth, it's easy to cause split / compaction storms as the regions all
 * roughly hit the same data size at the same time. With manual splits, you can
 * let staggered, time-based major compactions spread out your network IO load.
 * </ul>
 * <p>
 * <b>Question:</b> What's the optimal number of pre-split regions to create? <br>
 * <b>Answer:</b> Mileage will vary depending upon your application.
 * <p>
 * The short answer for our application is that we started with 10 pre-split
 * regions / server and watched our data growth over time. It's better to err on
 * the side of too little regions and rolling split later.
 * <p>
 * The more complicated answer is that this depends upon the largest storefile
 * in your region. With a growing data size, this will get larger over time. You
 * want the largest region to be just big enough that the
 * {@link org.apache.hadoop.hbase.regionserver.HStore} compact
 * selection algorithm only compacts it due to a timed major. If you don't, your
 * cluster can be prone to compaction storms as the algorithm decides to run
 * major compactions on a large series of regions all at once. Note that
 * compaction storms are due to the uniform data growth, not the manual split
 * decision.
 * <p>
 * If you pre-split your regions too thin, you can increase the major compaction
 * interval by configuring HConstants.MAJOR_COMPACTION_PERIOD. If your data size
 * grows too large, use this script to perform a network IO safe rolling split
 * of all regions.
 */
@InterfaceAudience.Private
public class RegionSplitter {
  private static final Log LOG = LogFactory.getLog(RegionSplitter.class);

  /**
   * A generic interface for the RegionSplitter code to use for all it's
   * functionality. Note that the original authors of this code use
   * {@link HexStringSplit} to partition their table and set it as default, but
   * provided this for your custom algorithm. To use, create a new derived class
   * from this interface and call {@link RegionSplitter#createPresplitTable} or
   * RegionSplitter#rollingSplit(TableName, SplitAlgorithm, Configuration) with the
   * argument splitClassName giving the name of your class.
   */
  public interface SplitAlgorithm {
    /**
     * Split a pre-existing region into 2 regions.
     *
     * @param start
     *          first row (inclusive)
     * @param end
     *          last row (exclusive)
     * @return the split row to use
     */
    byte[] split(byte[] start, byte[] end);

    /**
     * Split an entire table.
     *
     * @param numRegions
     *          number of regions to split the table into
     *
     * @throws RuntimeException
     *           user input is validated at this time. may throw a runtime
     *           exception in response to a parse failure
     * @return array of split keys for the initial regions of the table. The
     *         length of the returned array should be numRegions-1.
     */
    byte[][] split(int numRegions);

    /**
     * In HBase, the first row is represented by an empty byte array. This might
     * cause problems with your split algorithm or row printing. All your APIs
     * will be passed firstRow() instead of empty array.
     *
     * @return your representation of your first row
     */
    byte[] firstRow();

    /**
     * In HBase, the last row is represented by an empty byte array. This might
     * cause problems with your split algorithm or row printing. All your APIs
     * will be passed firstRow() instead of empty array.
     *
     * @return your representation of your last row
     */
    byte[] lastRow();

    /**
     * In HBase, the last row is represented by an empty byte array. Set this
     * value to help the split code understand how to evenly divide the first
     * region.
     *
     * @param userInput
     *          raw user input (may throw RuntimeException on parse failure)
     */
    void setFirstRow(String userInput);

    /**
     * In HBase, the last row is represented by an empty byte array. Set this
     * value to help the split code understand how to evenly divide the last
     * region. Note that this last row is inclusive for all rows sharing the
     * same prefix.
     *
     * @param userInput
     *          raw user input (may throw RuntimeException on parse failure)
     */
    void setLastRow(String userInput);

    /**
     * @param input
     *          user or file input for row
     * @return byte array representation of this row for HBase
     */
    byte[] strToRow(String input);

    /**
     * @param row
     *          byte array representing a row in HBase
     * @return String to use for debug &amp; file printing
     */
    String rowToStr(byte[] row);

    /**
     * @return the separator character to use when storing / printing the row
     */
    String separator();

    /**
     * Set the first row
     * @param userInput byte array of the row key.
     */
    void setFirstRow(byte[] userInput);

    /**
     * Set the last row
     * @param userInput byte array of the row key.
     */
    void setLastRow(byte[] userInput);
  }

  /**
   * The main function for the RegionSplitter application. Common uses:
   * <p>
   * <ul>
   * <li>create a table named 'myTable' with 60 pre-split regions containing 2
   * column families 'test' &amp; 'rs', assuming the keys are hex-encoded ASCII:
   * <ul>
   * <li>bin/hbase org.apache.hadoop.hbase.util.RegionSplitter -c 60 -f test:rs
   * myTable HexStringSplit
   * </ul>
   * <li>perform a rolling split of 'myTable' (i.e. 60 =&gt; 120 regions), # 2
   * outstanding splits at a time, assuming keys are uniformly distributed
   * bytes:
   * <ul>
   * <li>bin/hbase org.apache.hadoop.hbase.util.RegionSplitter -r -o 2 myTable
   * UniformSplit
   * </ul>
   * </ul>
   *
   * There are two SplitAlgorithms built into RegionSplitter, HexStringSplit
   * and UniformSplit. These are different strategies for choosing region
   * boundaries. See their source code for details.
   *
   * @param args
   *          Usage: RegionSplitter &lt;TABLE&gt; &lt;SPLITALGORITHM&gt;
   *          &lt;-c &lt;# regions&gt; -f &lt;family:family:...&gt; | -r
   *          [-o &lt;# outstanding splits&gt;]&gt;
   *          [-D &lt;conf.param=value&gt;]
   * @throws IOException
   *           HBase IO problem
   * @throws InterruptedException
   *           user requested exit
   * @throws ParseException
   *           problem parsing user input
   */
  @SuppressWarnings("static-access")
  public static void main(String[] args) throws IOException,
      InterruptedException, ParseException {
    Configuration conf = HBaseConfiguration.create();

    // parse user input
    Options opt = new Options();
    opt.addOption(OptionBuilder.withArgName("property=value").hasArg()
        .withDescription("Override HBase Configuration Settings").create("D"));
    opt.addOption(OptionBuilder.withArgName("region count").hasArg()
        .withDescription(
            "Create a new table with a pre-split number of regions")
        .create("c"));
    opt.addOption(OptionBuilder.withArgName("family:family:...").hasArg()
        .withDescription(
            "Column Families to create with new table.  Required with -c")
        .create("f"));
    opt.addOption("h", false, "Print this usage help");
    opt.addOption("r", false, "Perform a rolling split of an existing region");
    opt.addOption(OptionBuilder.withArgName("count").hasArg().withDescription(
        "Max outstanding splits that have unfinished major compactions")
        .create("o"));
    opt.addOption(null, "firstrow", true,
        "First Row in Table for Split Algorithm");
    opt.addOption(null, "lastrow", true,
        "Last Row in Table for Split Algorithm");
    opt.addOption(null, "risky", false,
        "Skip verification steps to complete quickly."
            + "STRONGLY DISCOURAGED for production systems.  ");
    CommandLine cmd = new GnuParser().parse(opt, args);

    if (cmd.hasOption("D")) {
      for (String confOpt : cmd.getOptionValues("D")) {
        String[] kv = confOpt.split("=", 2);
        if (kv.length == 2) {
          conf.set(kv[0], kv[1]);
          LOG.debug("-D configuration override: " + kv[0] + "=" + kv[1]);
        } else {
          throw new ParseException("-D option format invalid: " + confOpt);
        }
      }
    }

    if (cmd.hasOption("risky")) {
      conf.setBoolean("split.verify", false);
    }

    boolean createTable = cmd.hasOption("c") && cmd.hasOption("f");
    boolean rollingSplit = cmd.hasOption("r");
    boolean oneOperOnly = createTable ^ rollingSplit;

    if (2 != cmd.getArgList().size() || !oneOperOnly || cmd.hasOption("h")) {
      new HelpFormatter().printHelp("RegionSplitter <TABLE> <SPLITALGORITHM>\n"+
          "SPLITALGORITHM is a java class name of a class implementing " +
          "SplitAlgorithm, or one of the special strings HexStringSplit " +
          "or UniformSplit, which are built-in split algorithms. " +
          "HexStringSplit treats keys as hexadecimal ASCII, and " +
          "UniformSplit treats keys as arbitrary bytes.", opt);
      return;
    }
    TableName tableName = TableName.valueOf(cmd.getArgs()[0]);
    String splitClass = cmd.getArgs()[1];
    SplitAlgorithm splitAlgo = newSplitAlgoInstance(conf, splitClass);

    if (cmd.hasOption("firstrow")) {
      splitAlgo.setFirstRow(cmd.getOptionValue("firstrow"));
    }
    if (cmd.hasOption("lastrow")) {
      splitAlgo.setLastRow(cmd.getOptionValue("lastrow"));
    }

    if (createTable) {
      conf.set("split.count", cmd.getOptionValue("c"));
      createPresplitTable(tableName, splitAlgo, cmd.getOptionValue("f").split(":"), conf);
    }

    if (rollingSplit) {
      if (cmd.hasOption("o")) {
        conf.set("split.outstanding", cmd.getOptionValue("o"));
      }
      rollingSplit(tableName, splitAlgo, conf);
    }
  }

  static void createPresplitTable(TableName tableName, SplitAlgorithm splitAlgo,
          String[] columnFamilies, Configuration conf)
  throws IOException, InterruptedException {
    final int splitCount = conf.getInt("split.count", 0);
    Preconditions.checkArgument(splitCount > 1, "Split count must be > 1");

    Preconditions.checkArgument(columnFamilies.length > 0,
        "Must specify at least one column family. ");
    LOG.debug("Creating table " + tableName + " with " + columnFamilies.length
        + " column families.  Presplitting to " + splitCount + " regions");

    HTableDescriptor desc = new HTableDescriptor(tableName);
    for (String cf : columnFamilies) {
      desc.addFamily(new HColumnDescriptor(Bytes.toBytes(cf)));
    }
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      Admin admin = connection.getAdmin();
      try {
        Preconditions.checkArgument(!admin.tableExists(tableName),
          "Table already exists: " + tableName);
        admin.createTable(desc, splitAlgo.split(splitCount));
      } finally {
        admin.close();
      }
      LOG.debug("Table created!  Waiting for regions to show online in META...");
      if (!conf.getBoolean("split.verify", true)) {
        // NOTE: createTable is synchronous on the table, but not on the regions
        int onlineRegions = 0;
        while (onlineRegions < splitCount) {
          onlineRegions = MetaTableAccessor.getRegionCount(connection, tableName);
          LOG.debug(onlineRegions + " of " + splitCount + " regions online...");
          if (onlineRegions < splitCount) {
            Thread.sleep(10 * 1000); // sleep
          }
        }
      }
      LOG.debug("Finished creating table with " + splitCount + " regions");
    }
  }

  /**
   * Alternative getCurrentNrHRS which is no longer available.
   * @param connection
   * @return Rough count of regionservers out on cluster.
   * @throws IOException
   */
  private static int getRegionServerCount(final Connection connection) throws IOException {
    try (Admin admin = connection.getAdmin()) {
      ClusterStatus status = admin.getClusterStatus();
      Collection<ServerName> servers = status.getServers();
      return servers == null || servers.isEmpty()? 0: servers.size();
    }
  }

  private static byte [] readFile(final FileSystem fs, final Path path) throws IOException {
    FSDataInputStream tmpIn = fs.open(path);
    try {
      byte [] rawData = new byte[tmpIn.available()];
      tmpIn.readFully(rawData);
      return rawData;
    } finally {
      tmpIn.close();
    }
  }

  static void rollingSplit(TableName tableName, SplitAlgorithm splitAlgo, Configuration conf)
  throws IOException, InterruptedException {
    final int minOS = conf.getInt("split.outstanding", 2);
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      // Max outstanding splits. default == 50% of servers
      final int MAX_OUTSTANDING = Math.max(getRegionServerCount(connection) / 2, minOS);

      Path hbDir = FSUtils.getRootDir(conf);
      Path tableDir = FSUtils.getTableDir(hbDir, tableName);
      Path splitFile = new Path(tableDir, "_balancedSplit");
      FileSystem fs = FileSystem.get(conf);

      // Get a list of daughter regions to create
      LinkedList<Pair<byte[], byte[]>> tmpRegionSet = null;
      try (Table table = connection.getTable(tableName)) {
        tmpRegionSet = getSplits(connection, tableName, splitAlgo);
      }
      LinkedList<Pair<byte[], byte[]>> outstanding = Lists.newLinkedList();
      int splitCount = 0;
      final int origCount = tmpRegionSet.size();

      // all splits must compact & we have 1 compact thread, so 2 split
      // requests to the same RS can stall the outstanding split queue.
      // To fix, group the regions into an RS pool and round-robin through it
      LOG.debug("Bucketing regions by regionserver...");
      TreeMap<String, LinkedList<Pair<byte[], byte[]>>> daughterRegions =
          Maps.newTreeMap();
      // Get a regionLocator.  Need it in below.
      try (RegionLocator regionLocator = connection.getRegionLocator(tableName)) {
        for (Pair<byte[], byte[]> dr : tmpRegionSet) {
          String rsLocation = regionLocator.getRegionLocation(dr.getSecond()).getHostnamePort();
          if (!daughterRegions.containsKey(rsLocation)) {
            LinkedList<Pair<byte[], byte[]>> entry = Lists.newLinkedList();
            daughterRegions.put(rsLocation, entry);
          }
          daughterRegions.get(rsLocation).add(dr);
        }
        LOG.debug("Done with bucketing.  Split time!");
        long startTime = System.currentTimeMillis();

        // Open the split file and modify it as splits finish
        byte[] rawData = readFile(fs, splitFile);

        FSDataOutputStream splitOut = fs.create(splitFile);
        try {
          splitOut.write(rawData);

          try {
            // *** split code ***
            while (!daughterRegions.isEmpty()) {
              LOG.debug(daughterRegions.size() + " RS have regions to splt.");

              // Get ServerName to region count mapping
              final TreeMap<ServerName, Integer> rsSizes = Maps.newTreeMap();
              List<HRegionLocation> hrls = regionLocator.getAllRegionLocations();
              for (HRegionLocation hrl: hrls) {
                ServerName sn = hrl.getServerName();
                if (rsSizes.containsKey(sn)) {
                  rsSizes.put(sn, rsSizes.get(sn) + 1);
                } else {
                  rsSizes.put(sn, 1);
                }
              }

              // Sort the ServerNames by the number of regions they have
              List<String> serversLeft = Lists.newArrayList(daughterRegions .keySet());
              Collections.sort(serversLeft, new Comparator<String>() {
                public int compare(String o1, String o2) {
                  return rsSizes.get(o1).compareTo(rsSizes.get(o2));
                }
              });

              // Round-robin through the ServerName list. Choose the lightest-loaded servers
              // first to keep the master from load-balancing regions as we split.
              for (String rsLoc : serversLeft) {
                Pair<byte[], byte[]> dr = null;

                // Find a region in the ServerName list that hasn't been moved
                LOG.debug("Finding a region on " + rsLoc);
                LinkedList<Pair<byte[], byte[]>> regionList = daughterRegions.get(rsLoc);
                while (!regionList.isEmpty()) {
                  dr = regionList.pop();

                  // get current region info
                  byte[] split = dr.getSecond();
                  HRegionLocation regionLoc = regionLocator.getRegionLocation(split);

                  // if this region moved locations
                  String newRs = regionLoc.getHostnamePort();
                  if (newRs.compareTo(rsLoc) != 0) {
                    LOG.debug("Region with " + splitAlgo.rowToStr(split)
                        + " moved to " + newRs + ". Relocating...");
                    // relocate it, don't use it right now
                    if (!daughterRegions.containsKey(newRs)) {
                      LinkedList<Pair<byte[], byte[]>> entry = Lists.newLinkedList();
                      daughterRegions.put(newRs, entry);
                    }
                    daughterRegions.get(newRs).add(dr);
                    dr = null;
                    continue;
                  }

                  // make sure this region wasn't already split
                  byte[] sk = regionLoc.getRegionInfo().getStartKey();
                  if (sk.length != 0) {
                    if (Bytes.equals(split, sk)) {
                      LOG.debug("Region already split on "
                          + splitAlgo.rowToStr(split) + ".  Skipping this region...");
                      ++splitCount;
                      dr = null;
                      continue;
                    }
                    byte[] start = dr.getFirst();
                    Preconditions.checkArgument(Bytes.equals(start, sk), splitAlgo
                        .rowToStr(start) + " != " + splitAlgo.rowToStr(sk));
                  }

                  // passed all checks! found a good region
                  break;
                }
                if (regionList.isEmpty()) {
                  daughterRegions.remove(rsLoc);
                }
                if (dr == null)
                  continue;

                // we have a good region, time to split!
                byte[] split = dr.getSecond();
                LOG.debug("Splitting at " + splitAlgo.rowToStr(split));
                try (Admin admin = connection.getAdmin()) {
                  admin.split(tableName, split);
                }

                LinkedList<Pair<byte[], byte[]>> finished = Lists.newLinkedList();
                LinkedList<Pair<byte[], byte[]>> local_finished = Lists.newLinkedList();
                if (conf.getBoolean("split.verify", true)) {
                  // we need to verify and rate-limit our splits
                  outstanding.addLast(dr);
                  // with too many outstanding splits, wait for some to finish
                  while (outstanding.size() >= MAX_OUTSTANDING) {
                    LOG.debug("Wait for outstanding splits " + outstanding.size());
                    local_finished = splitScan(outstanding, connection, tableName, splitAlgo);
                    if (local_finished.isEmpty()) {
                      Thread.sleep(30 * 1000);
                    } else {
                      finished.addAll(local_finished);
                      outstanding.removeAll(local_finished);
                      LOG.debug(local_finished.size() + " outstanding splits finished");
                    }
                  }
                } else {
                  finished.add(dr);
                }

                // mark each finished region as successfully split.
                for (Pair<byte[], byte[]> region : finished) {
                  splitOut.writeChars("- " + splitAlgo.rowToStr(region.getFirst())
                      + " " + splitAlgo.rowToStr(region.getSecond()) + "\n");
                  splitCount++;
                  if (splitCount % 10 == 0) {
                    long tDiff = (System.currentTimeMillis() - startTime)
                        / splitCount;
                    LOG.debug("STATUS UPDATE: " + splitCount + " / " + origCount
                        + ". Avg Time / Split = "
                        + org.apache.hadoop.util.StringUtils.formatTime(tDiff));
                  }
                }
              }
            }
            if (conf.getBoolean("split.verify", true)) {
              while (!outstanding.isEmpty()) {
                LOG.debug("Finally Wait for outstanding splits " + outstanding.size());
                LinkedList<Pair<byte[], byte[]>> finished = splitScan(outstanding,
                    connection, tableName, splitAlgo);
                if (finished.isEmpty()) {
                  Thread.sleep(30 * 1000);
                } else {
                  outstanding.removeAll(finished);
                  for (Pair<byte[], byte[]> region : finished) {
                    splitOut.writeChars("- " + splitAlgo.rowToStr(region.getFirst())
                        + " " + splitAlgo.rowToStr(region.getSecond()) + "\n");
                    splitCount++;
                  }
                  LOG.debug("Finally " + finished.size() + " outstanding splits finished");
                }
              }
            }
            LOG.debug("All regions have been successfully split!");
          } finally {
            long tDiff = System.currentTimeMillis() - startTime;
            LOG.debug("TOTAL TIME = "
                + org.apache.hadoop.util.StringUtils.formatTime(tDiff));
            LOG.debug("Splits = " + splitCount);
            if (0 < splitCount) {
              LOG.debug("Avg Time / Split = "
                  + org.apache.hadoop.util.StringUtils.formatTime(tDiff / splitCount));
            }
          }
        } finally {
          splitOut.close();
          fs.delete(splitFile, false);
        }
      }
    }
  }

  /**
   * @throws IOException if the specified SplitAlgorithm class couldn't be
   * instantiated
   */
  public static SplitAlgorithm newSplitAlgoInstance(Configuration conf,
          String splitClassName) throws IOException {
    Class<?> splitClass;

    // For split algorithms builtin to RegionSplitter, the user can specify
    // their simple class name instead of a fully qualified class name.
    if(splitClassName.equals(HexStringSplit.class.getSimpleName())) {
      splitClass = HexStringSplit.class;
    } else if (splitClassName.equals(UniformSplit.class.getSimpleName())) {
      splitClass = UniformSplit.class;
    } else {
      try {
        splitClass = conf.getClassByName(splitClassName);
      } catch (ClassNotFoundException e) {
        throw new IOException("Couldn't load split class " + splitClassName, e);
      }
      if(splitClass == null) {
        throw new IOException("Failed loading split class " + splitClassName);
      }
      if(!SplitAlgorithm.class.isAssignableFrom(splitClass)) {
        throw new IOException(
                "Specified split class doesn't implement SplitAlgorithm");
      }
    }
    try {
      return splitClass.asSubclass(SplitAlgorithm.class).newInstance();
    } catch (Exception e) {
      throw new IOException("Problem loading split algorithm: ", e);
    }
  }

  static LinkedList<Pair<byte[], byte[]>> splitScan(
      LinkedList<Pair<byte[], byte[]>> regionList,
      final Connection connection,
      final TableName tableName,
      SplitAlgorithm splitAlgo)
      throws IOException, InterruptedException {
    LinkedList<Pair<byte[], byte[]>> finished = Lists.newLinkedList();
    LinkedList<Pair<byte[], byte[]>> logicalSplitting = Lists.newLinkedList();
    LinkedList<Pair<byte[], byte[]>> physicalSplitting = Lists.newLinkedList();

    // Get table info
    Pair<Path, Path> tableDirAndSplitFile =
      getTableDirAndSplitFile(connection.getConfiguration(), tableName);
    Path tableDir = tableDirAndSplitFile.getFirst();
    FileSystem fs = tableDir.getFileSystem(connection.getConfiguration());
    // Clear the cache to forcibly refresh region information
    ((ClusterConnection)connection).clearRegionCache();
    HTableDescriptor htd = null;
    try (Table table = connection.getTable(tableName)) {
      htd = table.getTableDescriptor();
    }
    try (RegionLocator regionLocator = connection.getRegionLocator(tableName)) {

      // for every region that hasn't been verified as a finished split
      for (Pair<byte[], byte[]> region : regionList) {
        byte[] start = region.getFirst();
        byte[] split = region.getSecond();

        // see if the new split daughter region has come online
        try {
          HRegionInfo dri = regionLocator.getRegionLocation(split).getRegionInfo();
          if (dri.isOffline() || !Bytes.equals(dri.getStartKey(), split)) {
            logicalSplitting.add(region);
            continue;
          }
        } catch (NoServerForRegionException nsfre) {
          // NSFRE will occur if the old hbase:meta entry has no server assigned
          LOG.info(nsfre);
          logicalSplitting.add(region);
          continue;
        }

        try {
          // when a daughter region is opened, a compaction is triggered
          // wait until compaction completes for both daughter regions
          LinkedList<HRegionInfo> check = Lists.newLinkedList();
          check.add(regionLocator.getRegionLocation(start).getRegionInfo());
          check.add(regionLocator.getRegionLocation(split).getRegionInfo());
          for (HRegionInfo hri : check.toArray(new HRegionInfo[check.size()])) {
            byte[] sk = hri.getStartKey();
            if (sk.length == 0)
              sk = splitAlgo.firstRow();

            RegionStorage regionFs = RegionStorage.open(connection.getConfiguration(), hri, true);

            // Check every Column Family for that region -- check does not have references.
            boolean refFound = false;
            for (HColumnDescriptor c : htd.getFamilies()) {
              if ((refFound = regionFs.hasReferences(c.getNameAsString()))) {
                break;
              }
            }

            // compaction is completed when all reference files are gone
            if (!refFound) {
              check.remove(hri);
            }
          }
          if (check.isEmpty()) {
            finished.add(region);
          } else {
            physicalSplitting.add(region);
          }
        } catch (NoServerForRegionException nsfre) {
          LOG.debug("No Server Exception thrown for: " + splitAlgo.rowToStr(start));
          physicalSplitting.add(region);
          ((ClusterConnection)connection).clearRegionCache();
        }
      }

      LOG.debug("Split Scan: " + finished.size() + " finished / "
          + logicalSplitting.size() + " split wait / "
          + physicalSplitting.size() + " reference wait");

      return finished;
    }
  }

  /**
   * @param conf
   * @param tableName
   * @return A Pair where first item is table dir and second is the split file.
   * @throws IOException
   */
  private static Pair<Path, Path> getTableDirAndSplitFile(final Configuration conf,
      final TableName tableName)
  throws IOException {
    Path hbDir = FSUtils.getRootDir(conf);
    Path tableDir = FSUtils.getTableDir(hbDir, tableName);
    Path splitFile = new Path(tableDir, "_balancedSplit");
    return new Pair<Path, Path>(tableDir, splitFile);
  }

  static LinkedList<Pair<byte[], byte[]>> getSplits(final Connection connection,
      TableName tableName, SplitAlgorithm splitAlgo)
  throws IOException {
    Pair<Path, Path> tableDirAndSplitFile =
      getTableDirAndSplitFile(connection.getConfiguration(), tableName);
    Path tableDir = tableDirAndSplitFile.getFirst();
    Path splitFile = tableDirAndSplitFile.getSecond();

    FileSystem fs = tableDir.getFileSystem(connection.getConfiguration());

    // Using strings because (new byte[]{0}).equals(new byte[]{0}) == false
    Set<Pair<String, String>> daughterRegions = Sets.newHashSet();

    // Does a split file exist?
    if (!fs.exists(splitFile)) {
      // NO = fresh start. calculate splits to make
      LOG.debug("No " + splitFile.getName() + " file. Calculating splits ");

      // Query meta for all regions in the table
      Set<Pair<byte[], byte[]>> rows = Sets.newHashSet();
      Pair<byte[][], byte[][]> tmp = null;
      try (RegionLocator regionLocator = connection.getRegionLocator(tableName)) {
        tmp = regionLocator.getStartEndKeys();
      }
      Preconditions.checkArgument(tmp.getFirst().length == tmp.getSecond().length,
          "Start and End rows should be equivalent");
      for (int i = 0; i < tmp.getFirst().length; ++i) {
        byte[] start = tmp.getFirst()[i], end = tmp.getSecond()[i];
        if (start.length == 0)
          start = splitAlgo.firstRow();
        if (end.length == 0)
          end = splitAlgo.lastRow();
        rows.add(Pair.newPair(start, end));
      }
      LOG.debug("Table " + tableName + " has " + rows.size() + " regions that will be split.");

      // prepare the split file
      Path tmpFile = new Path(tableDir, "_balancedSplit_prepare");
      FSDataOutputStream tmpOut = fs.create(tmpFile);

      // calculate all the splits == [daughterRegions] = [(start, splitPoint)]
      for (Pair<byte[], byte[]> r : rows) {
        byte[] splitPoint = splitAlgo.split(r.getFirst(), r.getSecond());
        String startStr = splitAlgo.rowToStr(r.getFirst());
        String splitStr = splitAlgo.rowToStr(splitPoint);
        daughterRegions.add(Pair.newPair(startStr, splitStr));
        LOG.debug("Will Split [" + startStr + " , "
            + splitAlgo.rowToStr(r.getSecond()) + ") at " + splitStr);
        tmpOut.writeChars("+ " + startStr + splitAlgo.separator() + splitStr
            + "\n");
      }
      tmpOut.close();
      fs.rename(tmpFile, splitFile);
    } else {
      LOG.debug("_balancedSplit file found. Replay log to restore state...");
      FSUtils.getInstance(fs, connection.getConfiguration())
        .recoverFileLease(fs, splitFile, connection.getConfiguration(), null);

      // parse split file and process remaining splits
      FSDataInputStream tmpIn = fs.open(splitFile);
      StringBuilder sb = new StringBuilder(tmpIn.available());
      while (tmpIn.available() > 0) {
        sb.append(tmpIn.readChar());
      }
      tmpIn.close();
      for (String line : sb.toString().split("\n")) {
        String[] cmd = line.split(splitAlgo.separator());
        Preconditions.checkArgument(3 == cmd.length);
        byte[] start = splitAlgo.strToRow(cmd[1]);
        String startStr = splitAlgo.rowToStr(start);
        byte[] splitPoint = splitAlgo.strToRow(cmd[2]);
        String splitStr = splitAlgo.rowToStr(splitPoint);
        Pair<String, String> r = Pair.newPair(startStr, splitStr);
        if (cmd[0].equals("+")) {
          LOG.debug("Adding: " + r);
          daughterRegions.add(r);
        } else {
          LOG.debug("Removing: " + r);
          Preconditions.checkArgument(cmd[0].equals("-"),
              "Unknown option: " + cmd[0]);
          Preconditions.checkState(daughterRegions.contains(r),
              "Missing row: " + r);
          daughterRegions.remove(r);
        }
      }
      LOG.debug("Done reading. " + daughterRegions.size() + " regions left.");
    }
    LinkedList<Pair<byte[], byte[]>> ret = Lists.newLinkedList();
    for (Pair<String, String> r : daughterRegions) {
      ret.add(Pair.newPair(splitAlgo.strToRow(r.getFirst()), splitAlgo
          .strToRow(r.getSecond())));
    }
    return ret;
  }

  /**
   * HexStringSplit is a well-known {@link SplitAlgorithm} for choosing region
   * boundaries. The format of a HexStringSplit region boundary is the ASCII
   * representation of an MD5 checksum, or any other uniformly distributed
   * hexadecimal value. Row are hex-encoded long values in the range
   * <b>"00000000" =&gt; "FFFFFFFF"</b> and are left-padded with zeros to keep the
   * same order lexicographically as if they were binary.
   *
   * Since this split algorithm uses hex strings as keys, it is easy to read &amp;
   * write in the shell but takes up more space and may be non-intuitive.
   */
  public static class HexStringSplit implements SplitAlgorithm {
    final static String DEFAULT_MIN_HEX = "00000000";
    final static String DEFAULT_MAX_HEX = "FFFFFFFF";

    String firstRow = DEFAULT_MIN_HEX;
    BigInteger firstRowInt = BigInteger.ZERO;
    String lastRow = DEFAULT_MAX_HEX;
    BigInteger lastRowInt = new BigInteger(lastRow, 16);
    int rowComparisonLength = lastRow.length();

    public byte[] split(byte[] start, byte[] end) {
      BigInteger s = convertToBigInteger(start);
      BigInteger e = convertToBigInteger(end);
      Preconditions.checkArgument(!e.equals(BigInteger.ZERO));
      return convertToByte(split2(s, e));
    }

    public byte[][] split(int n) {
      Preconditions.checkArgument(lastRowInt.compareTo(firstRowInt) > 0,
          "last row (%s) is configured less than first row (%s)", lastRow,
          firstRow);
      // +1 to range because the last row is inclusive
      BigInteger range = lastRowInt.subtract(firstRowInt).add(BigInteger.ONE);
      Preconditions.checkState(range.compareTo(BigInteger.valueOf(n)) >= 0,
          "split granularity (%s) is greater than the range (%s)", n, range);

      BigInteger[] splits = new BigInteger[n - 1];
      BigInteger sizeOfEachSplit = range.divide(BigInteger.valueOf(n));
      for (int i = 1; i < n; i++) {
        // NOTE: this means the last region gets all the slop.
        // This is not a big deal if we're assuming n << MAXHEX
        splits[i - 1] = firstRowInt.add(sizeOfEachSplit.multiply(BigInteger
            .valueOf(i)));
      }
      return convertToBytes(splits);
    }

    public byte[] firstRow() {
      return convertToByte(firstRowInt);
    }

    public byte[] lastRow() {
      return convertToByte(lastRowInt);
    }

    public void setFirstRow(String userInput) {
      firstRow = userInput;
      firstRowInt = new BigInteger(firstRow, 16);
    }

    public void setLastRow(String userInput) {
      lastRow = userInput;
      lastRowInt = new BigInteger(lastRow, 16);
      // Precondition: lastRow > firstRow, so last's length is the greater
      rowComparisonLength = lastRow.length();
    }

    public byte[] strToRow(String in) {
      return convertToByte(new BigInteger(in, 16));
    }

    public String rowToStr(byte[] row) {
      return Bytes.toStringBinary(row);
    }

    public String separator() {
      return " ";
    }

    @Override
    public void setFirstRow(byte[] userInput) {
      firstRow = Bytes.toString(userInput);
    }

    @Override
    public void setLastRow(byte[] userInput) {
      lastRow = Bytes.toString(userInput);
    }

    /**
     * Divide 2 numbers in half (for split algorithm)
     *
     * @param a number #1
     * @param b number #2
     * @return the midpoint of the 2 numbers
     */
    public BigInteger split2(BigInteger a, BigInteger b) {
      return a.add(b).divide(BigInteger.valueOf(2)).abs();
    }

    /**
     * Returns an array of bytes corresponding to an array of BigIntegers
     *
     * @param bigIntegers numbers to convert
     * @return bytes corresponding to the bigIntegers
     */
    public byte[][] convertToBytes(BigInteger[] bigIntegers) {
      byte[][] returnBytes = new byte[bigIntegers.length][];
      for (int i = 0; i < bigIntegers.length; i++) {
        returnBytes[i] = convertToByte(bigIntegers[i]);
      }
      return returnBytes;
    }

    /**
     * Returns the bytes corresponding to the BigInteger
     *
     * @param bigInteger number to convert
     * @param pad padding length
     * @return byte corresponding to input BigInteger
     */
    public static byte[] convertToByte(BigInteger bigInteger, int pad) {
      String bigIntegerString = bigInteger.toString(16);
      bigIntegerString = StringUtils.leftPad(bigIntegerString, pad, '0');
      return Bytes.toBytes(bigIntegerString);
    }

    /**
     * Returns the bytes corresponding to the BigInteger
     *
     * @param bigInteger number to convert
     * @return corresponding bytes
     */
    public byte[] convertToByte(BigInteger bigInteger) {
      return convertToByte(bigInteger, rowComparisonLength);
    }

    /**
     * Returns the BigInteger represented by the byte array
     *
     * @param row byte array representing row
     * @return the corresponding BigInteger
     */
    public BigInteger convertToBigInteger(byte[] row) {
      return (row.length > 0) ? new BigInteger(Bytes.toString(row), 16)
          : BigInteger.ZERO;
    }

    @Override
    public String toString() {
      return this.getClass().getSimpleName() + " [" + rowToStr(firstRow())
          + "," + rowToStr(lastRow()) + "]";
    }
  }

  /**
   * A SplitAlgorithm that divides the space of possible keys evenly. Useful
   * when the keys are approximately uniform random bytes (e.g. hashes). Rows
   * are raw byte values in the range <b>00 =&gt; FF</b> and are right-padded with
   * zeros to keep the same memcmp() order. This is the natural algorithm to use
   * for a byte[] environment and saves space, but is not necessarily the
   * easiest for readability.
   */
  public static class UniformSplit implements SplitAlgorithm {
    static final byte xFF = (byte) 0xFF;
    byte[] firstRowBytes = ArrayUtils.EMPTY_BYTE_ARRAY;
    byte[] lastRowBytes =
            new byte[] {xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF};
    public byte[] split(byte[] start, byte[] end) {
      return Bytes.split(start, end, 1)[1];
    }

    @Override
    public byte[][] split(int numRegions) {
      Preconditions.checkArgument(
          Bytes.compareTo(lastRowBytes, firstRowBytes) > 0,
          "last row (%s) is configured less than first row (%s)",
          Bytes.toStringBinary(lastRowBytes),
          Bytes.toStringBinary(firstRowBytes));

      byte[][] splits = Bytes.split(firstRowBytes, lastRowBytes, true,
          numRegions - 1);
      Preconditions.checkState(splits != null,
          "Could not split region with given user input: " + this);

      // remove endpoints, which are included in the splits list

      return splits == null? null: Arrays.copyOfRange(splits, 1, splits.length - 1);
    }

    @Override
    public byte[] firstRow() {
      return firstRowBytes;
    }

    @Override
    public byte[] lastRow() {
      return lastRowBytes;
    }

    @Override
    public void setFirstRow(String userInput) {
      firstRowBytes = Bytes.toBytesBinary(userInput);
    }

    @Override
    public void setLastRow(String userInput) {
      lastRowBytes = Bytes.toBytesBinary(userInput);
    }


    @Override
    public void setFirstRow(byte[] userInput) {
      firstRowBytes = userInput;
    }

    @Override
    public void setLastRow(byte[] userInput) {
      lastRowBytes = userInput;
    }

    @Override
    public byte[] strToRow(String input) {
      return Bytes.toBytesBinary(input);
    }

    @Override
    public String rowToStr(byte[] row) {
      return Bytes.toStringBinary(row);
    }

    @Override
    public String separator() {
      return ",";
    }

    @Override
    public String toString() {
      return this.getClass().getSimpleName() + " [" + rowToStr(firstRow())
          + "," + rowToStr(lastRow()) + "]";
    }
  }
}
