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

package org.apache.hadoop.hbase.util.compaction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLineParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.DefaultParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Option;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;
import org.apache.hbase.thirdparty.org.apache.commons.cli.ParseException;

/**
 * This tool compacts a table's regions that are beyond it's TTL. It helps to save disk space and
 * regions become empty as a result of compaction.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class MajorCompactorTTL extends MajorCompactor {

  private static final Logger LOG = LoggerFactory.getLogger(MajorCompactorTTL .class);

  private TableDescriptor htd;

  @InterfaceAudience.Private
  public MajorCompactorTTL(Configuration conf, TableDescriptor htd, int concurrency,
      long sleepForMs) throws IOException {
    this.connection = ConnectionFactory.createConnection(conf);
    this.htd = htd;
    this.tableName = htd.getTableName();
    this.storesToCompact = Sets.newHashSet(); // Empty set so all stores will be compacted
    this.sleepForMs = sleepForMs;
    this.executor = Executors.newFixedThreadPool(concurrency);
    this.clusterCompactionQueues = new ClusterCompactionQueues(concurrency);
  }

  protected MajorCompactorTTL() {
    super();
  }

  @Override
  protected Optional<MajorCompactionRequest> getMajorCompactionRequest(RegionInfo hri)
      throws IOException {
    return MajorCompactionTTLRequest.newRequest(connection, hri, htd);
  }

  @Override
  protected Set<String> getStoresRequiringCompaction(MajorCompactionRequest request)
      throws IOException {
    return ((MajorCompactionTTLRequest)request).getStoresRequiringCompaction(htd).keySet();
  }

  public int compactRegionsTTLOnTable(Configuration conf, String table, int concurrency,
      long sleep, int numServers, int numRegions, boolean dryRun, boolean skipWait)
      throws Exception {

    Connection conn = ConnectionFactory.createConnection(conf);
    TableName tableName = TableName.valueOf(table);

    TableDescriptor htd = conn.getAdmin().getDescriptor(tableName);
    if (!doesAnyColFamilyHaveTTL(htd)) {
      LOG.info("No TTL present for CF of table: " + tableName + ", skipping compaction");
      return 0;
    }

    LOG.info("Major compacting table " + tableName + " based on TTL");
    MajorCompactor compactor = new MajorCompactorTTL(conf, htd, concurrency, sleep);
    compactor.setNumServers(numServers);
    compactor.setNumRegions(numRegions);
    compactor.setSkipWait(skipWait);

    compactor.initializeWorkQueues();
    if (!dryRun) {
      compactor.compactAllRegions();
    }
    compactor.shutdown();
    return ERRORS.size();
  }

  private boolean doesAnyColFamilyHaveTTL(TableDescriptor htd) {
    for (ColumnFamilyDescriptor descriptor : htd.getColumnFamilies()) {
      if (descriptor.getTimeToLive() != HConstants.FOREVER) {
        return true;
      }
    }
    return false;
  }

  private Options getOptions() {
    Options options = getCommonOptions();

    options.addOption(
        Option.builder("table")
            .required()
            .desc("table name")
            .hasArg()
            .build()
    );

    return options;
  }

  @Override
  public int run(String[] args) throws Exception {
    Options options = getOptions();

    final CommandLineParser cmdLineParser = new DefaultParser();
    CommandLine commandLine;
    try {
      commandLine = cmdLineParser.parse(options, args);
    } catch (ParseException parseException) {
      System.out.println(
          "ERROR: Unable to parse command-line arguments " + Arrays.toString(args) + " due to: "
              + parseException);
      printUsage(options);
      return -1;
    }
    if (commandLine == null) {
      System.out.println("ERROR: Failed parse, empty commandLine; " + Arrays.toString(args));
      printUsage(options);
      return -1;
    }

    String table = commandLine.getOptionValue("table");
    int numServers = Integer.parseInt(commandLine.getOptionValue("numservers", "-1"));
    int numRegions = Integer.parseInt(commandLine.getOptionValue("numregions", "-1"));
    int concurrency = Integer.parseInt(commandLine.getOptionValue("servers", "1"));
    long sleep = Long.parseLong(commandLine.getOptionValue("sleep", Long.toString(30000)));
    boolean dryRun = commandLine.hasOption("dryRun");
    boolean skipWait = commandLine.hasOption("skipWait");

    return compactRegionsTTLOnTable(HBaseConfiguration.create(), table, concurrency, sleep,
        numServers, numRegions, dryRun, skipWait);
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(HBaseConfiguration.create(), new MajorCompactorTTL(), args);
  }
}
