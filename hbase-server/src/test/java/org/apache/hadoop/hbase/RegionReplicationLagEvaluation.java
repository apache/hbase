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

import com.google.errorprone.annotations.RestrictedApi;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.metrics.impl.FastLongHistogram;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.DefaultParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;

/**
 * A tool to evaluating the lag between primary replica and secondary replica.
 * <p/>
 * It simply adds a row to the primary replica, and then check how long before we can read it from
 * the secondary replica.
 */
@InterfaceAudience.Private
public class RegionReplicationLagEvaluation extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(RegionReplicationLagEvaluation.class);

  public static final String TABLE_NAME = "TestLagTable";

  public static final String FAMILY_NAME = "info";

  public static final String QUALIFIER_NAME = "qual";

  public static final int VALUE_LENGTH = 256;

  public static final int ROW_LENGTH = 16;

  private static final Options OPTIONS = new Options().addOption("t", "table", true, "Table name")
    .addOption("rlen", "rlength", true, "The length of row key")
    .addOption("vlen", "vlength", true, "The length of value")
    .addRequiredOption("r", "rows", true, "Number of rows to test");

  private FastLongHistogram histogram = new FastLongHistogram();

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*/src/test/.*")
  FastLongHistogram getHistogram() {
    return histogram;
  }

  @Override
  public int run(String[] args) throws Exception {
    TableName tableName;
    int rlen;
    int vlen;
    int rows;
    try {
      CommandLine cli = new DefaultParser().parse(OPTIONS, args);
      tableName = TableName.valueOf(cli.getOptionValue("t", TABLE_NAME));
      rlen = Integer.parseInt(cli.getOptionValue("rlen", String.valueOf(ROW_LENGTH)));
      vlen = Integer.parseInt(cli.getOptionValue("vlen", String.valueOf(VALUE_LENGTH)));
      rows = Integer.parseInt(cli.getOptionValue("r"));
    } catch (Exception e) {
      LOG.warn("Error parsing command line options", e);
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(getClass().getName(), OPTIONS);
      return -1;
    }
    exec(tableName, rlen, vlen, rows);
    return 0;
  }

  private void createTable(Admin admin, TableName tableName) throws IOException {
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_NAME)).setRegionReplication(2)
      .build();
    admin.createTable(td);
  }

  private void checkLag(Table table, int rlen, int vlen, int rows) throws IOException {
    byte[] family = Bytes.toBytes(FAMILY_NAME);
    byte[] qualifier = Bytes.toBytes(QUALIFIER_NAME);
    LOG.info("Test replication lag on table {} with {} rows", table.getName(), rows);
    for (int i = 0; i < rows; i++) {
      byte[] row = new byte[rlen];
      Bytes.random(row);
      byte[] value = new byte[vlen];
      Bytes.random(value);
      table.put(new Put(row).addColumn(family, qualifier, value));
      // get from secondary replica
      Get get = new Get(row).setConsistency(Consistency.TIMELINE).setReplicaId(1);
      long startNs = System.nanoTime();
      for (int retry = 0;; retry++) {
        Result result = table.get(get);
        byte[] gotValue = result.getValue(family, qualifier);
        if (Arrays.equals(value, gotValue)) {
          break;
        }
        long pauseTimeMs = Math.min(ConnectionUtils.getPauseTime(1, retry), 1000);
        Threads.sleepWithoutInterrupt(pauseTimeMs);
      }
      long lagMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
      histogram.add(lagMs, 1);
    }
    LOG.info("Test finished, min lag {} ms, max lag {} ms, mean lag {} ms", histogram.getMin(),
      histogram.getMax(), histogram.getMean());
    long[] q = histogram.getQuantiles(FastLongHistogram.DEFAULT_QUANTILES);
    for (int i = 0; i < q.length; i++) {
      LOG.info("{}% lag: {} ms", FastLongHistogram.DEFAULT_QUANTILES[i] * 100, q[i]);
    }
  }

  private void exec(TableName tableName, int rlen, int vlen, int rows) throws IOException {
    try (Connection conn = ConnectionFactory.createConnection(getConf())) {
      try (Admin admin = conn.getAdmin()) {
        if (!admin.tableExists(tableName)) {
          createTable(admin, tableName);
        }
      }
      try (Table table = conn.getTable(tableName)) {
        checkLag(table, rlen, vlen, rows);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    int res =
      ToolRunner.run(HBaseConfiguration.create(), new RegionReplicationLagEvaluation(), args);
    System.exit(res);
  }
}
