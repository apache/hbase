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

package org.apache.hadoop.hbase.trace;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.htrace.Sampler;
import org.htrace.Trace;
import org.htrace.TraceScope;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Category(IntegrationTests.class)
public class IntegrationTestSendTraceRequests extends AbstractHBaseTool {

  public static final String TABLE_ARG = "t";
  public static final String CF_ARG = "f";

  public static final String TABLE_NAME_DEFAULT = "SendTracesTable";
  public static final String COLUMN_FAMILY_DEFAULT = "D";
  private TableName tableName = TableName.valueOf(TABLE_NAME_DEFAULT);
  private byte[] familyName = Bytes.toBytes(COLUMN_FAMILY_DEFAULT);
  private IntegrationTestingUtility util;
  private Random random = new Random();
  private Admin admin;
  private SpanReceiverHost receiverHost;

  public static void main(String[] args) throws Exception {
    Configuration configuration = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(configuration);
    IntegrationTestSendTraceRequests tool = new IntegrationTestSendTraceRequests();
    ToolRunner.run(configuration, tool, args);
  }

  @Override
  protected void addOptions() {
    addOptWithArg(TABLE_ARG, "The table name to target.  Will be created if not there already.");
    addOptWithArg(CF_ARG, "The family to target");
  }

  @Override
  public void processOptions(CommandLine cmd) {
    String tableNameString = cmd.getOptionValue(TABLE_ARG, TABLE_NAME_DEFAULT);
    String familyString = cmd.getOptionValue(CF_ARG, COLUMN_FAMILY_DEFAULT);

    this.tableName = TableName.valueOf(tableNameString);
    this.familyName = Bytes.toBytes(familyString);
  }

  @Override
  public int doWork() throws Exception {
    internalDoWork();
    return 0;
  }

  @Test
  public void internalDoWork() throws Exception {
    util = createUtil();
    admin = util.getHBaseAdmin();
    setupReceiver();

    deleteTable();
    createTable();
    LinkedBlockingQueue<Long> rks = insertData();

    ExecutorService service = Executors.newFixedThreadPool(20);
    doScans(service, rks);
    doGets(service, rks);

    service.shutdown();
    service.awaitTermination(100, TimeUnit.SECONDS);
    Thread.sleep(90000);
    receiverHost.closeReceivers();
    util.restoreCluster();
    util = null;
  }

  private void doScans(ExecutorService service, final LinkedBlockingQueue<Long> rks) {

      for (int i = 0; i < 100; i++) {
        Runnable runnable = new Runnable() {
          private TraceScope innerScope = null;
          private final LinkedBlockingQueue<Long> rowKeyQueue = rks;
          @Override
          public void run() {
            ResultScanner rs = null;
            try {
              innerScope = Trace.startSpan("Scan", Sampler.ALWAYS);
              Table ht = new HTable(util.getConfiguration(), tableName);
              Scan s = new Scan();
              s.setStartRow(Bytes.toBytes(rowKeyQueue.take()));
              s.setBatch(7);
              rs = ht.getScanner(s);
              // Something to keep the jvm from removing the loop.
              long accum = 0;

              for(int x = 0; x < 1000; x++) {
                Result r = rs.next();
                accum |= Bytes.toLong(r.getRow());
              }

              innerScope.getSpan().addTimelineAnnotation("Accum result = " + accum);

              ht.close();
              ht = null;
            } catch (IOException e) {
              e.printStackTrace();

              innerScope.getSpan().addKVAnnotation(
                  Bytes.toBytes("exception"),
                  Bytes.toBytes(e.getClass().getSimpleName()));

            } catch (Exception e) {
            } finally {
              if (innerScope != null) innerScope.close();
              if (rs != null) rs.close();
            }

          }
        };
        service.submit(runnable);
      }

  }

  private void doGets(ExecutorService service, final LinkedBlockingQueue<Long> rowKeys)
      throws IOException {
    for (int i = 0; i < 100; i++) {
      Runnable runnable = new Runnable() {
        private TraceScope innerScope = null;
        private final LinkedBlockingQueue<Long> rowKeyQueue = rowKeys;

        @Override
        public void run() {


          Table ht = null;
          try {
            ht = new HTable(util.getConfiguration(), tableName);
          } catch (IOException e) {
            e.printStackTrace();
          }

          long accum = 0;
          for (int x = 0; x < 5; x++) {
            try {
              innerScope = Trace.startSpan("gets", Sampler.ALWAYS);
              long rk = rowKeyQueue.take();
              Result r1 = ht.get(new Get(Bytes.toBytes(rk)));
              if (r1 != null) {
                accum |= Bytes.toLong(r1.getRow());
              }
              Result r2 = ht.get(new Get(Bytes.toBytes(rk)));
              if (r2 != null) {
                accum |= Bytes.toLong(r2.getRow());
              }
              innerScope.getSpan().addTimelineAnnotation("Accum = " + accum);

            } catch (IOException e) {
              // IGNORED
            } catch (InterruptedException ie) {
              // IGNORED
            } finally {
              if (innerScope != null) innerScope.close();
            }
          }

        }
      };
      service.submit(runnable);
    }
  }

  private void createTable() throws IOException {
    TraceScope createScope = null;
    try {
      createScope = Trace.startSpan("createTable", Sampler.ALWAYS);
      util.createTable(tableName, familyName);
    } finally {
      if (createScope != null) createScope.close();
    }
  }

  private void deleteTable() throws IOException {
    TraceScope deleteScope = null;

    try {
      if (admin.tableExists(tableName)) {
        deleteScope = Trace.startSpan("deleteTable", Sampler.ALWAYS);
        util.deleteTable(tableName);
      }
    } finally {
      if (deleteScope != null) deleteScope.close();
    }
  }

  private LinkedBlockingQueue<Long> insertData() throws IOException, InterruptedException {
    LinkedBlockingQueue<Long> rowKeys = new LinkedBlockingQueue<Long>(25000);
    HTable ht = new HTable(util.getConfiguration(), this.tableName);
    byte[] value = new byte[300];
    for (int x = 0; x < 5000; x++) {
      TraceScope traceScope = Trace.startSpan("insertData", Sampler.ALWAYS);
      try {
        ht.setAutoFlush(false, true);
        for (int i = 0; i < 5; i++) {
          long rk = random.nextLong();
          rowKeys.add(rk);
          Put p = new Put(Bytes.toBytes(rk));
          for (int y = 0; y < 10; y++) {
            random.nextBytes(value);
            p.add(familyName, Bytes.toBytes(random.nextLong()), value);
          }
          ht.put(p);
        }
        if ((x % 1000) == 0) {
          admin.flush(tableName);
        }
      } finally {
        traceScope.close();
      }
    }
    admin.flush(tableName);
    return rowKeys;
  }

  private IntegrationTestingUtility createUtil() throws Exception {
    Configuration conf = getConf();
    if (this.util == null) {
      IntegrationTestingUtility u;
      if (conf == null) {
        u = new IntegrationTestingUtility();
      } else {
        u = new IntegrationTestingUtility(conf);
      }
      util = u;
      util.initializeCluster(1);

    }
    return this.util;
  }

  private void setupReceiver() {
    Configuration conf = new Configuration(util.getConfiguration());
    conf.setBoolean("hbase.zipkin.is-in-client-mode", true);

    this.receiverHost = SpanReceiverHost.getInstance(conf);
  }
}
