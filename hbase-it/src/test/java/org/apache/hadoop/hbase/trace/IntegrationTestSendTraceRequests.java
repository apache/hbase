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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;

@Category(IntegrationTests.class)
public class IntegrationTestSendTraceRequests extends AbstractHBaseTool {

  public static final String TABLE_ARG = "t";
  public static final String CF_ARG = "f";

  public static final String TABLE_NAME_DEFAULT = "SendTracesTable";
  public static final String COLUMN_FAMILY_DEFAULT = "D";
  private TableName tableName = TableName.valueOf(TABLE_NAME_DEFAULT);
  private byte[] familyName = Bytes.toBytes(COLUMN_FAMILY_DEFAULT);
  private IntegrationTestingUtility util;
  private Admin admin;

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
    admin = util.getAdmin();

    deleteTable();
    createTable();
    LinkedBlockingQueue<Long> rks = insertData();

    ExecutorService service = Executors.newFixedThreadPool(20);
    doScans(service, rks);
    doGets(service, rks);

    service.shutdown();
    service.awaitTermination(100, TimeUnit.SECONDS);
    Thread.sleep(90000);
    util.restoreCluster();
    util = null;
  }

  private void doScans(ExecutorService service, final LinkedBlockingQueue<Long> rks) {
    for (int i = 0; i < 100; i++) {
      Runnable runnable = new Runnable() {
        private final LinkedBlockingQueue<Long> rowKeyQueue = rks;

        @Override
        public void run() {
          ResultScanner rs = null;
          Span span = TraceUtil.getGlobalTracer().spanBuilder("Scan").startSpan();
          try (Scope scope = span.makeCurrent()) {
            Table ht = util.getConnection().getTable(tableName);
            Scan s = new Scan();
            s.withStartRow(Bytes.toBytes(rowKeyQueue.take()));
            s.setBatch(7);
            rs = ht.getScanner(s);
            // Something to keep the jvm from removing the loop.
            long accum = 0;

            for (int x = 0; x < 1000; x++) {
              Result r = rs.next();
              accum |= Bytes.toLong(r.getRow());
            }

            span.addEvent("Accum result = " + accum);

            ht.close();
            ht = null;
          } catch (IOException e) {
            e.printStackTrace();
            span.addEvent("exception",
              Attributes.of(AttributeKey.stringKey("exception"), e.getClass().getSimpleName()));
          } catch (Exception e) {
          } finally {
            span.end();
            if (rs != null) {
              rs.close();
            }
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
        private final LinkedBlockingQueue<Long> rowKeyQueue = rowKeys;

        @Override
        public void run() {


          Table ht = null;
          try {
            ht = util.getConnection().getTable(tableName);
          } catch (IOException e) {
            e.printStackTrace();
          }

          long accum = 0;
          for (int x = 0; x < 5; x++) {
            Span span = TraceUtil.getGlobalTracer().spanBuilder("gets").startSpan();
            try (Scope scope = span.makeCurrent()) {
              long rk = rowKeyQueue.take();
              Result r1 = ht.get(new Get(Bytes.toBytes(rk)));
              if (r1 != null) {
                accum |= Bytes.toLong(r1.getRow());
              }
              Result r2 = ht.get(new Get(Bytes.toBytes(rk)));
              if (r2 != null) {
                accum |= Bytes.toLong(r2.getRow());
              }
              span.addEvent("Accum = " + accum);

            } catch (IOException|InterruptedException ie) {
              // IGNORED
            } finally {
              span.end();
            }
          }

        }
      };
      service.submit(runnable);
    }
  }

  private void createTable() throws IOException {
    Span span = TraceUtil.getGlobalTracer().spanBuilder("createTable").startSpan();
    try (Scope scope = span.makeCurrent()) {
      util.createTable(tableName, familyName);
    } finally {
      span.end();
    }
  }

  private void deleteTable() throws IOException {
    Span span = TraceUtil.getGlobalTracer().spanBuilder("deleteTable").startSpan();
    try (Scope scope = span.makeCurrent()) {
      if (admin.tableExists(tableName)) {
        util.deleteTable(tableName);
      }
    } finally {
      span.end();
    }
  }

  private LinkedBlockingQueue<Long> insertData() throws IOException, InterruptedException {
    LinkedBlockingQueue<Long> rowKeys = new LinkedBlockingQueue<>(25000);
    BufferedMutator ht = util.getConnection().getBufferedMutator(this.tableName);
    Random rand = ThreadLocalRandom.current();
    byte[] value = new byte[300];
    for (int x = 0; x < 5000; x++) {
      Span span = TraceUtil.getGlobalTracer().spanBuilder("insertData").startSpan();
      try (Scope scope = span.makeCurrent()) {
        for (int i = 0; i < 5; i++) {
          long rk = rand.nextLong();
          rowKeys.add(rk);
          Put p = new Put(Bytes.toBytes(rk));
          for (int y = 0; y < 10; y++) {
            Bytes.random(value);
            p.addColumn(familyName, Bytes.toBytes(rand.nextLong()), value);
          }
          ht.mutate(p);
        }
        if ((x % 1000) == 0) {
          admin.flush(tableName);
        }
      } finally {
        span.end();
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
}
