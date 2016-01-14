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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import com.yammer.metrics.stats.Snapshot;

/**
 * Simple Increments Performance Test. Run this from main. It is to go against a cluster.
 * Presumption is the table exists already. Defaults are a zk ensemble of localhost:2181,
 * a tableName of 'tableName', a column famly name of 'columnFamilyName', with 80 threads by
 * default and 10000 increments per thread. To change any of these configs, pass -DNAME=VALUE as
 * in -DtableName="newTableName". It prints out configuration it is running with at the start and
 * on the end it prints out percentiles.
 */
public class IncrementPerformanceTest implements Tool {
  private static final Log LOG = LogFactory.getLog(IncrementPerformanceTest.class);
  private static final byte [] QUALIFIER = new byte [] {'q'};
  private Configuration conf;
  private final MetricName metricName = new MetricName(this.getClass(), "increment");
  private static final String TABLENAME = "tableName";
  private static final String COLUMN_FAMILY = "columnFamilyName";
  private static final String THREAD_COUNT = "threadCount";
  private static final int DEFAULT_THREAD_COUNT = 80;
  private static final String INCREMENT_COUNT = "incrementCount";
  private static final int DEFAULT_INCREMENT_COUNT = 10000;

  IncrementPerformanceTest() {}

  public int run(final String [] args) throws Exception {
    Configuration conf = getConf();
    final TableName tableName = TableName.valueOf(conf.get(TABLENAME), TABLENAME);
    final byte [] columnFamilyName = Bytes.toBytes(conf.get(COLUMN_FAMILY, COLUMN_FAMILY));
    int threadCount = conf.getInt(THREAD_COUNT, DEFAULT_THREAD_COUNT);
    final int incrementCount = conf.getInt(INCREMENT_COUNT, DEFAULT_INCREMENT_COUNT);
    LOG.info("Running test with " + HConstants.ZOOKEEPER_QUORUM + "=" +
      getConf().get(HConstants.ZOOKEEPER_QUORUM) + ", tableName=" + tableName +
      ", columnFamilyName=" + columnFamilyName + ", threadCount=" + threadCount +
      ", incrementCount=" + incrementCount);

    ExecutorService service = Executors.newFixedThreadPool(threadCount);
    Set<Future<?>> futures = new HashSet<Future<?>>();
    final AtomicInteger integer = new AtomicInteger(0); // needed a simple "final" counter
    while (integer.incrementAndGet() <= threadCount) {
      futures.add(service.submit(new Runnable() {
        @Override
        public void run() {
          HTable table;
          try {
            // ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(TABLE_NAME));
            table = new HTable(getConf(), tableName.getName());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          Timer timer = Metrics.newTimer(metricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
          for (int i = 0; i < incrementCount; i++) {
            byte[] row = Bytes.toBytes(i);
            TimerContext context = timer.time();
            try {
              table.incrementColumnValue(row, columnFamilyName, QUALIFIER, 1l);
            } catch (IOException e) {
              // swallow..it's a test.
            } finally {
              context.stop();
            }
          }
        }
      }));
    }

    for(Future<?> future : futures) future.get();
    service.shutdown();
    Snapshot s = Metrics.newTimer(this.metricName,
        TimeUnit.MILLISECONDS, TimeUnit.SECONDS).getSnapshot();
    LOG.info(String.format("75th=%s, 95th=%s, 99th=%s", s.get75thPercentile(),
        s.get95thPercentile(), s.get99thPercentile()));
    return 0;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(HBaseConfiguration.create(), new IncrementPerformanceTest(), args));
  }
}