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
package org.apache.hadoop.hbase.client.example;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;


/**
 * Example on how to use HBase's {@link Connection} and {@link Table} in a
 * multi-threaded environment. Each table is a light weight object
 * that is created and thrown away. Connections are heavy weight objects
 * that hold on to zookeeper connections, async processes, and other state.
 *
 * <pre>
 * Usage:
 * bin/hbase org.apache.hadoop.hbase.client.example.MultiThreadedClientExample testTableName 500000
 * </pre>
 *
 * <p>
 * The table should already be created before running the command.
 * This example expects one column family named d.
 * </p>
 * <p>
 * This is meant to show different operations that are likely to be
 * done in a real world application. These operations are:
 * </p>
 *
 * <ul>
 *   <li>
 *     30% of all operations performed are batch writes.
 *     30 puts are created and sent out at a time.
 *     The response for all puts is waited on.
 *   </li>
 *   <li>
 *     20% of all operations are single writes.
 *     A single put is sent out and the response is waited for.
 *   </li>
 *   <li>
 *     50% of all operations are scans.
 *     These scans start at a random place and scan up to 100 rows.
 *   </li>
 * </ul>
 *
 */
public class MultiThreadedClientExample extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(MultiThreadedClientExample.class);
  private static final int DEFAULT_NUM_OPERATIONS = 500000;

  /**
   * The name of the column family.
   *
   * d for default.
   */
  private static final byte[] FAMILY = Bytes.toBytes("d");

  /**
   * For the example we're just using one qualifier.
   */
  private static final byte[] QUAL = Bytes.toBytes("test");

  private final ExecutorService internalPool;

  private final int threads;

  public MultiThreadedClientExample() throws IOException {
    // Base number of threads.
    // This represents the number of threads you application has
    // that can be interacting with an hbase client.
    this.threads = Runtime.getRuntime().availableProcessors() * 4;

    // Daemon threads are great for things that get shut down.
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("internal-pol-%d").build();


    this.internalPool = Executors.newFixedThreadPool(threads, threadFactory);
  }

  @Override
  public int run(String[] args) throws Exception {

    if (args.length < 1 || args.length > 2) {
      System.out.println("Usage: " + this.getClass().getName() + " tableName [num_operations]");
      return -1;
    }

    final TableName tableName = TableName.valueOf(args[0]);
    int numOperations = DEFAULT_NUM_OPERATIONS;

    // the second arg is the number of operations to send.
    if (args.length == 2) {
      numOperations = Integer.parseInt(args[1]);
    }

    // Threads for the client only.
    //
    // We don't want to mix hbase and business logic.
    //
    ExecutorService service = new ForkJoinPool(threads * 2);

    // Create two different connections showing how it's possible to
    // separate different types of requests onto different connections
    final Connection writeConnection = ConnectionFactory.createConnection(getConf(), service);
    final Connection readConnection = ConnectionFactory.createConnection(getConf(), service);

    // At this point the entire cache for the region locations is full.
    // Only do this if the number of regions in a table is easy to fit into memory.
    //
    // If you are interacting with more than 25k regions on a client then it's probably not good
    // to do this at all.
    warmUpConnectionCache(readConnection, tableName);
    warmUpConnectionCache(writeConnection, tableName);

    List<Future<Boolean>> futures = new ArrayList<>(numOperations);
    for (int i = 0; i < numOperations; i++) {
      double r = ThreadLocalRandom.current().nextDouble();
      Future<Boolean> f;

      // For the sake of generating some synthetic load this queues
      // some different callables.
      // These callables are meant to represent real work done by your application.
      if (r < .30) {
        f = internalPool.submit(new WriteExampleCallable(writeConnection, tableName));
      } else if (r < .50) {
        f = internalPool.submit(new SingleWriteExampleCallable(writeConnection, tableName));
      } else {
        f = internalPool.submit(new ReadExampleCallable(writeConnection, tableName));
      }
      futures.add(f);
    }

    // Wait a long time for all the reads/writes to complete
    for (Future<Boolean> f : futures) {
      f.get(10, TimeUnit.MINUTES);
    }

    // Clean up after our selves for cleanliness
    internalPool.shutdownNow();
    service.shutdownNow();
    return 0;
  }

  private void warmUpConnectionCache(Connection connection, TableName tn) throws IOException {
    try (RegionLocator locator = connection.getRegionLocator(tn)) {
      LOG.info(
          "Warmed up region location cache for " + tn
              + " got " + locator.getAllRegionLocations().size());
    }
  }

  /**
   * Class that will show how to send batches of puts at the same time.
   */
  public static class WriteExampleCallable implements Callable<Boolean> {
    private final Connection connection;
    private final TableName tableName;

    public WriteExampleCallable(Connection connection, TableName tableName) {
      this.connection = connection;
      this.tableName = tableName;
    }

    @Override
    public Boolean call() throws Exception {

      // Table implements Closable so we use the try with resource structure here.
      // https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html
      try (Table t = connection.getTable(tableName)) {
        byte[] value = Bytes.toBytes(Double.toString(ThreadLocalRandom.current().nextDouble()));
        int rows = 30;

        // Array to put the batch
        ArrayList<Put> puts = new ArrayList<>(rows);
        for (int i = 0; i < 30; i++) {
          byte[] rk = Bytes.toBytes(ThreadLocalRandom.current().nextLong());
          Put p = new Put(rk);
          p.addImmutable(FAMILY, QUAL, value);
          puts.add(p);
        }

        // now that we've assembled the batch it's time to push it to hbase.
        t.put(puts);
      }
      return true;
    }
  }

  /**
   * Class to show how to send a single put.
   */
  public static class SingleWriteExampleCallable implements Callable<Boolean> {
    private final Connection connection;
    private final TableName tableName;

    public SingleWriteExampleCallable(Connection connection, TableName tableName) {
      this.connection = connection;
      this.tableName = tableName;
    }

    @Override
    public Boolean call() throws Exception {
      try (Table t = connection.getTable(tableName)) {

        byte[] value = Bytes.toBytes(Double.toString(ThreadLocalRandom.current().nextDouble()));
        byte[] rk = Bytes.toBytes(ThreadLocalRandom.current().nextLong());
        Put p = new Put(rk);
        p.addImmutable(FAMILY, QUAL, value);
        t.put(p);
      }
      return true;
    }
  }


  /**
   * Class to show how to scan some rows starting at a random location.
   */
  public static class ReadExampleCallable implements Callable<Boolean> {
    private final Connection connection;
    private final TableName tableName;

    public ReadExampleCallable(Connection connection, TableName tableName) {
      this.connection = connection;
      this.tableName = tableName;
    }

    @Override
    public Boolean call() throws Exception {

      // total length in bytes of all read rows.
      int result = 0;

      // Number of rows the scan will read before being considered done.
      int toRead = 100;
      try (Table t = connection.getTable(tableName)) {
        byte[] rk = Bytes.toBytes(ThreadLocalRandom.current().nextLong());
        Scan s = new Scan(rk);

        // This filter will keep the values from being sent accross the wire.
        // This is good for counting or other scans that are checking for
        // existence and don't rely on the value.
        s.setFilter(new KeyOnlyFilter());

        // Don't go back to the server for every single row.
        // We know these rows are small. So ask for 20 at a time.
        // This would be application specific.
        //
        // The goal is to reduce round trips but asking for too
        // many rows can lead to GC problems on client and server sides.
        s.setCaching(20);

        // Don't use the cache. While this is a silly test program it's still good to be
        // explicit that scans normally don't use the block cache.
        s.setCacheBlocks(false);

        // Open up the scanner and close it automatically when done.
        try (ResultScanner rs = t.getScanner(s)) {

          // Now go through rows.
          for (Result r : rs) {
            // Keep track of things size to simulate doing some real work.
            result += r.getRow().length;
            toRead -= 1;

            // Most online applications won't be
            // reading the entire table so this break
            // simulates small to medium size scans,
            // without needing to know an end row.
            if (toRead <= 0)  {
              break;
            }
          }
        }
      }
      return result > 0;
    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new MultiThreadedClientExample(), args);
  }
}
