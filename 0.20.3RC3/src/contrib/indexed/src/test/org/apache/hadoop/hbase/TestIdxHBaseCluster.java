/*
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import junit.framework.Assert;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.idx.IdxColumnDescriptor;
import org.apache.hadoop.hbase.client.idx.IdxIndexDescriptor;
import org.apache.hadoop.hbase.client.idx.IdxQualifierType;
import org.apache.hadoop.hbase.client.idx.IdxScan;
import org.apache.hadoop.hbase.client.idx.exp.Comparison;
import org.apache.hadoop.hbase.client.idx.exp.Expression;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.regionserver.IdxRegion;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * tests administrative functions
 */
public class TestIdxHBaseCluster extends TestHBaseCluster {
  private static final String FIXED_PART = "Some string with a rolling value ";

  /**
   * constructor
   */
  public TestIdxHBaseCluster() {
    super();
    conf.setClass(HConstants.REGION_IMPL, IdxRegion.class, IdxRegion.class);
    // Force flushes and compactions
    conf.set("hbase.hregion.memstore.flush.size", "262144");
  }

  /**
   * Tests reading and writing concurrently to a table.
   *
   * @throws IOException exception
   */
  @SuppressWarnings({"unchecked"})
  public void testConcurrentReadWrite() throws IOException {
    int maxRows = 20000;
    Random random = new Random(4111994L);
    byte[][] rows = new byte[maxRows][];
    for (int i = 0; i < rows.length; i++) {
      rows[i] = Bytes.toBytes(random.nextInt());
    }
    final AtomicInteger sequence = new AtomicInteger(0);

    HTableDescriptor desc = new HTableDescriptor("testConcurrentReadWrite");
    byte[] family = Bytes.toBytes("concurrentRW");
    byte[] qualifier = Bytes.toBytes("strings");
    IdxColumnDescriptor descriptor = new IdxColumnDescriptor(family);
    descriptor.addIndexDescriptor(new IdxIndexDescriptor(qualifier,
      IdxQualifierType.CHAR_ARRAY));
    desc.addFamily(descriptor);
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    HTable table = new HTable(conf, desc.getName());

    ExecutorService service = Executors.newCachedThreadPool();
    for (int i = 0; i < 5; i++) {
      service.submit(new Writer(table, family, qualifier, sequence, rows));
    }

    byte[] value = Bytes.toBytes((FIXED_PART + 0).toCharArray());
    IdxScan idxScan = new IdxScan();
    idxScan.setExpression(Expression.comparison(family, qualifier,
      Comparison.Operator.EQ, value));
    idxScan.setFilter(new SingleColumnValueFilter(family, qualifier,
      CompareFilter.CompareOp.EQUAL, value));
    idxScan.setCaching(1000);

    int count = 0;
    int finalCount = maxRows / 10;
    int printCount = 0;
    while (count < finalCount) {
      ResultScanner scanner = table.getScanner(idxScan);
      int nextCount = 0;
      for (Result res : scanner) {
        nextCount++;
        Assert.assertTrue(Arrays.equals(res.getValue(family, qualifier),
          value));
      }
      if (nextCount > printCount + 1000) {
        System.out.printf("++ found %d matching rows\n", nextCount);
        printCount = nextCount;
      }
      String infoString = "nextCount=" + nextCount + ", count=" + count +
        ", finalCount=" + finalCount;
      boolean condition = nextCount >= count && nextCount <= finalCount;
      if (!condition) {
        System.out.println("-------- " + infoString);
      }
      Assert.assertTrue(infoString, condition);
      count = nextCount;
    }
    service.shutdown();
  }


  private static class Writer implements Callable {

    private HTable table;
    private byte[] family;
    private byte[] qualifier;
    private AtomicInteger sequence;
    private byte[][] rows;

    private Writer(HTable table, byte[] family, byte[] qualifier,
      AtomicInteger sequence, byte[][] rows) {
      this.table = table;
      this.family = family;
      this.qualifier = qualifier;
      this.sequence = sequence;
      this.rows = rows;
    }

    @Override
    public Object call() throws Exception {
      while (true) {
        int num = sequence.getAndIncrement();
        if (num % 10 == 0) {
          System.out.printf("-- writing row %d\n", num);
        }
        if (num <= rows.length) {
          Put put = new Put(rows[num]);
          char[] chars = (FIXED_PART + num % 10).toCharArray();
          put.add(family, qualifier, Bytes.toBytes(chars));
          table.put(put);
        } else {
          return null;
        }
        //Thread.sleep(0L, 100000); // sleep .1 millis
      }
    }
  }
}