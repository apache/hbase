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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.hfile.CorruptHFileException;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Tests a scanner on a corrupt hfile.
 */
@Category(MediumTests.class)
public class TestScannerWithCorruptHFile {
  @Rule public TestName name = new TestName();
  private static final byte[] FAMILY_NAME = Bytes.toBytes("f");
  private final static HBaseTestingUtility TEST_UTIL = HBaseTestingUtility.createLocalHTU();


  @BeforeClass
  public static void setup() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public static class CorruptHFileCoprocessor extends BaseRegionObserver {
    @Override
    public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> e,
      InternalScanner s, List<Result> results, int limit, boolean hasMore) throws IOException {
      throw new CorruptHFileException("For test");
    }
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testScanOnCorruptHFile() throws IOException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addCoprocessor(CorruptHFileCoprocessor.class.getName());
    htd.addFamily(new HColumnDescriptor(FAMILY_NAME));
    Table table = TEST_UTIL.createTable(htd, null);
    try {
      loadTable(table, 1);
      scan(table);
    } finally {
      table.close();
    }
  }

  private void loadTable(Table table, int numRows) throws IOException {
    for (int i = 0; i < numRows; ++i) {
      byte[] row = Bytes.toBytes(i);
      Put put = new Put(row);
      put.setDurability(Durability.SKIP_WAL);
      put.addColumn(FAMILY_NAME, null, row);
      table.put(put);
    }
  }

  private void scan(Table table) throws IOException {
    Scan scan = new Scan();
    scan.setCaching(1);
    scan.setCacheBlocks(false);
    ResultScanner scanner = table.getScanner(scan);
    try {
      scanner.next();
    } finally {
      scanner.close();
    }
  }
}
