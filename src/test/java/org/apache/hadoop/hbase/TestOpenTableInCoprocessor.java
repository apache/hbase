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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that a coprocessor can open a connection and write to another table, inside a hook.
 */
@Category(MediumTests.class)
public class TestOpenTableInCoprocessor {

  private static final byte[] otherTable = Bytes.toBytes("otherTable");
  private static final byte[] family = new byte[] { 'f' };

  private static boolean completed = false;

  /**
   * Custom coprocessor that just copies the write to another table.
   */
  public static class SendToOtherTableCoprocessor extends BaseRegionObserver {

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit,
        boolean writeToWAL) throws IOException {
      HTableInterface table = e.getEnvironment().getTable(otherTable);
      Put p = new Put(new byte[] { 'a' });
      p.add(family, null, new byte[] { 'a' });
      table.put(put);
      table.flushCommits();
      completed = true;
      table.close();
    }

  }

  @Test
  public void testCoprocessorCanCreateConnectionToRemoteTable() throws Throwable {
    HBaseTestingUtility UTIL = new HBaseTestingUtility();
    HTableDescriptor primary = new HTableDescriptor("primary");
    primary.addFamily(new HColumnDescriptor(family));
    // add our coprocessor
    primary.addCoprocessor(SendToOtherTableCoprocessor.class.getName());

    HTableDescriptor other = new HTableDescriptor(otherTable);
    other.addFamily(new HColumnDescriptor(family));
    UTIL.startMiniCluster();

    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.createTable(primary);
    admin.createTable(other);
    admin.close();

    HTable table = new HTable(UTIL.getConfiguration(), "primary");
    Put p = new Put(new byte[] { 'a' });
    p.add(family, null, new byte[] { 'a' });
    table.put(p);
    table.flushCommits();
    table.close();

    HTable target = new HTable(UTIL.getConfiguration(), otherTable);
    assertTrue("Didn't complete update to target table!", completed);
    assertEquals("Didn't find inserted row", 1, getKeyValueCount(target));
    target.close();

    UTIL.shutdownMiniCluster();
  }

  /**
   * Count the number of keyvalue in the table. Scans all possible versions
   * @param table table to scan
   * @return number of keyvalues over all rows in the table
   * @throws IOException
   */
  private int getKeyValueCount(HTable table) throws IOException {
    Scan scan = new Scan();
    scan.setMaxVersions(Integer.MAX_VALUE - 1);

    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      count += res.list().size();
      System.out.println(count + ") " + res);
    }
    results.close();

    return count;
  }
}