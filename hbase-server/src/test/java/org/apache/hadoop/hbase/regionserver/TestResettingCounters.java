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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({RegionServerTests.class, SmallTests.class})
public class TestResettingCounters {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestResettingCounters.class);

  @Rule
  public TestName name = new TestName();

  @Test
  public void testResettingCounters() throws Exception {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    Configuration conf = htu.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    byte [] table = Bytes.toBytes(name.getMethodName());
    byte [][] families = new byte [][] {
        Bytes.toBytes("family1"),
        Bytes.toBytes("family2"),
        Bytes.toBytes("family3")
    };
    int numQualifiers = 10;
    byte [][] qualifiers = new byte [numQualifiers][];
    for (int i=0; i<numQualifiers; i++) qualifiers[i] = Bytes.toBytes("qf" + i);
    int numRows = 10;
    byte [][] rows = new byte [numRows][];
    for (int i=0; i<numRows; i++) rows[i] = Bytes.toBytes("r" + i);

    TableDescriptorBuilder.ModifyableTableDescriptor tableDescriptor =
      new TableDescriptorBuilder.ModifyableTableDescriptor(TableName.valueOf(table));
    for (byte[] family : families) {
      tableDescriptor.setColumnFamily(
        new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(family));
    }

    HRegionInfo hri = new HRegionInfo(tableDescriptor.getTableName(), null, null, false);
    String testDir = htu.getDataTestDir() + "/TestResettingCounters/";
    Path path = new Path(testDir);
    if (fs.exists(path)) {
      if (!fs.delete(path, true)) {
        throw new IOException("Failed delete of " + path);
      }
    }
    HRegion region = HBaseTestingUtility.createRegionAndWAL(hri, path, conf, tableDescriptor);
    try {
      Increment odd = new Increment(rows[0]);
      odd.setDurability(Durability.SKIP_WAL);
      Increment even = new Increment(rows[0]);
      even.setDurability(Durability.SKIP_WAL);
      Increment all = new Increment(rows[0]);
      all.setDurability(Durability.SKIP_WAL);
      for (int i=0;i<numQualifiers;i++) {
        if (i % 2 == 0) even.addColumn(families[0], qualifiers[i], 1);
        else odd.addColumn(families[0], qualifiers[i], 1);
        all.addColumn(families[0], qualifiers[i], 1);
      }

      // increment odd qualifiers 5 times and flush
      for (int i=0;i<5;i++) region.increment(odd, HConstants.NO_NONCE, HConstants.NO_NONCE);
      region.flush(true);

      // increment even qualifiers 5 times
      for (int i=0;i<5;i++) region.increment(even, HConstants.NO_NONCE, HConstants.NO_NONCE);

      // increment all qualifiers, should have value=6 for all
      Result result = region.increment(all, HConstants.NO_NONCE, HConstants.NO_NONCE);
      assertEquals(numQualifiers, result.size());
      Cell[] kvs = result.rawCells();
      for (int i=0;i<kvs.length;i++) {
        System.out.println(kvs[i].toString());
        assertTrue(CellUtil.matchingQualifier(kvs[i], qualifiers[i]));
        assertEquals(6, Bytes.toLong(CellUtil.cloneValue(kvs[i])));
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(region);
    }
    HBaseTestingUtility.closeRegionAndWAL(region);
  }

}

