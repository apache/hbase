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
package org.apache.hadoop.hbase.mapred;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({MapReduceTests.class, SmallTests.class})
public class TestSplitTable {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSplitTable.class);

  @Rule
  public TestName name = new TestName();

  @Test
  @SuppressWarnings({"deprecation", "SelfComparison"})
  public void testSplitTableCompareTo() {
    TableSplit aTableSplit = new TableSplit(Bytes.toBytes("tableA"),
        Bytes.toBytes("aaa"), Bytes.toBytes("ddd"), "locationA");

    TableSplit bTableSplit = new TableSplit(Bytes.toBytes("tableA"),
        Bytes.toBytes("iii"), Bytes.toBytes("kkk"), "locationA");

    TableSplit cTableSplit = new TableSplit(Bytes.toBytes("tableA"),
        Bytes.toBytes("lll"), Bytes.toBytes("zzz"), "locationA");

    assertEquals(0, aTableSplit.compareTo(aTableSplit));
    assertEquals(0, bTableSplit.compareTo(bTableSplit));
    assertEquals(0, cTableSplit.compareTo(cTableSplit));

    assertTrue(aTableSplit.compareTo(bTableSplit) < 0);
    assertTrue(bTableSplit.compareTo(aTableSplit) > 0);

    assertTrue(aTableSplit.compareTo(cTableSplit) < 0);
    assertTrue(cTableSplit.compareTo(aTableSplit) > 0);

    assertTrue(bTableSplit.compareTo(cTableSplit) < 0);
    assertTrue(cTableSplit.compareTo(bTableSplit) > 0);

    assertTrue(cTableSplit.compareTo(aTableSplit) > 0);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testSplitTableEquals() {
    byte[] tableA = Bytes.toBytes("tableA");
    byte[] aaa = Bytes.toBytes("aaa");
    byte[] ddd = Bytes.toBytes("ddd");
    String locationA = "locationA";

    TableSplit tablesplit = new TableSplit(tableA, aaa, ddd, locationA);

    TableSplit tableB = new TableSplit(Bytes.toBytes("tableB"), aaa, ddd, locationA);
    assertNotEquals(tablesplit.hashCode(), tableB.hashCode());
    assertNotEquals(tablesplit, tableB);

    TableSplit startBbb = new TableSplit(tableA, Bytes.toBytes("bbb"), ddd, locationA);
    assertNotEquals(tablesplit.hashCode(), startBbb.hashCode());
    assertNotEquals(tablesplit, startBbb);

    TableSplit endEee = new TableSplit(tableA, aaa, Bytes.toBytes("eee"), locationA);
    assertNotEquals(tablesplit.hashCode(), endEee.hashCode());
    assertNotEquals(tablesplit, endEee);

    TableSplit locationB = new TableSplit(tableA, aaa, ddd, "locationB");
    assertNotEquals(tablesplit.hashCode(), locationB.hashCode());
    assertNotEquals(tablesplit, locationB);

    TableSplit same = new TableSplit(tableA, aaa, ddd, locationA);
    assertEquals(tablesplit.hashCode(), same.hashCode());
    assertEquals(tablesplit, same);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testToString() {
    TableSplit split =
        new TableSplit(TableName.valueOf(name.getMethodName()), "row-start".getBytes(), "row-end".getBytes(),
            "location");
    String str =
        "HBase table split(table name: " + name.getMethodName() + ", start row: row-start, "
            + "end row: row-end, region location: location)";
    Assert.assertEquals(str, split.toString());

    split = new TableSplit((TableName) null, null, null, null);
    str =
        "HBase table split(table name: null, start row: null, "
            + "end row: null, region location: null)";
    Assert.assertEquals(str, split.toString());
  }
}
