/*
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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(MapReduceTests.TAG)
@Tag(SmallTests.TAG)
public class TestTableSplit {

  @Test
  public void testHashCode(TestInfo testInfo) {
    TableSplit split1 = new TableSplit(TableName.valueOf(testInfo.getTestMethod().get().getName()),
      Bytes.toBytes("row-start"), Bytes.toBytes("row-end"), "location");
    TableSplit split2 = new TableSplit(TableName.valueOf(testInfo.getTestMethod().get().getName()),
      Bytes.toBytes("row-start"), Bytes.toBytes("row-end"), "location");
    assertEquals(split1, split2);
    assertTrue(split1.hashCode() == split2.hashCode());
    HashSet<TableSplit> set = new HashSet<>(2);
    set.add(split1);
    set.add(split2);
    assertEquals(1, set.size());
  }

  /**
   * length of region should not influence hashcode
   */
  @Test
  public void testHashCode_length(TestInfo testInfo) {
    TableSplit split1 = new TableSplit(TableName.valueOf(testInfo.getTestMethod().get().getName()),
      Bytes.toBytes("row-start"), Bytes.toBytes("row-end"), "location", 1984);
    TableSplit split2 = new TableSplit(TableName.valueOf(testInfo.getTestMethod().get().getName()),
      Bytes.toBytes("row-start"), Bytes.toBytes("row-end"), "location", 1982);

    assertEquals(split1, split2);
    assertTrue(split1.hashCode() == split2.hashCode());
    HashSet<TableSplit> set = new HashSet<>(2);
    set.add(split1);
    set.add(split2);
    assertEquals(1, set.size());
  }

  /**
   * Length of region need to be properly serialized.
   */
  @Test
  public void testLengthIsSerialized(TestInfo testInfo) throws Exception {
    TableSplit split1 = new TableSplit(TableName.valueOf(testInfo.getTestMethod().get().getName()),
      Bytes.toBytes("row-start"), Bytes.toBytes("row-end"), "location", 666);

    TableSplit deserialized =
      new TableSplit(TableName.valueOf(testInfo.getTestMethod().get().getName()),
        Bytes.toBytes("row-start2"), Bytes.toBytes("row-end2"), "location1");
    ReflectionUtils.copy(new Configuration(), split1, deserialized);

    assertEquals(666, deserialized.getLength());
  }

  @Test
  public void testToString(TestInfo testInfo) {
    TableSplit split = new TableSplit(TableName.valueOf(testInfo.getTestMethod().get().getName()),
      Bytes.toBytes("row-start"), Bytes.toBytes("row-end"), "location");
    String str = "Split(tablename=" + testInfo.getTestMethod().get().getName()
      + ", startrow=row-start, " + "endrow=row-end, regionLocation=location, " + "regionname=)";
    assertEquals(str, split.toString());

    split = new TableSplit(TableName.valueOf(testInfo.getTestMethod().get().getName()), null,
      Bytes.toBytes("row-start"), Bytes.toBytes("row-end"), "location", "encoded-region-name",
      1000L);
    str = "Split(tablename=" + testInfo.getTestMethod().get().getName() + ", startrow=row-start, "
      + "endrow=row-end, regionLocation=location, " + "regionname=encoded-region-name)";
    assertEquals(str, split.toString());

    split = new TableSplit(null, null, null, null);
    str = "Split(tablename=null, startrow=null, " + "endrow=null, regionLocation=null, "
      + "regionname=)";
    assertEquals(str, split.toString());

    split = new TableSplit(null, null, null, null, null, null, 1000L);
    str = "Split(tablename=null, startrow=null, " + "endrow=null, regionLocation=null, "
      + "regionname=null)";
    assertEquals(str, split.toString());
  }
}
