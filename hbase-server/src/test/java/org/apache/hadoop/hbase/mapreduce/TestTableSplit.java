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
package org.apache.hadoop.hbase.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(SmallTests.class)
public class TestTableSplit {
  @Test
  public void testHashCode() {
    TableSplit split1 = new TableSplit(TableName.valueOf("table"),
        "row-start".getBytes(),
        "row-end".getBytes(), "location");
    TableSplit split2 = new TableSplit(TableName.valueOf("table"),
        "row-start".getBytes(),
        "row-end".getBytes(), "location");
    assertEquals (split1, split2);
    assertTrue   (split1.hashCode() == split2.hashCode());
    HashSet<TableSplit> set = new HashSet<TableSplit>(2);
    set.add(split1);
    set.add(split2);
    assertTrue(set.size() == 1);
  }

  /**
   * length of region should not influence hashcode
   * */
  @Test
  public void testHashCode_length() {
    TableSplit split1 = new TableSplit(TableName.valueOf("table"),
            "row-start".getBytes(),
            "row-end".getBytes(), "location", 1984);
    TableSplit split2 = new TableSplit(TableName.valueOf("table"),
            "row-start".getBytes(),
            "row-end".getBytes(), "location", 1982);

    assertEquals (split1, split2);
    assertTrue   (split1.hashCode() == split2.hashCode());
    HashSet<TableSplit> set = new HashSet<TableSplit>(2);
    set.add(split1);
    set.add(split2);
    assertTrue(set.size() == 1);
  }

  /**
   * Length of region need to be properly serialized.
   * */
  @Test
  public void testLengthIsSerialized() throws Exception {
    TableSplit split1 = new TableSplit(TableName.valueOf("table"),
            "row-start".getBytes(),
            "row-end".getBytes(), "location", 666);

    TableSplit deserialized = new TableSplit(TableName.valueOf("table"),
            "row-start2".getBytes(),
            "row-end2".getBytes(), "location1");
    ReflectionUtils.copy(new Configuration(), split1, deserialized);

    Assert.assertEquals(666, deserialized.getLength());
  }

  @Test
  public void testToString() {
    TableSplit split =
        new TableSplit(TableName.valueOf("table"), "row-start".getBytes(), "row-end".getBytes(),
            "location");
    String str =
        "HBase table split(table name: table, scan: , start row: row-start, "
            + "end row: row-end, region location: location)";
    Assert.assertEquals(str, split.toString());

    split = new TableSplit((TableName) null, null, null, null);
    str =
        "HBase table split(table name: null, scan: , start row: null, "
            + "end row: null, region location: null)";
    Assert.assertEquals(str, split.toString());
  }
}

