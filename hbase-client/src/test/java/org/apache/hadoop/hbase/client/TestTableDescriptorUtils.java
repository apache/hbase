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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptorUtils.TableDescriptorDelta;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestTableDescriptorUtils {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTableDescriptorUtils.class);

  @Test
  public void testDelta() {
    ColumnFamilyDescriptor cf1 = ColumnFamilyDescriptorBuilder.of("cf1");
    ColumnFamilyDescriptor cf2 = ColumnFamilyDescriptorBuilder.of("cf2");
    ColumnFamilyDescriptor cf3 = ColumnFamilyDescriptorBuilder.of("cf3");
    ColumnFamilyDescriptor cf4 = ColumnFamilyDescriptorBuilder.of("cf4");
    TableDescriptor td = TableDescriptorBuilder
        .newBuilder(TableName.valueOf("test"))
        .setColumnFamilies(Arrays.asList(cf1, cf2, cf3, cf4))
        .build();

    TableDescriptorDelta selfCompare = TableDescriptorUtils.computeDelta(td, td);
    assertEquals(0, selfCompare.getColumnsAdded().size());
    assertEquals(0, selfCompare.getColumnsDeleted().size());
    assertEquals(0, selfCompare.getColumnsModified().size());

    ColumnFamilyDescriptor modCf2 = ColumnFamilyDescriptorBuilder
        .newBuilder(cf2).setMaxVersions(5).build();
    ColumnFamilyDescriptor modCf3 = ColumnFamilyDescriptorBuilder
        .newBuilder(cf3).setMaxVersions(5).build();
    ColumnFamilyDescriptor cf5 = ColumnFamilyDescriptorBuilder.of("cf5");
    ColumnFamilyDescriptor cf6 = ColumnFamilyDescriptorBuilder.of("cf6");
    ColumnFamilyDescriptor cf7 = ColumnFamilyDescriptorBuilder.of("cf7");
    TableDescriptor newTd = TableDescriptorBuilder
        .newBuilder(td)
        .removeColumnFamily(Bytes.toBytes("cf1"))
        .modifyColumnFamily(modCf2)
        .modifyColumnFamily(modCf3)
        .setColumnFamily(cf5)
        .setColumnFamily(cf6)
        .setColumnFamily(cf7)
        .build();

    TableDescriptorDelta delta = TableDescriptorUtils.computeDelta(td, newTd);

    assertEquals(3, delta.getColumnsAdded().size());
    assertEquals(1, delta.getColumnsDeleted().size());
    assertEquals(2, delta.getColumnsModified().size());

    TableDescriptorDelta inverseDelta = TableDescriptorUtils.computeDelta(newTd, td);

    // Equality here relies on implementation detail of the returned Set being a TreeSet
    assertEquals(delta.getColumnsDeleted(), inverseDelta.getColumnsAdded());
    assertEquals(delta.getColumnsAdded(), inverseDelta.getColumnsDeleted());
    assertEquals(delta.getColumnsModified(), inverseDelta.getColumnsModified());
  }
}
