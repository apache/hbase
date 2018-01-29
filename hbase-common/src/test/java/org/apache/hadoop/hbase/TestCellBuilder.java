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

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestCellBuilder {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCellBuilder.class);

  private static final byte OLD_DATA = 87;
  private static final byte NEW_DATA = 100;

  @Test
  public void testCellBuilderWithDeepCopy() {
    byte[] row = new byte[]{OLD_DATA};
    byte[] family = new byte[]{OLD_DATA};
    byte[] qualifier = new byte[]{OLD_DATA};
    byte[] value = new byte[]{OLD_DATA};
    Cell cell = CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
            .setRow(row)
            .setFamily(family)
            .setQualifier(qualifier)
            .setType(Cell.Type.Put)
            .setValue(value)
            .build();
    row[0] = NEW_DATA;
    family[0] = NEW_DATA;
    qualifier[0] = NEW_DATA;
    value[0] = NEW_DATA;
    assertEquals(OLD_DATA, cell.getRowArray()[cell.getRowOffset()]);
    assertEquals(OLD_DATA, cell.getFamilyArray()[cell.getFamilyOffset()]);
    assertEquals(OLD_DATA, cell.getQualifierArray()[cell.getQualifierOffset()]);
    assertEquals(OLD_DATA, cell.getValueArray()[cell.getValueOffset()]);
  }

  @Test
  public void testCellBuilderWithShallowCopy() {
    byte[] row = new byte[]{OLD_DATA};
    byte[] family = new byte[]{OLD_DATA};
    byte[] qualifier = new byte[]{OLD_DATA};
    byte[] value = new byte[]{OLD_DATA};
    Cell cell = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
            .setRow(row)
            .setFamily(family)
            .setQualifier(qualifier)
            .setType(Cell.Type.Put)
            .setValue(value)
            .build();
    row[0] = NEW_DATA;
    family[0] = NEW_DATA;
    qualifier[0] = NEW_DATA;
    value[0] = NEW_DATA;
    assertEquals(NEW_DATA, cell.getRowArray()[cell.getRowOffset()]);
    assertEquals(NEW_DATA, cell.getFamilyArray()[cell.getFamilyOffset()]);
    assertEquals(NEW_DATA, cell.getQualifierArray()[cell.getQualifierOffset()]);
    assertEquals(NEW_DATA, cell.getValueArray()[cell.getValueOffset()]);
  }

  @Test
  public void testExtendedCellBuilderWithShallowCopy() {
    byte[] row = new byte[]{OLD_DATA};
    byte[] family = new byte[]{OLD_DATA};
    byte[] qualifier = new byte[]{OLD_DATA};
    byte[] value = new byte[]{OLD_DATA};
    byte[] tags = new byte[]{OLD_DATA};
    long seqId = 999;
    Cell cell = ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
            .setRow(row)
            .setFamily(family)
            .setQualifier(qualifier)
            .setType(KeyValue.Type.Put.getCode())
            .setValue(value)
            .setTags(tags)
            .setSequenceId(seqId)
            .build();
    row[0] = NEW_DATA;
    family[0] = NEW_DATA;
    qualifier[0] = NEW_DATA;
    value[0] = NEW_DATA;
    tags[0] = NEW_DATA;
    assertEquals(NEW_DATA, cell.getRowArray()[cell.getRowOffset()]);
    assertEquals(NEW_DATA, cell.getFamilyArray()[cell.getFamilyOffset()]);
    assertEquals(NEW_DATA, cell.getQualifierArray()[cell.getQualifierOffset()]);
    assertEquals(NEW_DATA, cell.getValueArray()[cell.getValueOffset()]);
    assertEquals(NEW_DATA, cell.getTagsArray()[cell.getTagsOffset()]);
    assertEquals(seqId, cell.getSequenceId());
  }

  @Test
  public void testExtendedCellBuilderWithDeepCopy() {
    byte[] row = new byte[]{OLD_DATA};
    byte[] family = new byte[]{OLD_DATA};
    byte[] qualifier = new byte[]{OLD_DATA};
    byte[] value = new byte[]{OLD_DATA};
    byte[] tags = new byte[]{OLD_DATA};
    long seqId = 999;
    Cell cell = ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
            .setRow(row)
            .setFamily(family)
            .setQualifier(qualifier)
            .setType(KeyValue.Type.Put.getCode())
            .setValue(value)
            .setTags(tags)
            .setSequenceId(seqId)
            .build();
    row[0] = NEW_DATA;
    family[0] = NEW_DATA;
    qualifier[0] = NEW_DATA;
    value[0] = NEW_DATA;
    tags[0] = NEW_DATA;
    assertEquals(OLD_DATA, cell.getRowArray()[cell.getRowOffset()]);
    assertEquals(OLD_DATA, cell.getFamilyArray()[cell.getFamilyOffset()]);
    assertEquals(OLD_DATA, cell.getQualifierArray()[cell.getQualifierOffset()]);
    assertEquals(OLD_DATA, cell.getValueArray()[cell.getValueOffset()]);
    assertEquals(OLD_DATA, cell.getTagsArray()[cell.getTagsOffset()]);
    assertEquals(seqId, cell.getSequenceId());
  }
}
