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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestStorageSize {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestStorageSize.class);

  @Test
  public void testParse() {
    // megabytes
    StorageSize size = StorageSize.parse("20m");
    assertEquals(StorageUnit.MB, size.getUnit());
    assertEquals(20.0d, size.getValue(), 0.0001);
    size = StorageSize.parse("40mb");
    assertEquals(StorageUnit.MB, size.getUnit());
    assertEquals(40.0d, size.getValue(), 0.0001);
    size = StorageSize.parse("60megabytes");
    assertEquals(StorageUnit.MB, size.getUnit());
    assertEquals(60.0d, size.getValue(), 0.0001);

    // gigabytes
    size = StorageSize.parse("10g");
    assertEquals(StorageUnit.GB, size.getUnit());
    assertEquals(10.0d, size.getValue(), 0.0001);
    size = StorageSize.parse("30gb");
    assertEquals(StorageUnit.GB, size.getUnit());
    assertEquals(30.0d, size.getValue(), 0.0001);
    size = StorageSize.parse("50gigabytes");
    assertEquals(StorageUnit.GB, size.getUnit());
    assertEquals(50.0d, size.getValue(), 0.0001);
  }

  @Test
  public void testGetStorageSize() {
    assertEquals(1024 * 1024 * 4, StorageSize.getStorageSize("4m", -1, StorageUnit.BYTES), 0.0001);
    assertEquals(1024 * 6, StorageSize.getStorageSize("6g", -1, StorageUnit.MB), 0.0001);
    assertEquals(-1, StorageSize.getStorageSize(null, -1, StorageUnit.BYTES), 0.0001);
    assertEquals(-2, StorageSize.getStorageSize("", -2, StorageUnit.BYTES), 0.0001);
  }
}
