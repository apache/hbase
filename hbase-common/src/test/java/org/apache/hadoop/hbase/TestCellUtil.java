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

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestCellUtil {

  @Test
  public void testOverlappingKeys() {
    byte[] empty = HConstants.EMPTY_BYTE_ARRAY;
    byte[] a = Bytes.toBytes("a");
    byte[] b = Bytes.toBytes("b");
    byte[] c = Bytes.toBytes("c");
    byte[] d = Bytes.toBytes("d");

    // overlaps
    Assert.assertTrue(CellUtil.overlappingKeys(a, b, a, b));
    Assert.assertTrue(CellUtil.overlappingKeys(a, c, a, b));
    Assert.assertTrue(CellUtil.overlappingKeys(a, b, a, c));
    Assert.assertTrue(CellUtil.overlappingKeys(b, c, a, c));
    Assert.assertTrue(CellUtil.overlappingKeys(a, c, b, c));
    Assert.assertTrue(CellUtil.overlappingKeys(a, d, b, c));
    Assert.assertTrue(CellUtil.overlappingKeys(b, c, a, d));

    Assert.assertTrue(CellUtil.overlappingKeys(empty, b, a, b));
    Assert.assertTrue(CellUtil.overlappingKeys(empty, b, a, c));

    Assert.assertTrue(CellUtil.overlappingKeys(a, b, empty, b));
    Assert.assertTrue(CellUtil.overlappingKeys(a, b, empty, c));

    Assert.assertTrue(CellUtil.overlappingKeys(a, empty, a, b));
    Assert.assertTrue(CellUtil.overlappingKeys(a, empty, a, c));

    Assert.assertTrue(CellUtil.overlappingKeys(a, b, empty, empty));
    Assert.assertTrue(CellUtil.overlappingKeys(empty, empty, a, b));

    // non overlaps
    Assert.assertFalse(CellUtil.overlappingKeys(a, b, c, d));
    Assert.assertFalse(CellUtil.overlappingKeys(c, d, a, b));

    Assert.assertFalse(CellUtil.overlappingKeys(b, c, c, d));
    Assert.assertFalse(CellUtil.overlappingKeys(b, c, c, empty));
    Assert.assertFalse(CellUtil.overlappingKeys(b, c, d, empty));
    Assert.assertFalse(CellUtil.overlappingKeys(c, d, b, c));
    Assert.assertFalse(CellUtil.overlappingKeys(c, empty, b, c));
    Assert.assertFalse(CellUtil.overlappingKeys(d, empty, b, c));

    Assert.assertFalse(CellUtil.overlappingKeys(b, c, a, b));
    Assert.assertFalse(CellUtil.overlappingKeys(b, c, empty, b));
    Assert.assertFalse(CellUtil.overlappingKeys(b, c, empty, a));
    Assert.assertFalse(CellUtil.overlappingKeys(a,b, b, c));
    Assert.assertFalse(CellUtil.overlappingKeys(empty, b, b, c));
    Assert.assertFalse(CellUtil.overlappingKeys(empty, a, b, c));
  }
}
