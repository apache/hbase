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
package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestPositionedByteRange {
  @Test
  public void testPosition() {
    PositionedByteRange r = new SimplePositionedByteRange(new byte[5], 1, 3);

    // exercise single-byte put
    r.put(Bytes.toBytes("f")[0])
     .put(Bytes.toBytes("o")[0])
     .put(Bytes.toBytes("o")[0]);
    Assert.assertEquals(3, r.getPosition());
    Assert.assertArrayEquals(
      new byte[] { 0, Bytes.toBytes("f")[0], Bytes.toBytes("o")[0], Bytes.toBytes("o")[0], 0 },
      r.getBytes());

    // exercise multi-byte put
    r.setPosition(0);
    r.put(Bytes.toBytes("f"))
     .put(Bytes.toBytes("o"))
     .put(Bytes.toBytes("o"));
    Assert.assertEquals(3, r.getPosition());
    Assert.assertArrayEquals(
      new byte[] { 0, Bytes.toBytes("f")[0], Bytes.toBytes("o")[0], Bytes.toBytes("o")[0], 0 },
      r.getBytes());

    // exercise single-byte get
    r.setPosition(0);
    Assert.assertEquals(Bytes.toBytes("f")[0], r.get());
    Assert.assertEquals(Bytes.toBytes("o")[0], r.get());
    Assert.assertEquals(Bytes.toBytes("o")[0], r.get());

    r.setPosition(1);
    Assert.assertEquals(Bytes.toBytes("o")[0], r.get());

    // exercise multi-byte get
    r.setPosition(0);
    byte[] dst = new byte[3];
    r.get(dst);
    Assert.assertArrayEquals(Bytes.toBytes("foo"), dst);

    // set position to the end of the range; this should not throw.
    r.setPosition(3);
  }
}
