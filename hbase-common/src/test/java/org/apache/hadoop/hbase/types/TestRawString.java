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
package org.apache.hadoop.hbase.types;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestRawString {

  static final String[] VALUES = new String[] {
    "", "1", "22", "333", "4444", "55555", "666666", "7777777", "88888888", "999999999",
  };

  @Test
  public void testReadWrite() {
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      RawString type =
          Order.ASCENDING == ord ? RawString.ASCENDING : RawString.DESCENDING;
      for (String val : VALUES) {
        PositionedByteRange buff = new SimplePositionedByteRange(Bytes.toBytes(val).length);
        assertEquals(buff.getLength(), type.encode(buff, val));
        byte[] expected = Bytes.toBytes(val);
        ord.apply(expected);
        assertArrayEquals(expected, buff.getBytes());
        buff.setPosition(0);
        assertEquals(val, type.decode(buff));
        buff.setPosition(0);
        assertEquals(buff.getLength(), type.skip(buff));
        assertEquals(buff.getLength(), buff.getPosition());
      }
    }
  }
}
