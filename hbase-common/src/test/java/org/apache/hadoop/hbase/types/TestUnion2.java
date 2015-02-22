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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestUnion2 {

  /**
   * An example <code>Union</code>
   */
  private static class SampleUnion1 extends Union2<Integer, String> {

    private static final byte IS_INTEGER = 0x00;
    private static final byte IS_STRING  = 0x01;

    public SampleUnion1() {
      super(new RawInteger(), new RawStringTerminated(Order.DESCENDING, "."));
    }

    @Override
    public int skip(PositionedByteRange src) {
      switch (src.get()) {
        case IS_INTEGER:
          return 1 + typeA.skip(src);
        case IS_STRING:
          return 1 + typeB.skip(src);
        default:
          throw new IllegalArgumentException("Unrecognized encoding format.");
      }
    }

    @Override
    public Object decode(PositionedByteRange src) {
      switch (src.get()) {
        case IS_INTEGER:
          return typeA.decode(src);
        case IS_STRING:
          return typeB.decode(src);
        default:
          throw new IllegalArgumentException("Unrecognized encoding format.");
      }
    }

    @Override
    public int encodedLength(Object val) {
      Integer i = null;
      String s = null;
      try {
        i = (Integer) val;
      } catch (ClassCastException e) {}
      try {
        s = (String) val;
      } catch (ClassCastException e) {}

      if (null != i) return 1 + typeA.encodedLength(i);
      if (null != s) return 1 + typeB.encodedLength(s);
      throw new IllegalArgumentException("val is not a valid member of this union.");
    }

    @Override
    public int encode(PositionedByteRange dst, Object val) {
      Integer i = null;
      String s = null;
      try {
        i = (Integer) val;
      } catch (ClassCastException e) {}
      try {
        s = (String) val;
      } catch (ClassCastException e) {}

      if (null != i) {
        dst.put(IS_INTEGER);
        return 1 + typeA.encode(dst, i);
      } else if (null != s) {
        dst.put(IS_STRING);
        return 1 + typeB.encode(dst, s);
      }
      else
        throw new IllegalArgumentException("val is not of a supported type.");
    }
  }

  @Test
  public void testEncodeDecode() {
    Integer intVal = Integer.valueOf(10);
    String strVal = "hello";
    PositionedByteRange buff = new SimplePositionedMutableByteRange(10);
    SampleUnion1 type = new SampleUnion1();

    type.encode(buff, intVal);
    buff.setPosition(0);
    assertTrue(0 == intVal.compareTo(type.decodeA(buff)));
    buff.setPosition(0);
    type.encode(buff, strVal);
    buff.setPosition(0);
    assertTrue(0 == strVal.compareTo(type.decodeB(buff)));
  }

  @Test
  public void testSkip() {
    Integer intVal = Integer.valueOf(10);
    String strVal = "hello";
    PositionedByteRange buff = new SimplePositionedMutableByteRange(10);
    SampleUnion1 type = new SampleUnion1();

    int len = type.encode(buff, intVal);
    buff.setPosition(0);
    assertEquals(len, type.skip(buff));
    buff.setPosition(0);
    len = type.encode(buff, strVal);
    buff.setPosition(0);
    assertEquals(len, type.skip(buff));
  }
}
