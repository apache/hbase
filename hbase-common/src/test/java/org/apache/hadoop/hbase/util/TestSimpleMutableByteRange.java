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
package org.apache.hadoop.hbase.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestSimpleMutableByteRange {

  @Test
  public void testEmpty() {
    assertTrue(SimpleMutableByteRange.isEmpty(null));
    ByteRange r = new SimpleMutableByteRange();
    assertTrue(SimpleMutableByteRange.isEmpty(r));
    assertTrue(r.isEmpty());
    r.set(new byte[0]);
    assertEquals(0, r.getBytes().length);
    assertEquals(0, r.getOffset());
    assertEquals(0, r.getLength());
    assertTrue(Bytes.equals(new byte[0], r.deepCopyToNewArray()));
    assertEquals(0, r.compareTo(new SimpleMutableByteRange(new byte[0], 0, 0)));
    assertEquals(0, r.hashCode());
  }

  @Test
  public void testBasics() {
    ByteRange r = new SimpleMutableByteRange(new byte[] { 1, 3, 2 });
    assertFalse(SimpleMutableByteRange.isEmpty(r));
    assertNotNull(r.getBytes());// should be empty byte[], but could change this behavior
    assertEquals(3, r.getBytes().length);
    assertEquals(0, r.getOffset());
    assertEquals(3, r.getLength());

    // cloning (deep copying)
    assertTrue(Bytes.equals(new byte[] { 1, 3, 2 }, r.deepCopyToNewArray()));
    assertNotSame(r.getBytes(), r.deepCopyToNewArray());

    // hash code
    assertTrue(r.hashCode() > 0);
    assertEquals(r.hashCode(), r.deepCopy().hashCode());

    // copying to arrays
    byte[] destination = new byte[] { -59 };// junk
    r.deepCopySubRangeTo(2, 1, destination, 0);
    assertTrue(Bytes.equals(new byte[] { 2 }, destination));

    // set length
    r.setLength(1);
    assertTrue(Bytes.equals(new byte[] { 1 }, r.deepCopyToNewArray()));
    r.setLength(2);// verify we retained the 2nd byte, but dangerous in real code
    assertTrue(Bytes.equals(new byte[] { 1, 3 }, r.deepCopyToNewArray()));
  }

  @Test
  public void testPutandGetPrimitiveTypes() throws Exception {
    ByteRange r = new SimpleMutableByteRange(100);
    int offset = 0;
    int i1 = 18, i2 = 2;
    short s1 = 0;
    long l1 = 1234L, l2 = 0;
    r.putInt(offset, i1);
    offset += Bytes.SIZEOF_INT;
    r.putInt(offset, i2);
    offset += Bytes.SIZEOF_INT;
    r.putShort(offset, s1);
    offset += Bytes.SIZEOF_SHORT;
    r.putLong(offset, l1);
    offset += Bytes.SIZEOF_LONG;
    int len = r.putVLong(offset, l1);
    offset += len;
    len = r.putVLong(offset, l2);
    offset += len;
    len = r.putVLong(offset, Long.MAX_VALUE);
    offset += len;
    r.putVLong(offset, Long.MIN_VALUE);

    offset = 0;
    assertEquals(i1, r.getInt(offset));
    offset += Bytes.SIZEOF_INT;
    assertEquals(i2, r.getInt(offset));
    offset += Bytes.SIZEOF_INT;
    assertEquals(s1, r.getShort(offset));
    offset += Bytes.SIZEOF_SHORT;
    assertEquals(l1, r.getLong(offset));
    offset += Bytes.SIZEOF_LONG;
    assertEquals(l1, r.getVLong(offset));
    offset += SimpleByteRange.getVLongSize(l1);
    assertEquals(l2, r.getVLong(offset));
    offset += SimpleByteRange.getVLongSize(l2);
    assertEquals(Long.MAX_VALUE, r.getVLong(offset));
    offset += SimpleByteRange.getVLongSize(Long.MAX_VALUE);
    assertEquals(Long.MIN_VALUE, r.getVLong(offset));
  }
}
