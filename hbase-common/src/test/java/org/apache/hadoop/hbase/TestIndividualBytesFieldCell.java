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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;

import org.apache.hadoop.hbase.util.Bytes;
import static org.apache.hadoop.hbase.KeyValue.Type;

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

@Category({MiscTests.class, SmallTests.class})
public class TestIndividualBytesFieldCell {
  private static IndividualBytesFieldCell ic0  = null;
  private static KeyValue                 kv0 = null;

  @BeforeClass
  public static void testConstructorAndVerify() {
    // Immutable inputs
    byte[] row       = Bytes.toBytes("immutable-row");
    byte[] family    = Bytes.toBytes("immutable-family");
    byte[] qualifier = Bytes.toBytes("immutable-qualifier");
    byte[] value     = Bytes.toBytes("immutable-value");
    byte[] tags      = Bytes.toBytes("immutable-tags");

    // Other inputs
    long timestamp = 5000L;
    long seqId     = 0L;
    Type type      = KeyValue.Type.Put;

    ic0 = new IndividualBytesFieldCell(row, family, qualifier, timestamp, type, seqId, value, tags);
    kv0 = new KeyValue(row, family, qualifier, timestamp, type, value, tags);

    // Verify if no local copy is made for row, family, qualifier, value or tags.
    assertTrue(ic0.getRowArray()       == row);
    assertTrue(ic0.getFamilyArray()    == family);
    assertTrue(ic0.getQualifierArray() == qualifier);
    assertTrue(ic0.getValueArray()     == value);
    assertTrue(ic0.getTagsArray()      == tags);

    // Verify others.
    assertEquals(timestamp     , ic0.getTimestamp());
    assertEquals(seqId         , ic0.getSequenceId());
    assertEquals(type.getCode(), ic0.getTypeByte());

    // Verify offsets of backing byte arrays are always 0.
    assertEquals(0, ic0.getRowOffset());
    assertEquals(0, ic0.getFamilyOffset());
    assertEquals(0, ic0.getQualifierOffset());
    assertEquals(0, ic0.getValueOffset());
    assertEquals(0, ic0.getTagsOffset());
  }

  // Verify clone() and deepClone()
  @Test
  public void testClone() throws CloneNotSupportedException {
    // Verify clone. Only shadow copies are made for backing byte arrays.
    IndividualBytesFieldCell cloned = (IndividualBytesFieldCell) ic0.clone();
    assertTrue(cloned.getRowArray()       == ic0.getRowArray());
    assertTrue(cloned.getFamilyArray()    == ic0.getFamilyArray());
    assertTrue(cloned.getQualifierArray() == ic0.getQualifierArray());
    assertTrue(cloned.getValueArray()     == ic0.getValueArray());
    assertTrue(cloned.getTagsArray()      == ic0.getTagsArray());

    // Verify if deep clone returns a KeyValue object
    assertTrue(ic0.deepClone() instanceof KeyValue);
  }

  /**
   * Verify KeyValue format related functions: write() and getSerializedSize().
   * Should have the same behaviors as {@link KeyValue}.
   */
  @Test
  public void testWriteIntoKeyValueFormat() throws IOException {
    // Verify getSerializedSize().
    assertEquals(kv0.getSerializedSize(true),  ic0.getSerializedSize(true));   // with tags
    assertEquals(kv0.getSerializedSize(false), ic0.getSerializedSize(false));  // without tags

    // Verify writing into ByteBuffer.
    ByteBuffer bbufIC = ByteBuffer.allocate(ic0.getSerializedSize(true));
    ic0.write(bbufIC, 0);

    ByteBuffer bbufKV = ByteBuffer.allocate(kv0.getSerializedSize(true));
    kv0.write(bbufKV, 0);

    assertTrue(bbufIC.equals(bbufKV));

    // Verify writing into OutputStream.
    testWriteIntoOutputStream(ic0, kv0, true);   // with tags
    testWriteIntoOutputStream(ic0, kv0, false);  // without tags
  }

  /**
   * @param ic An instance of IndividualBytesFieldCell to compare.
   * @param kv An instance of KeyValue to compare.
   * @param withTags Whether to write tags.
   * @throws IOException
   */
  private void testWriteIntoOutputStream(IndividualBytesFieldCell ic, KeyValue kv, boolean withTags)
          throws IOException {
    ByteArrayOutputStream outIC = new ByteArrayOutputStream(ic.getSerializedSize(withTags));
    ByteArrayOutputStream outKV = new ByteArrayOutputStream(kv.getSerializedSize(withTags));

    assertEquals(kv.write(outKV, withTags), ic.write(outIC, withTags));  // compare the number of bytes written
    assertArrayEquals(outKV.getBuffer(), outIC.getBuffer());             // compare the underlying byte array
  }

  /**
   * Verify getXXXArray() and getXXXLength() when family/qualifier/value/tags are null.
   * Should have the same behaviors as {@link KeyValue}.
   */
  @Test
  public void testNullFamilyQualifierValueTags() {
    byte[] row = Bytes.toBytes("row1");

    long timestamp = 5000L;
    long seqId     = 0L;
    Type type      = KeyValue.Type.Put;

    // Test when following fields are null.
    byte[] family    = null;
    byte[] qualifier = null;
    byte[] value     = null;
    byte[] tags      = null;

    Cell ic1 = new IndividualBytesFieldCell(row, family, qualifier, timestamp, type, seqId, value, tags);

    Cell kv1 = new KeyValue(row, family, qualifier, timestamp, type, value, tags);
    byte[] familyArrayInKV    = Bytes.copy(kv1.getFamilyArray()   , kv1.getFamilyOffset()   , kv1.getFamilyLength());
    byte[] qualifierArrayInKV = Bytes.copy(kv1.getQualifierArray(), kv1.getQualifierOffset(), kv1.getQualifierLength());
    byte[] valueArrayInKV     = Bytes.copy(kv1.getValueArray()    , kv1.getValueOffset()    , kv1.getValueLength());
    byte[] tagsArrayInKV      = Bytes.copy(kv1.getTagsArray()     , kv1.getTagsOffset()     , kv1.getTagsLength());

    // getXXXArray() for family, qualifier, value and tags are supposed to return empty byte array, rather than null.
    assertArrayEquals(familyArrayInKV   , ic1.getFamilyArray());
    assertArrayEquals(qualifierArrayInKV, ic1.getQualifierArray());
    assertArrayEquals(valueArrayInKV    , ic1.getValueArray());
    assertArrayEquals(tagsArrayInKV     , ic1.getTagsArray());

    // getXXXLength() for family, qualifier, value and tags are supposed to return 0.
    assertEquals(kv1.getFamilyLength()   , ic1.getFamilyLength());
    assertEquals(kv1.getQualifierLength(), ic1.getQualifierLength());
    assertEquals(kv1.getValueLength()    , ic1.getValueLength());
    assertEquals(kv1.getTagsLength()     , ic1.getTagsLength());
  }

  // Verify if SettableSequenceId interface is implemented
  @Test
  public void testIfSettableSequenceIdImplemented() {
    assertTrue(ic0 instanceof SettableSequenceId);
  }

  // Verify if SettableTimestamp interface is implemented
  @Test
  public void testIfSettableTimestampImplemented() {
    assertTrue(ic0 instanceof SettableTimestamp);
  }
}
