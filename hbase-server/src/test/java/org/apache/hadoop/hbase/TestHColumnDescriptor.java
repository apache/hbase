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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.experimental.categories.Category;

import org.junit.Test;

/** Tests the HColumnDescriptor with appropriate arguments */
@Category(SmallTests.class)
public class TestHColumnDescriptor {
  @Test
  public void testPb() throws DeserializationException {
    HColumnDescriptor hcd = new HColumnDescriptor(
      HTableDescriptor.META_TABLEDESC.getColumnFamilies()[0]);
    final int v = 123;
    hcd.setBlocksize(v);
    hcd.setTimeToLive(v);
    hcd.setBlockCacheEnabled(!HColumnDescriptor.DEFAULT_BLOCKCACHE);
    hcd.setValue("a", "b");
    hcd.setMaxVersions(v);
    assertEquals(v, hcd.getMaxVersions());
    hcd.setMinVersions(v);
    assertEquals(v, hcd.getMinVersions());
    hcd.setKeepDeletedCells(!HColumnDescriptor.DEFAULT_KEEP_DELETED);
    hcd.setInMemory(!HColumnDescriptor.DEFAULT_IN_MEMORY);
    boolean inmemory = hcd.isInMemory();
    hcd.setScope(v);
    hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
    hcd.setBloomFilterType(BloomType.ROW);
    hcd.setCompressionType(Algorithm.SNAPPY);


    byte [] bytes = hcd.toByteArray();
    HColumnDescriptor deserializedHcd = HColumnDescriptor.parseFrom(bytes);
    assertTrue(hcd.equals(deserializedHcd));
    assertEquals(v, hcd.getBlocksize());
    assertEquals(v, hcd.getTimeToLive());
    assertEquals(hcd.getValue("a"), deserializedHcd.getValue("a"));
    assertEquals(hcd.getMaxVersions(), deserializedHcd.getMaxVersions());
    assertEquals(hcd.getMinVersions(), deserializedHcd.getMinVersions());
    assertEquals(hcd.getKeepDeletedCells(), deserializedHcd.getKeepDeletedCells());
    assertEquals(inmemory, deserializedHcd.isInMemory());
    assertEquals(hcd.getScope(), deserializedHcd.getScope());
    assertTrue(deserializedHcd.getCompressionType().equals(Compression.Algorithm.SNAPPY));
    assertTrue(deserializedHcd.getDataBlockEncoding().equals(DataBlockEncoding.FAST_DIFF));
    assertTrue(deserializedHcd.getBloomFilterType().equals(BloomType.ROW));
  }

  @Test
  /** Tests HColumnDescriptor with empty familyName*/
  public void testHColumnDescriptorShouldThrowIAEWhenFamiliyNameEmpty()
      throws Exception {
    try {
      new HColumnDescriptor("".getBytes());
    } catch (IllegalArgumentException e) {
      assertEquals("Family name can not be empty", e.getLocalizedMessage());
    }
  }

  /**
   * Test that we add and remove strings from configuration properly.
   */
  @Test
  public void testAddGetRemoveConfiguration() throws Exception {
    HColumnDescriptor desc = new HColumnDescriptor("foo");
    String key = "Some";
    String value = "value";
    desc.setConfiguration(key, value);
    assertEquals(value, desc.getConfigurationValue(key));
    desc.removeConfiguration(key);
    assertEquals(null, desc.getConfigurationValue(key));
  }

  @Test
  public void testEqualsWithFamilyName() {
    final String name1 = "someFamilyName";
    HColumnDescriptor hcd1 = new HColumnDescriptor(name1);
    HColumnDescriptor hcd2 = new HColumnDescriptor("someOtherFamilyName");
    HColumnDescriptor hcd3 = new HColumnDescriptor(name1);

    assertFalse(hcd1.equals(hcd2));
    assertFalse(hcd2.equals(hcd1));

    assertTrue(hcd3.equals(hcd1));
    assertTrue(hcd1.equals(hcd3));
  }

  @Test
  public void testEqualsWithAdditionalProperties() {
    final String name1 = "someFamilyName";
    HColumnDescriptor hcd1 = new HColumnDescriptor(name1);
    HColumnDescriptor hcd2 = new HColumnDescriptor(name1);
    hcd2.setBlocksize(4);

    assertFalse(hcd1.equals(hcd2));
    assertFalse(hcd2.equals(hcd1));

    hcd1.setBlocksize(4);

    assertTrue(hcd2.equals(hcd1));
    assertTrue(hcd1.equals(hcd2));
  }

  @Test
  public void testEqualsWithDifferentNumberOfProperties() {
    final String name1 = "someFamilyName";
    HColumnDescriptor hcd1 = new HColumnDescriptor(name1);
    HColumnDescriptor hcd2 = new HColumnDescriptor(name1);
    hcd2.setBlocksize(4);
    hcd1.setBlocksize(4);

    assertTrue(hcd2.equals(hcd1));
    assertTrue(hcd1.equals(hcd2));

    hcd2.setBloomFilterType(BloomType.ROW);

    assertFalse(hcd1.equals(hcd2));
    assertFalse(hcd2.equals(hcd1));
  }

  @Test
  public void testEqualsWithDifferentOrderingOfProperties() {
    final String name1 = "someFamilyName";
    HColumnDescriptor hcd1 = new HColumnDescriptor(name1);
    HColumnDescriptor hcd2 = new HColumnDescriptor(name1);
    hcd2.setBlocksize(4);
    hcd2.setBloomFilterType(BloomType.ROW);
    hcd1.setBloomFilterType(BloomType.ROW);
    hcd1.setBlocksize(4);

    assertTrue(hcd2.equals(hcd1));
    assertTrue(hcd1.equals(hcd2));
  }

  @Test
  public void testEqualityWithSameObject() {
    HColumnDescriptor hcd1 = new HColumnDescriptor("someName");
    assertTrue(hcd1.equals(hcd1));
  }
}
