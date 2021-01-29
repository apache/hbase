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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.BuilderStyleTest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PrettyPrinter;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import java.util.Map;

@Category({MiscTests.class, SmallTests.class})
public class TestColumnFamilyDescriptorBuilder {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestColumnFamilyDescriptorBuilder.class);

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testBuilder() throws DeserializationException {
    ColumnFamilyDescriptorBuilder builder
      = ColumnFamilyDescriptorBuilder.newBuilder(HConstants.CATALOG_FAMILY)
            .setInMemory(true)
            .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
            .setBloomFilterType(BloomType.NONE);
    final int v = 123;
    builder.setBlocksize(v);
    builder.setTimeToLive(v);
    builder.setBlockCacheEnabled(!HColumnDescriptor.DEFAULT_BLOCKCACHE);
    builder.setValue(Bytes.toBytes("a"), Bytes.toBytes("b"));
    builder.setMaxVersions(v);
    assertEquals(v, builder.build().getMaxVersions());
    builder.setMinVersions(v);
    assertEquals(v, builder.build().getMinVersions());
    builder.setKeepDeletedCells(KeepDeletedCells.TRUE);
    builder.setInMemory(!HColumnDescriptor.DEFAULT_IN_MEMORY);
    boolean inmemory = builder.build().isInMemory();
    builder.setScope(v);
    builder.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
    builder.setBloomFilterType(BloomType.ROW);
    builder.setCompressionType(Algorithm.SNAPPY);
    builder.setMobEnabled(true);
    builder.setMobThreshold(1000L);
    builder.setDFSReplication((short) v);

    ColumnFamilyDescriptor hcd = builder.build();
    byte [] bytes = ColumnFamilyDescriptorBuilder.toByteArray(hcd);
    ColumnFamilyDescriptor deserializedHcd = ColumnFamilyDescriptorBuilder.parseFrom(bytes);
    assertTrue(hcd.equals(deserializedHcd));
    assertEquals(v, hcd.getBlocksize());
    assertEquals(v, hcd.getTimeToLive());
    assertTrue(Bytes.equals(hcd.getValue(Bytes.toBytes("a")),
        deserializedHcd.getValue(Bytes.toBytes("a"))));
    assertEquals(hcd.getMaxVersions(), deserializedHcd.getMaxVersions());
    assertEquals(hcd.getMinVersions(), deserializedHcd.getMinVersions());
    assertEquals(hcd.getKeepDeletedCells(), deserializedHcd.getKeepDeletedCells());
    assertEquals(inmemory, deserializedHcd.isInMemory());
    assertEquals(hcd.getScope(), deserializedHcd.getScope());
    assertTrue(deserializedHcd.getCompressionType().equals(Compression.Algorithm.SNAPPY));
    assertTrue(deserializedHcd.getDataBlockEncoding().equals(DataBlockEncoding.FAST_DIFF));
    assertTrue(deserializedHcd.getBloomFilterType().equals(BloomType.ROW));
    assertEquals(hcd.isMobEnabled(), deserializedHcd.isMobEnabled());
    assertEquals(hcd.getMobThreshold(), deserializedHcd.getMobThreshold());
    assertEquals(v, deserializedHcd.getDFSReplication());
  }

  /**
   * Tests HColumnDescriptor with empty familyName
   */
  @Test
  public void testHColumnDescriptorShouldThrowIAEWhenFamilyNameEmpty() throws Exception {
    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage("Column Family name can not be empty");
    ColumnFamilyDescriptorBuilder.of("");
  }

  /**
   * Test that we add and remove strings from configuration properly.
   */
  @Test
  public void testAddGetRemoveConfiguration() {
    ColumnFamilyDescriptorBuilder builder
      = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("foo"));
    String key = "Some";
    String value = "value";
    builder.setConfiguration(key, value);
    assertEquals(value, builder.build().getConfigurationValue(key));
    builder.removeConfiguration(key);
    assertEquals(null, builder.build().getConfigurationValue(key));
  }

  @Test
  public void testMobValuesInHColumnDescriptorShouldReadable() {
    boolean isMob = true;
    long threshold = 1000;
    String policy = "weekly";
    // We unify the format of all values saved in the descriptor.
    // Each value is stored as bytes of string.
    String isMobString = PrettyPrinter.format(String.valueOf(isMob),
            HColumnDescriptor.getUnit(HColumnDescriptor.IS_MOB));
    String thresholdString = PrettyPrinter.format(String.valueOf(threshold),
            HColumnDescriptor.getUnit(HColumnDescriptor.MOB_THRESHOLD));
    String policyString = PrettyPrinter.format(Bytes.toStringBinary(Bytes.toBytes(policy)),
        HColumnDescriptor.getUnit(HColumnDescriptor.MOB_COMPACT_PARTITION_POLICY));
    assertEquals(String.valueOf(isMob), isMobString);
    assertEquals(String.valueOf(threshold), thresholdString);
    assertEquals(String.valueOf(policy), policyString);
  }

  @Test
  public void testClassMethodsAreBuilderStyle() {
    /* HColumnDescriptor should have a builder style setup where setXXX/addXXX methods
     * can be chainable together:
     * . For example:
     * HColumnDescriptor hcd
     *   = new HColumnDescriptor()
     *     .setFoo(foo)
     *     .setBar(bar)
     *     .setBuz(buz)
     *
     * This test ensures that all methods starting with "set" returns the declaring object
     */

    BuilderStyleTest.assertClassesAreBuilderStyle(ColumnFamilyDescriptorBuilder.class);
  }

  @Test
  public void testSetTimeToLive() throws HBaseException {
    String ttl;
    ColumnFamilyDescriptorBuilder builder
      = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("foo"));

    ttl = "50000";
    builder.setTimeToLive(ttl);
    Assert.assertEquals(50000, builder.build().getTimeToLive());

    ttl = "50000 seconds";
    builder.setTimeToLive(ttl);
    Assert.assertEquals(50000, builder.build().getTimeToLive());

    ttl = "";
    builder.setTimeToLive(ttl);
    Assert.assertEquals(0, builder.build().getTimeToLive());

    ttl = "FOREVER";
    builder.setTimeToLive(ttl);
    Assert.assertEquals(HConstants.FOREVER, builder.build().getTimeToLive());

    ttl = "1 HOUR 10 minutes 1 second";
    builder.setTimeToLive(ttl);
    Assert.assertEquals(4201, builder.build().getTimeToLive());

    ttl = "500 Days 23 HOURS";
    builder.setTimeToLive(ttl);
    Assert.assertEquals(43282800, builder.build().getTimeToLive());

    ttl = "43282800 SECONDS (500 Days 23 hours)";
    builder.setTimeToLive(ttl);
    Assert.assertEquals(43282800, builder.build().getTimeToLive());
  }

  /**
   * Test for verifying the ColumnFamilyDescriptorBuilder's default values so that backward
   * compatibility with hbase-1.x can be mantained (see HBASE-24981).
   */
  @Test
  public void testDefaultBuilder() {
    final Map<String, String> defaultValueMap = ColumnFamilyDescriptorBuilder.getDefaultValues();
    assertEquals(defaultValueMap.size(), 11);
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.BLOOMFILTER),
      BloomType.ROW.toString());
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.REPLICATION_SCOPE), "0");
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.MAX_VERSIONS), "1");
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.MIN_VERSIONS), "0");
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.COMPRESSION),
      Compression.Algorithm.NONE.toString());
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.TTL),
      Integer.toString(Integer.MAX_VALUE));
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.BLOCKSIZE),
      Integer.toString(64 * 1024));
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.IN_MEMORY),
      Boolean.toString(false));
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.BLOCKCACHE),
      Boolean.toString(true));
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS),
      KeepDeletedCells.FALSE.toString());
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.DATA_BLOCK_ENCODING),
      DataBlockEncoding.NONE.toString());
  }

  @Test
  public void testSetEmptyValue() {
    ColumnFamilyDescriptorBuilder builder =
      ColumnFamilyDescriptorBuilder.newBuilder(HConstants.CATALOG_FAMILY);
    String testConf = "TestConfiguration";
    String testValue = "TestValue";
    // test set value
    builder.setValue(testValue, "2");
    assertEquals("2", Bytes.toString(builder.build().getValue(Bytes.toBytes(testValue))));
    builder.setValue(testValue, "");
    assertNull(builder.build().getValue(Bytes.toBytes(testValue)));

    // test set configuration
    builder.setConfiguration(testConf, "1");
    assertEquals("1", builder.build().getConfigurationValue(testConf));
    builder.setConfiguration(testConf, "");
    assertNull(builder.build().getConfigurationValue(testConf));
  }
}
