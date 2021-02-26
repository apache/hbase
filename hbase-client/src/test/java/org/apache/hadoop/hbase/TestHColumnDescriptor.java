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
import static org.junit.Assert.assertTrue;

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

/**
 * Tests the HColumnDescriptor with appropriate arguments.
 *
 * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0 together with
 *            {@link HColumnDescriptor}.
 */
@Category({MiscTests.class, SmallTests.class})
@Deprecated
public class TestHColumnDescriptor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHColumnDescriptor.class);

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();
  @Test
  public void testPb() throws DeserializationException {
    HColumnDescriptor hcd = new HColumnDescriptor(
        new HColumnDescriptor(HConstants.CATALOG_FAMILY)
            .setInMemory(true)
            .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
            .setBloomFilterType(BloomType.NONE)
            .setCacheDataInL1(true));
    final int v = 123;
    hcd.setBlocksize(v);
    hcd.setTimeToLive(v);
    hcd.setBlockCacheEnabled(!HColumnDescriptor.DEFAULT_BLOCKCACHE);
    hcd.setValue("a", "b");
    hcd.setMaxVersions(v);
    assertEquals(v, hcd.getMaxVersions());
    hcd.setMinVersions(v);
    assertEquals(v, hcd.getMinVersions());
    hcd.setKeepDeletedCells(KeepDeletedCells.TRUE);
    hcd.setInMemory(!HColumnDescriptor.DEFAULT_IN_MEMORY);
    boolean inmemory = hcd.isInMemory();
    hcd.setScope(v);
    hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
    hcd.setBloomFilterType(BloomType.ROW);
    hcd.setCompressionType(Algorithm.SNAPPY);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(1000L);
    hcd.setDFSReplication((short) v);

    byte [] bytes = hcd.toByteArray();
    HColumnDescriptor deserializedHcd = HColumnDescriptor.parseFrom(bytes);
    assertTrue(hcd.equals(deserializedHcd));
    assertEquals(v, hcd.getBlocksize());
    assertEquals(v, hcd.getTimeToLive());
    assertEquals(v, hcd.getScope());
    assertEquals(hcd.getValue("a"), deserializedHcd.getValue("a"));
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
    new HColumnDescriptor(Bytes.toBytes(""));
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

    BuilderStyleTest.assertClassesAreBuilderStyle(HColumnDescriptor.class);
  }

  @Test
  public void testSetTimeToLive() throws HBaseException {
    String ttl;
    HColumnDescriptor desc = new HColumnDescriptor("foo");

    ttl = "50000";
    desc.setTimeToLive(ttl);
    Assert.assertEquals(50000, desc.getTimeToLive());

    ttl = "50000 seconds";
    desc.setTimeToLive(ttl);
    Assert.assertEquals(50000, desc.getTimeToLive());

    ttl = "";
    desc.setTimeToLive(ttl);
    Assert.assertEquals(0, desc.getTimeToLive());

    ttl = "FOREVER";
    desc.setTimeToLive(ttl);
    Assert.assertEquals(HConstants.FOREVER, desc.getTimeToLive());

    ttl = "1 HOUR 10 minutes 1 second";
    desc.setTimeToLive(ttl);
    Assert.assertEquals(4201, desc.getTimeToLive());

    ttl = "500 Days 23 HOURS";
    desc.setTimeToLive(ttl);
    Assert.assertEquals(43282800, desc.getTimeToLive());

    ttl = "43282800 SECONDS (500 Days 23 hours)";
    desc.setTimeToLive(ttl);
    Assert.assertEquals(43282800, desc.getTimeToLive());
  }
}
