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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.BuilderStyleTest;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test setting values in the descriptor
 */
@Category({MiscTests.class, SmallTests.class})
@Deprecated
public class TestHTableDescriptor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHTableDescriptor.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHTableDescriptor.class);

  @Rule
  public TestName name = new TestName();

  @Test (expected=IOException.class)
  public void testAddCoprocessorTwice() throws IOException {
    HTableDescriptor htd = new HTableDescriptor(TableName.META_TABLE_NAME);
    String cpName = "a.b.c.d";
    htd.addCoprocessor(cpName);
    htd.addCoprocessor(cpName);
  }

  @Test
  public void testAddCoprocessorWithSpecStr() throws IOException {
    HTableDescriptor htd = new HTableDescriptor(TableName.META_TABLE_NAME);
    String cpName = "a.b.c.d";
    try {
      htd.addCoprocessorWithSpec(cpName);
      fail();
    } catch (IllegalArgumentException iae) {
      // Expected as cpName is invalid
    }

    // Try minimal spec.
    try {
      htd.addCoprocessorWithSpec("file:///some/path" + "|" + cpName);
      fail();
    } catch (IllegalArgumentException iae) {
      // Expected to be invalid
    }

    // Try more spec.
    String spec = "hdfs:///foo.jar|com.foo.FooRegionObserver|1001|arg1=1,arg2=2";
    try {
      htd.addCoprocessorWithSpec(spec);
    } catch (IllegalArgumentException iae) {
      fail();
    }

    // Try double add of same coprocessor
    try {
      htd.addCoprocessorWithSpec(spec);
      fail();
    } catch (IOException ioe) {
      // Expect that the coprocessor already exists
    }
  }

  @Test
  public void testPb() throws DeserializationException, IOException {
    HTableDescriptor htd = new HTableDescriptor(TableName.META_TABLE_NAME);
    final int v = 123;
    htd.setMaxFileSize(v);
    htd.setDurability(Durability.ASYNC_WAL);
    htd.setReadOnly(true);
    htd.setRegionReplication(2);
    byte [] bytes = htd.toByteArray();
    HTableDescriptor deserializedHtd = HTableDescriptor.parseFrom(bytes);
    assertEquals(htd, deserializedHtd);
    assertEquals(v, deserializedHtd.getMaxFileSize());
    assertTrue(deserializedHtd.isReadOnly());
    assertEquals(Durability.ASYNC_WAL, deserializedHtd.getDurability());
    assertEquals(2, deserializedHtd.getRegionReplication());
  }

  /**
   * Test cps in the table description
   * @throws Exception
   */
  @Test
  public void testGetSetRemoveCP() throws Exception {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    // simple CP
    String className = "org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver";
    // add and check that it is present
    desc.addCoprocessor(className);
    assertTrue(desc.hasCoprocessor(className));
    // remove it and check that it is gone
    desc.removeCoprocessor(className);
    assertFalse(desc.hasCoprocessor(className));
  }

  /**
   * Test cps in the table description
   * @throws Exception
   */
  @Test
  public void testSetListRemoveCP() throws Exception {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    // simple CP
    String className1 = "org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver";
    String className2 = "org.apache.hadoop.hbase.coprocessor.SampleRegionWALObserver";
    // Check that any coprocessor is present.
    assertTrue(desc.getCoprocessors().isEmpty());

    // Add the 1 coprocessor and check if present.
    desc.addCoprocessor(className1);
    assertTrue(desc.getCoprocessors().size() == 1);
    assertTrue(desc.getCoprocessors().contains(className1));

    // Add the 2nd coprocessor and check if present.
    // remove it and check that it is gone
    desc.addCoprocessor(className2);
    assertTrue(desc.getCoprocessors().size() == 2);
    assertTrue(desc.getCoprocessors().contains(className2));

    // Remove one and check
    desc.removeCoprocessor(className1);
    assertTrue(desc.getCoprocessors().size() == 1);
    assertFalse(desc.getCoprocessors().contains(className1));
    assertTrue(desc.getCoprocessors().contains(className2));

    // Remove the last and check
    desc.removeCoprocessor(className2);
    assertTrue(desc.getCoprocessors().isEmpty());
    assertFalse(desc.getCoprocessors().contains(className1));
    assertFalse(desc.getCoprocessors().contains(className2));
  }

  /**
   * Test that we add and remove strings from settings properly.
   * @throws Exception
   */
  @Test
  public void testAddGetRemoveString() throws Exception {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    String key = "Some";
    String value = "value";
    desc.setValue(key, value);
    assertEquals(value, desc.getValue(key));
    desc.remove(key);
    assertEquals(null, desc.getValue(key));
    String keyShouldNotNull = "Some2";
    String valueIsNull = null;
    desc.setValue(keyShouldNotNull, valueIsNull);
    assertEquals(valueIsNull, desc.getValue(keyShouldNotNull));
    desc.remove(keyShouldNotNull);
    assertEquals(null, desc.getValue(keyShouldNotNull));
  }

  String legalTableNames[] = { "foo", "with-dash_under.dot", "_under_start_ok",
      "with-dash.with_underscore", "02-01-2012.my_table_01-02", "xyz._mytable_", "9_9_0.table_02"
      , "dot1.dot2.table", "new.-mytable", "with-dash.with.dot", "legal..t2", "legal..legal.t2",
      "trailingdots..", "trailing.dots...", "ns:mytable", "ns:_mytable_", "ns:my_table_01-02",
      "汉", "汉:字", "_字_", "foo:字", "foo.字", "字.foo"};
  // Avoiding "zookeeper" in here as it's tough to encode in regex
  String illegalTableNames[] = { ".dot_start_illegal", "-dash_start_illegal", "spaces not ok",
      "-dash-.start_illegal", "new.table with space", "01 .table", "ns:-illegaldash",
      "new:.illegaldot", "new:illegalcolon1:", "new:illegalcolon1:2", String.valueOf((char)130),
      String.valueOf((char)5), String.valueOf((char)65530)};

  @Test
  public void testLegalHTableNames() {
    for (String tn : legalTableNames) {
      TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(tn));
    }
  }

  @Test
  public void testIllegalHTableNames() {
    for (String tn : illegalTableNames) {
      try {
        TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(tn));
        fail("invalid tablename " + tn + " should have failed");
      } catch (Exception e) {
        // expected
      }
    }
  }

  @Test
  public void testIllegalZooKeeperName() {
    for (String name : Arrays.asList("zookeeper", "ns:zookeeper", "zookeeper:table")) {
      try {
        TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(name));
        fail("invalid tablename " + name + " should have failed");
      } catch (Exception e) {
        // expected
      }
    }
  }

  @Test
  public void testLegalHTableNamesRegex() {
    for (String tn : legalTableNames) {
      TableName tName = TableName.valueOf(tn);
      assertTrue("Testing: '" + tn + "'", Pattern.matches(TableName.VALID_USER_TABLE_REGEX,
          tName.getNameAsString()));
    }
  }

  @Test
  public void testIllegalHTableNamesRegex() {
    for (String tn : illegalTableNames) {
      LOG.info("Testing: '" + tn + "'");
      assertFalse(Pattern.matches(TableName.VALID_USER_TABLE_REGEX, tn));
    }
  }

    /**
   * Test default value handling for maxFileSize
   */
  @Test
  public void testGetMaxFileSize() {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    assertEquals(-1, desc.getMaxFileSize());
    desc.setMaxFileSize(1111L);
    assertEquals(1111L, desc.getMaxFileSize());
  }

  /**
   * Test default value handling for memStoreFlushSize
   */
  @Test
  public void testGetMemStoreFlushSize() {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    assertEquals(-1, desc.getMemStoreFlushSize());
    desc.setMemStoreFlushSize(1111L);
    assertEquals(1111L, desc.getMemStoreFlushSize());
  }

  /**
   * Test that we add and remove strings from configuration properly.
   */
  @Test
  public void testAddGetRemoveConfiguration() throws Exception {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    String key = "Some";
    String value = "value";
    desc.setConfiguration(key, value);
    assertEquals(value, desc.getConfigurationValue(key));
    desc.removeConfiguration(key);
    assertEquals(null, desc.getConfigurationValue(key));
  }

  @Test
  public void testClassMethodsAreBuilderStyle() {
    /* HTableDescriptor should have a builder style setup where setXXX/addXXX methods
     * can be chainable together:
     * . For example:
     * HTableDescriptor htd
     *   = new HTableDescriptor()
     *     .setFoo(foo)
     *     .setBar(bar)
     *     .setBuz(buz)
     *
     * This test ensures that all methods starting with "set" returns the declaring object
     */

    BuilderStyleTest.assertClassesAreBuilderStyle(HTableDescriptor.class);
  }

  @Test
  public void testModifyFamily() {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    byte[] familyName = Bytes.toBytes("cf");
    HColumnDescriptor hcd = new HColumnDescriptor(familyName);
    hcd.setBlocksize(1000);
    hcd.setDFSReplication((short) 3);
    htd.addFamily(hcd);
    assertEquals(1000, htd.getFamily(familyName).getBlocksize());
    assertEquals(3, htd.getFamily(familyName).getDFSReplication());
    hcd = new HColumnDescriptor(familyName);
    hcd.setBlocksize(2000);
    hcd.setDFSReplication((short) 1);
    htd.modifyFamily(hcd);
    assertEquals(2000, htd.getFamily(familyName).getBlocksize());
    assertEquals(1, htd.getFamily(familyName).getDFSReplication());
  }

  @Test(expected=IllegalArgumentException.class)
  public void testModifyInexistentFamily() {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    byte[] familyName = Bytes.toBytes("cf");
    HColumnDescriptor hcd = new HColumnDescriptor(familyName);
    htd.modifyFamily(hcd);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testAddDuplicateFamilies() {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    byte[] familyName = Bytes.toBytes("cf");
    HColumnDescriptor hcd = new HColumnDescriptor(familyName);
    hcd.setBlocksize(1000);
    htd.addFamily(hcd);
    assertEquals(1000, htd.getFamily(familyName).getBlocksize());
    hcd = new HColumnDescriptor(familyName);
    hcd.setBlocksize(2000);
    htd.addFamily(hcd);
  }

  @Test
  public void testPriority() {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    htd.setPriority(42);
    assertEquals(42, htd.getPriority());
  }
}
