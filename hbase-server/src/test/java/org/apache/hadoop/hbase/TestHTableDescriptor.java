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
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.SampleRegionWALObserver;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test setting values in the descriptor
 */
@Category(SmallTests.class)
public class TestHTableDescriptor {
  final static Log LOG = LogFactory.getLog(TestHTableDescriptor.class);

  @Test
  public void testPb() throws DeserializationException, IOException {
    HTableDescriptor htd = new HTableDescriptor(HTableDescriptor.META_TABLEDESC);
    final int v = 123;
    htd.setMaxFileSize(v);
    htd.setDeferredLogFlush(true);
    htd.setReadOnly(true);
    byte [] bytes = htd.toByteArray();
    HTableDescriptor deserializedHtd = HTableDescriptor.parseFrom(bytes);
    assertEquals(htd, deserializedHtd);
    assertEquals(v, deserializedHtd.getMaxFileSize());
    assertTrue(deserializedHtd.isReadOnly());
    assertTrue(deserializedHtd.isDeferredLogFlush());
  }

  /**
   * Test cps in the table description
   * @throws Exception
   */
  @Test
  public void testGetSetRemoveCP() throws Exception {
    HTableDescriptor desc = new HTableDescriptor("table");
    // simple CP
    String className = BaseRegionObserver.class.getName();
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
    HTableDescriptor desc = new HTableDescriptor("testGetSetRemoveCP");
    // simple CP
    String className1 = BaseRegionObserver.class.getName();
    String className2 = SampleRegionWALObserver.class.getName();
    // Check that any coprocessor is present.
    assertTrue(desc.getCoprocessors().size() == 0);

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
    assertTrue(desc.getCoprocessors().size() == 0);
    assertFalse(desc.getCoprocessors().contains(className1));
    assertFalse(desc.getCoprocessors().contains(className2));
  }

  /**
   * Test that we add and remove strings from settings properly.
   * @throws Exception
   */
  @Test
  public void testRemoveString() throws Exception {
    HTableDescriptor desc = new HTableDescriptor("table");
    String key = "Some";
    String value = "value";
    desc.setValue(key, value);
    assertEquals(value, desc.getValue(key));
    desc.remove(key);
    assertEquals(null, desc.getValue(key));
  }

  String legalTableNames[] = { "foo", "with-dash_under.dot", "_under_start_ok",  };
  String illegalTableNames[] = { ".dot_start_illegal", "-dash_start_illegal", "spaces not ok" };

  @Test
  public void testLegalHTableNames() {
    for (String tn : legalTableNames) {
      HTableDescriptor.isLegalTableName(Bytes.toBytes(tn));
    }
  }

  @Test
  public void testIllegalHTableNames() {
    for (String tn : illegalTableNames) {
      try {
        HTableDescriptor.isLegalTableName(Bytes.toBytes(tn));
        fail("invalid tablename " + tn + " should have failed");
      } catch (Exception e) {
        // expected
      }
    }
  }

  @Test
  public void testLegalHTableNamesRegex() {
    for (String tn : legalTableNames) {
      LOG.info("Testing: '" + tn + "'");
      assertTrue(Pattern.matches(HTableDescriptor.VALID_USER_TABLE_REGEX, tn));
    }
  }

  @Test
  public void testIllegalHTableNamesRegex() {
    for (String tn : illegalTableNames) {
      LOG.info("Testing: '" + tn + "'");
      assertFalse(Pattern.matches(HTableDescriptor.VALID_USER_TABLE_REGEX, tn));
    }
  }

    /**
   * Test default value handling for maxFileSize
   */
  @Test
  public void testGetMaxFileSize() {
    HTableDescriptor desc = new HTableDescriptor("table");
    assertEquals(-1, desc.getMaxFileSize());
    desc.setMaxFileSize(1111L);
    assertEquals(1111L, desc.getMaxFileSize());
  }

  /**
   * Test default value handling for memStoreFlushSize
   */
  @Test
  public void testGetMemStoreFlushSize() {
    HTableDescriptor desc = new HTableDescriptor("table");
    assertEquals(-1, desc.getMemStoreFlushSize());
    desc.setMemStoreFlushSize(1111L);
    assertEquals(1111L, desc.getMemStoreFlushSize());
  }

  /**
   * Test that we add and remove strings from configuration properly.
   */
  @Test
  public void testAddGetRemoveConfiguration() throws Exception {
    HTableDescriptor desc = new HTableDescriptor("table");
    String key = "Some";
    String value = "value";
    desc.setConfiguration(key, value);
    assertEquals(value, desc.getConfigurationValue(key));
    desc.removeConfiguration(key);
    assertEquals(null, desc.getConfigurationValue(key));
  }

  @Test
  public void testEqualsWithDifferentProperties() {
    // Test basic property difference
    HTableDescriptor h1 = new HTableDescriptor();
    h1.setName(Bytes.toBytes("n1"));

    HTableDescriptor h2 = new HTableDescriptor();
    h2.setName(Bytes.toBytes("n2"));

    assertFalse(h2.equals(h1));
    assertFalse(h1.equals(h2));

    h2.setName(Bytes.toBytes("n1"));
    assertTrue(h2.equals(h1));
    assertTrue(h1.equals(h2));
  }

  @Test
  public void testEqualsWithDifferentNumberOfItems() {
    HTableDescriptor h1 = new HTableDescriptor();
    HTableDescriptor h2 = new HTableDescriptor();

    // Test diff # of items
    h1 = new HTableDescriptor();
    h1.setName(Bytes.toBytes("n1"));

    h2 = new HTableDescriptor();
    h2.setName(Bytes.toBytes("n1"));

    HColumnDescriptor hcd1 = new HColumnDescriptor(Bytes.toBytes("someName"));
    HColumnDescriptor hcd2 = new HColumnDescriptor(Bytes.toBytes("someOtherName"));

    h1.addFamily(hcd1);
    h2.addFamily(hcd1);
    h1.addFamily(hcd2);

    assertFalse(h2.equals(h1));
    assertFalse(h1.equals(h2));

    h2.addFamily(hcd2);

    assertTrue(h2.equals(h1));
    assertTrue(h1.equals(h2));
  }

  @Test
  public void testNotEqualsWithDifferentHCDs() {
    HTableDescriptor h1 = new HTableDescriptor();
    HTableDescriptor h2 = new HTableDescriptor();

    // Test diff # of items
    h1 = new HTableDescriptor();
    h1.setName(Bytes.toBytes("n1"));

    h2 = new HTableDescriptor();
    h2.setName(Bytes.toBytes("n1"));

    HColumnDescriptor hcd1 = new HColumnDescriptor(Bytes.toBytes("someName"));
    HColumnDescriptor hcd2 = new HColumnDescriptor(Bytes.toBytes("someOtherName"));

    h1.addFamily(hcd1);
    h2.addFamily(hcd2);

    assertFalse(h2.equals(h1));
    assertFalse(h1.equals(h2));
  }

  @Test
  public void testEqualsWithDifferentHCDObjects() {
    HTableDescriptor h1 = new HTableDescriptor();
    HTableDescriptor h2 = new HTableDescriptor();

    // Test diff # of items
    h1 = new HTableDescriptor();
    h1.setName(Bytes.toBytes("n1"));

    h2 = new HTableDescriptor();
    h2.setName(Bytes.toBytes("n1"));

    HColumnDescriptor hcd1 = new HColumnDescriptor(Bytes.toBytes("someName"));
    HColumnDescriptor hcd2 = new HColumnDescriptor(Bytes.toBytes("someName"));

    h1.addFamily(hcd1);
    h2.addFamily(hcd2);

    assertTrue(h2.equals(h1));
    assertTrue(h1.equals(h2));
  }

  @Test
  public void testNotEqualsWithDifferentItems() {
    HTableDescriptor h1 = new HTableDescriptor();
    HTableDescriptor h2 = new HTableDescriptor();

    // Test diff # of items
    h1 = new HTableDescriptor();
    h1.setName(Bytes.toBytes("n1"));

    h2 = new HTableDescriptor();
    h2.setName(Bytes.toBytes("n1"));

    HColumnDescriptor hcd1 = new HColumnDescriptor(Bytes.toBytes("someName"));
    HColumnDescriptor hcd2 = new HColumnDescriptor(Bytes.toBytes("someOtherName"));
    h1.addFamily(hcd1);
    h2.addFamily(hcd2);

    assertFalse(h2.equals(h1));
    assertFalse(h1.equals(h2));
  }

  @Test
  public void testEqualsWithDifferentOrderingsOfItems() {
    HTableDescriptor h1 = new HTableDescriptor();
    HTableDescriptor h2 = new HTableDescriptor();

    //Test diff # of items
    h1 = new HTableDescriptor();
    h1.setName(Bytes.toBytes("n1"));

    h2 = new HTableDescriptor();
    h2.setName(Bytes.toBytes("n1"));

    HColumnDescriptor hcd1 = new HColumnDescriptor(Bytes.toBytes("someName"));
    HColumnDescriptor hcd2 = new HColumnDescriptor(Bytes.toBytes("someOtherName"));
    h1.addFamily(hcd1);
    h2.addFamily(hcd2);
    h1.addFamily(hcd2);
    h2.addFamily(hcd1);

    assertTrue(h2.equals(h1));
    assertTrue(h1.equals(h2));
  }

  @Test
  public void testSingleItemEquals() {
    HTableDescriptor h1 = new HTableDescriptor();
    HTableDescriptor h2 = new HTableDescriptor();

    //Test diff # of items
    h1 = new HTableDescriptor();
    h1.setName(Bytes.toBytes("n1"));

    h2 = new HTableDescriptor();
    h2.setName(Bytes.toBytes("n1"));

    HColumnDescriptor hcd1 = new HColumnDescriptor(Bytes.toBytes("someName"));
    HColumnDescriptor hcd2 = new HColumnDescriptor(Bytes.toBytes("someName"));
    h1.addFamily(hcd1);
    h2.addFamily(hcd2);

    assertTrue(h2.equals(h1));
    assertTrue(h1.equals(h2));
  }

  @Test
  public void testEmptyEquals() {
    HTableDescriptor h1 = new HTableDescriptor();
    HTableDescriptor h2 = new HTableDescriptor();

    assertTrue(h2.equals(h1));
    assertTrue(h1.equals(h2));
  }

  @Test
  public void testEqualityWithSameObject() {
    HTableDescriptor htd = new HTableDescriptor("someName");
    assertTrue(htd.equals(htd));
  }
}
