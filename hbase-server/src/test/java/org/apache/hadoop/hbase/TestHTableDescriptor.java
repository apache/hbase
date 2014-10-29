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
import org.apache.hadoop.hbase.client.Durability;
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
    HTableDescriptor htd = new HTableDescriptor(TableName.META_TABLE_NAME);
    final int v = 123;
    htd.setMaxFileSize(v);
    htd.setDurability(Durability.ASYNC_WAL);
    htd.setReadOnly(true);
    byte [] bytes = htd.toByteArray();
    HTableDescriptor deserializedHtd = HTableDescriptor.parseFrom(bytes);
    assertEquals(htd, deserializedHtd);
    assertEquals(v, deserializedHtd.getMaxFileSize());
    assertTrue(deserializedHtd.isReadOnly());
    assertEquals(Durability.ASYNC_WAL, deserializedHtd.getDurability());
  }

  /**
   * Test cps in the table description
   * @throws Exception
   */
  @Test
  public void testGetSetRemoveCP() throws Exception {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("table"));
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
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("testGetSetRemoveCP"));
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
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("table"));
    String key = "Some";
    String value = "value";
    desc.setValue(key, value);
    assertEquals(value, desc.getValue(key));
    desc.remove(key);
    assertEquals(null, desc.getValue(key));
  }

  String legalTableNames[] = { "foo", "with-dash_under.dot", "_under_start_ok",
      "with-dash.with_underscore", "02-01-2012.my_table_01-02", "xyz._mytable_", "9_9_0.table_02"
      , "dot1.dot2.table", "new.-mytable", "with-dash.with.dot", "legal..t2", "legal..legal.t2",
      "trailingdots..", "trailing.dots...", "ns:mytable", "ns:_mytable_", "ns:my_table_01-02"};
  String illegalTableNames[] = { ".dot_start_illegal", "-dash_start_illegal", "spaces not ok",
      "-dash-.start_illegal", "new.table with space", "01 .table", "ns:-illegaldash",
      "new:.illegaldot", "new:illegalcolon1:", "new:illegalcolon1:2"};

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
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("table"));
    assertEquals(-1, desc.getMaxFileSize());
    desc.setMaxFileSize(1111L);
    assertEquals(1111L, desc.getMaxFileSize());
  }

  /**
   * Test default value handling for memStoreFlushSize
   */
  @Test
  public void testGetMemStoreFlushSize() {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("table"));
    assertEquals(-1, desc.getMemStoreFlushSize());
    desc.setMemStoreFlushSize(1111L);
    assertEquals(1111L, desc.getMemStoreFlushSize());
  }

  /**
   * Test that we add and remove strings from configuration properly.
   */
  @Test
  public void testAddGetRemoveConfiguration() throws Exception {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("table"));
    String key = "Some";
    String value = "value";
    desc.setConfiguration(key, value);
    assertEquals(value, desc.getConfigurationValue(key));
    desc.removeConfiguration(key);
    assertEquals(null, desc.getConfigurationValue(key));
  }
}
