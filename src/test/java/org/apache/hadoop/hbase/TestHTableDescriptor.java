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
 
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.SampleRegionWALObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test setting values in the descriptor
 */
@Category(SmallTests.class)
public class TestHTableDescriptor {
  final static Log LOG = LogFactory.getLog(TestHTableDescriptor.class);

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
}
