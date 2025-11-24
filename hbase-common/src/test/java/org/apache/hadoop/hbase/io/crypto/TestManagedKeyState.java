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
package org.apache.hadoop.hbase.io.crypto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for ManagedKeyState enum and its utility methods.
 */
@Category({ MiscTests.class, SmallTests.class })
public class TestManagedKeyState {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestManagedKeyState.class);

  @Test
  public void testGetVal() {
    assertEquals((byte) 1, ManagedKeyState.ACTIVE.getVal());
    assertEquals((byte) 2, ManagedKeyState.FAILED.getVal());
    assertEquals((byte) 3, ManagedKeyState.DISABLED.getVal());
    assertEquals((byte) 4, ManagedKeyState.INACTIVE.getVal());
    assertEquals((byte) 5, ManagedKeyState.ACTIVE_DISABLED.getVal());
    assertEquals((byte) 6, ManagedKeyState.INACTIVE_DISABLED.getVal());
  }

  @Test
  public void testForValue() {
    assertEquals(ManagedKeyState.ACTIVE, ManagedKeyState.forValue((byte) 1));
    assertEquals(ManagedKeyState.FAILED, ManagedKeyState.forValue((byte) 2));
    assertEquals(ManagedKeyState.DISABLED, ManagedKeyState.forValue((byte) 3));
    assertEquals(ManagedKeyState.INACTIVE, ManagedKeyState.forValue((byte) 4));
    assertEquals(ManagedKeyState.ACTIVE_DISABLED, ManagedKeyState.forValue((byte) 5));
    assertEquals(ManagedKeyState.INACTIVE_DISABLED, ManagedKeyState.forValue((byte) 6));
  }

  @Test
  public void testIsUsable() {
    // ACTIVE and INACTIVE are usable for encryption/decryption
    assertTrue(ManagedKeyState.isUsable(ManagedKeyState.ACTIVE));
    assertTrue(ManagedKeyState.isUsable(ManagedKeyState.INACTIVE));

    // Other states are not usable
    assertFalse(ManagedKeyState.isUsable(ManagedKeyState.FAILED));
    assertFalse(ManagedKeyState.isUsable(ManagedKeyState.DISABLED));
    assertFalse(ManagedKeyState.isUsable(ManagedKeyState.ACTIVE_DISABLED));
    assertFalse(ManagedKeyState.isUsable(ManagedKeyState.INACTIVE_DISABLED));
  }

  @Test
  public void testIsKeyManagementState() {
    // States with val < 4 are key management states (apply to namespaces)
    assertTrue(ManagedKeyState.isKeyManagementState(ManagedKeyState.ACTIVE));
    assertTrue(ManagedKeyState.isKeyManagementState(ManagedKeyState.FAILED));
    assertTrue(ManagedKeyState.isKeyManagementState(ManagedKeyState.DISABLED));

    // States with val >= 4 are key-specific states
    assertFalse(ManagedKeyState.isKeyManagementState(ManagedKeyState.INACTIVE));
    assertFalse(ManagedKeyState.isKeyManagementState(ManagedKeyState.ACTIVE_DISABLED));
    assertFalse(ManagedKeyState.isKeyManagementState(ManagedKeyState.INACTIVE_DISABLED));
  }

  @Test
  public void testGetExternalState() {
    // ACTIVE_DISABLED and INACTIVE_DISABLED should map to DISABLED
    assertEquals(ManagedKeyState.DISABLED, ManagedKeyState.ACTIVE_DISABLED.getExternalState());
    assertEquals(ManagedKeyState.DISABLED, ManagedKeyState.INACTIVE_DISABLED.getExternalState());

    // Other states should return themselves
    assertEquals(ManagedKeyState.ACTIVE, ManagedKeyState.ACTIVE.getExternalState());
    assertEquals(ManagedKeyState.INACTIVE, ManagedKeyState.INACTIVE.getExternalState());
    assertEquals(ManagedKeyState.FAILED, ManagedKeyState.FAILED.getExternalState());
    assertEquals(ManagedKeyState.DISABLED, ManagedKeyState.DISABLED.getExternalState());
  }

  @Test
  public void testStateValuesUnique() {
    // Ensure all state values are unique
    ManagedKeyState[] states = ManagedKeyState.values();
    for (int i = 0; i < states.length; i++) {
      for (int j = i + 1; j < states.length; j++) {
        assertNotNull(states[i]);
        assertNotNull(states[j]);
        assertFalse("State values must be unique: " + states[i] + " vs " + states[j],
          states[i].getVal() == states[j].getVal());
      }
    }
  }
}

