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
package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class, ClientTests.class})
public class TestConnectionId {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
          HBaseClassTestRule.forClass(TestConnectionId.class);

  private Configuration testConfig = HBaseConfiguration.create();
  private User testUser1 = User.createUserForTesting(testConfig, "test", new String[]{"testgroup"});
  private User testUser2 = User.createUserForTesting(testConfig, "test", new String[]{"testgroup"});
  private String serviceName = "test";
  private InetSocketAddress address = new InetSocketAddress(999);
  private ConnectionId connectionId1 = new ConnectionId(testUser1, serviceName, address);
  private ConnectionId connectionId2 = new ConnectionId(testUser2, serviceName, address);

  @Test
  public void testGetServiceName() {
    assertEquals("test", connectionId1.getServiceName());
  }

  @Test
  public void testGetAddress() {
    assertEquals(address, connectionId1.getAddress());
    assertEquals(address, connectionId2.getAddress());
  }

  @Test
  public void testGetTicket() {
    assertEquals(testUser1, connectionId1.getTicket());
    assertNotEquals(testUser2, connectionId1.getTicket());
  }

  @Test
  public void testToString() {
    String expectedString = "0.0.0.0/0.0.0.0:999/test/test (auth:SIMPLE)";
    assertEquals(expectedString, connectionId1.toString());
  }

  /**
   * Test if the over-ridden equals method satisfies all the properties
   * (reflexive, symmetry, transitive and null)
   * along with their hashcode
   */
  @Test
  public void testEqualsWithHashCode() {
    // Test the Reflexive Property
    assertTrue(connectionId1.equals(connectionId1));

    // Test the Symmetry Property
    ConnectionId connectionId = new ConnectionId(testUser1, serviceName, address);
    assertTrue(connectionId.equals(connectionId1) && connectionId1.equals(connectionId));
    assertEquals(connectionId.hashCode(), connectionId1.hashCode());

    // Test the Transitive Property
    ConnectionId connectionId3 = new ConnectionId(testUser1, serviceName, address);
    assertTrue(connectionId1.equals(connectionId) && connectionId.equals(connectionId3) &&
            connectionId1.equals(connectionId3));
    assertEquals(connectionId.hashCode(), connectionId3.hashCode());

    // Test For null
    assertFalse(connectionId1.equals(null));

    // Test different instances of same class
    assertFalse(connectionId1.equals(connectionId2));
  }

  /**
   * Test the hashcode for same object and different object with both hashcode
   * function and static hashcode function
   */
  @Test
  public void testHashCode() {
    int testHashCode = connectionId1.hashCode();
    int expectedHashCode = ConnectionId.hashCode(testUser1, serviceName, address);
    assertEquals(expectedHashCode, testHashCode);

    // Make sure two objects are not same and test for hashcode
    assertNotEquals(connectionId1, connectionId2);
    assertNotEquals(connectionId1.hashCode(), connectionId2.hashCode());
  }
}
