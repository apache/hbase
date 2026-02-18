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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestServerNameIPv6 {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestServerNameIPv6.class);

  @Test
  public void testIpv6FullForm() {
    assertTrue(ServerName.isIpv6ServerName("2001:0db8:85a3:0000:0000:8a2e:0370:7334"));
  }

  @Test
  public void testIpv6CompressedForm() {
    assertTrue(ServerName.isIpv6ServerName("2001:db8:85a3::8a2e:370:7334"));
    assertTrue(ServerName.isIpv6ServerName("::1"));
  }

  @Test
  public void testIpv6WithBrackets() {
    // Brackets are often used in URIs
    assertTrue(ServerName.isIpv6ServerName("[::1]"));
  }

  @Test
  public void testIpv4Address() {
    assertFalse(ServerName.isIpv6ServerName("192.168.1.1"));
  }

  @Test
  public void testHostname() {
    assertFalse(ServerName.isIpv6ServerName("example.com"));
  }

  @Test
  public void testEncodedIpv6() {
    // If you are supporting encoded values (such as %3A), add related coverage.
    String encoded = java.net.URLEncoder.encode("::1", StandardCharsets.UTF_8);
    assertFalse(ServerName.isIpv6ServerName(encoded));
  }

  @Test
  public void testInvalidAddress() {
    assertFalse(ServerName.isIpv6ServerName("notAValidAddress"));
  }

  @Test
  public void testParseEncodedIPv6ServerName() {
    String encodedIPv6 = "2001%3a0db8%3a85a3%3a0%3a0%3a8a2e%3a370%3a7334";
    String serverNameString = encodedIPv6 + ",16020,12345";

    ServerName sn = ServerName.parseServerName(serverNameString);
    assertEquals("2001:0db8:85a3:0:0:8a2e:370:7334", sn.getHostname());
    assertEquals(16020, sn.getPort());
  }

  @Test
  public void testEncodeIPv6ForServerName() {
    ServerName sn = ServerName.valueOf("2001:0db8:85a3::8a2e:370:7334", 16020, 12345L);
    // hypothetical method to encode for paths
    String encoded = ServerName.getEncodedServerName(sn.getServerName()).getServerName();
    assertFalse(encoded.contains(":"));
    assertTrue(encoded.contains("%3a")); // percent encoded colons
  }
}
