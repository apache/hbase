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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.util.regex.Pattern;

import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestServerName {
  @Test
  public void testGetHostNameMinusDomain() {
    assertEquals("2607:f0d0:1002:51::4",
      ServerName.getHostNameMinusDomain("2607:f0d0:1002:51::4"));
    assertEquals("2607:f0d0:1002:0051:0000:0000:0000:0004",
        ServerName.getHostNameMinusDomain("2607:f0d0:1002:0051:0000:0000:0000:0004"));
    assertEquals("1.1.1.1", ServerName.getHostNameMinusDomain("1.1.1.1"));
    assertEquals("x", ServerName.getHostNameMinusDomain("x"));
    assertEquals("x", ServerName.getHostNameMinusDomain("x.y.z"));
    assertEquals("asf000", ServerName.getHostNameMinusDomain("asf000.sp2.ygridcore.net"));
    ServerName sn = ServerName.valueOf("asf000.sp2.ygridcore.net", 1, 1);
    assertEquals("asf000.sp2.ygridcore.net,1,1", sn.toString());
  }

  @Test
  public void testShortString() {
    ServerName sn = ServerName.valueOf("asf000.sp2.ygridcore.net", 1, 1);
    assertEquals("asf000:1", sn.toShortString());
    sn = ServerName.valueOf("2607:f0d0:1002:0051:0000:0000:0000:0004", 1, 1);
    assertEquals("2607:f0d0:1002:0051:0000:0000:0000:0004:1", sn.toShortString());
    sn = ServerName.valueOf("1.1.1.1", 1, 1);
    assertEquals("1.1.1.1:1", sn.toShortString());
  }

  @Test
  public void testRegexPatterns() {
    assertTrue(Pattern.matches(Addressing.VALID_PORT_REGEX, "123"));
    assertFalse(Pattern.matches(Addressing.VALID_PORT_REGEX, ""));
    assertTrue(ServerName.SERVERNAME_PATTERN.matcher("www1.example.org,1234,567").matches());
    ServerName.parseServerName("a.b.c,58102,1319771740322");
    ServerName.parseServerName("192.168.1.199,58102,1319771740322");
    ServerName.parseServerName("a.b.c:58102");
    ServerName.parseServerName("192.168.1.199:58102");
  }

  @Test public void testParseOfBytes() {
    final String snStr = "www.example.org,1234,5678";
    ServerName sn = ServerName.valueOf(snStr);
    byte [] versionedBytes = sn.getVersionedBytes();
    assertEquals(sn.toString(), ServerName.parseVersionedServerName(versionedBytes).toString());
    final String hostnamePortStr = sn.getHostAndPort();
    byte [] bytes = Bytes.toBytes(hostnamePortStr);
    String expecting =
      hostnamePortStr.replace(":", ServerName.SERVERNAME_SEPARATOR) +
      ServerName.SERVERNAME_SEPARATOR + ServerName.NON_STARTCODE;
    assertEquals(expecting, ServerName.parseVersionedServerName(bytes).toString());
  }

  @Test
  public void testServerName() {
    ServerName sn = ServerName.valueOf("www.example.org", 1234, 5678);
    ServerName sn2 = ServerName.valueOf("www.example.org", 1234, 5678);
    ServerName sn3 = ServerName.valueOf("www.example.org", 1234, 56789);
    assertTrue(sn.equals(sn2));
    assertFalse(sn.equals(sn3));
    assertEquals(sn.hashCode(), sn2.hashCode());
    assertNotSame(sn.hashCode(), sn3.hashCode());
    assertEquals(sn.toString(),
      ServerName.getServerName("www.example.org", 1234, 5678));
    assertEquals(sn.toString(),
      ServerName.getServerName("www.example.org:1234", 5678));
    assertEquals(sn.toString(),
      "www.example.org" + ServerName.SERVERNAME_SEPARATOR + "1234" +
      ServerName.SERVERNAME_SEPARATOR + "5678");
  }

  @Test
  public void getServerStartcodeFromServerName() {
    ServerName sn = ServerName.valueOf("www.example.org", 1234, 5678);
    assertEquals(5678,
      ServerName.getServerStartcodeFromServerName(sn.toString()));
    assertNotSame(5677,
      ServerName.getServerStartcodeFromServerName(sn.toString()));
  }

}

