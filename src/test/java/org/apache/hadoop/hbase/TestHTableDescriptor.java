/**
 * Copyright The Apache Software Foundation
 *
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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataInputBuffer;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;
import static org.junit.matchers.JUnitMatchers.containsString;

@Category(SmallTests.class)
public class TestHTableDescriptor {
  @Test
  public void testGetNameAsString() throws Exception {
    HTableDescriptor htd = new HTableDescriptor(Bytes.toBytes("Test"));

    // Make sure the to and from string works
    assertEquals("Test", htd.getNameAsString());

    // Make sure the string instance is cached.
    assertSame(htd.getNameAsString(), htd.getNameAsString());
  }

  @Test
  public void testWriteAndReadFieldsWithServerSet() throws Exception {
    HTableDescriptor htd = new HTableDescriptor("TestTable");
    htd.addFamily(new HColumnDescriptor("d"));
    htd.setMaxFileSize(Long.MAX_VALUE - 1);
    htd.setWALDisabled(true);


    Set<HServerAddress> servers = new HashSet<>();

    servers.add(new HServerAddress(java.net.InetAddress.getLocalHost().getHostName(),0));
    servers.add(new HServerAddress(java.net.InetAddress.getLocalHost().getHostName(),1));
    servers.add(new HServerAddress(java.net.InetAddress.getLocalHost().getHostName(),2));
    servers.add(new HServerAddress(java.net.InetAddress.getLocalHost().getHostName(),3));
    htd.setServers(servers);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    htd.write(out);

    byte[] buf = baos.toByteArray();

    DataInputBuffer in = new DataInputBuffer();
    in.reset(buf, 0, buf.length);

    HTableDescriptor n = new HTableDescriptor();
    n.readFields(in);

    assertEquals("TestTable", n.getNameAsString());
    assertEquals(servers, n.getServers());
    assertEquals(true, n.isWALDisabled());
    assertEquals(1, n.getColumnFamilies().length);
    assertEquals(Long.MAX_VALUE -1, n.getMaxFileSize());

  }

  @Test
  public void testWriteAndReadFieldsWithoutServerSet() throws Exception {
    HTableDescriptor htd = new HTableDescriptor("TestTable");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    htd.write(out);

    byte[] buf = baos.toByteArray();

    DataInputBuffer in = new DataInputBuffer();
    in.reset(buf, 0, buf.length);

    HTableDescriptor n = new HTableDescriptor();
    n.readFields(in);

    assertEquals("TestTable", n.getNameAsString());
    assertEquals(null, n.getServers());
  }

  @Test
  public void testSetServers() throws Exception {
    HTableDescriptor htd = new HTableDescriptor("TestTable");
    Set<HServerAddress> servers = new HashSet<>();

    servers.add(new HServerAddress(java.net.InetAddress.getLocalHost().getHostName(),0));
    servers.add(new HServerAddress(java.net.InetAddress.getLocalHost().getHostName(),1));
    servers.add(new HServerAddress(java.net.InetAddress.getLocalHost().getHostName(),2));
    servers.add(new HServerAddress(java.net.InetAddress.getLocalHost().getHostName(),3));
    htd.setServers(servers);

    assertEquals(servers, htd.getServers());

    htd.setServers(null);
    assertEquals(null, htd.getServers());
  }


  @Test(expected=IllegalArgumentException.class)
  public void testSetServersThrowErrorNoServers() throws Exception {
    HTableDescriptor htd = new HTableDescriptor("testThrowErrorNoServers");
    htd.addFamily(new HColumnDescriptor("d"));
    htd.setServers(new HashSet<HServerAddress>());
  }

  @Test
  public void testToStringServerSet() throws Exception {
    HTableDescriptor htd = new HTableDescriptor("TestTable");
    htd.addFamily(new HColumnDescriptor("d"));

    Set<HServerAddress> servers = new HashSet<>();

    String hostName = InetAddress.getLocalHost().getHostName();

    servers.add(new HServerAddress(hostName,0));
    servers.add(new HServerAddress(hostName,1));
    servers.add(new HServerAddress(hostName,2));
    servers.add(new HServerAddress(hostName,3));
    htd.setServers(servers);

    final String htdString = htd.toStringCustomizedValues();

    // Don't assert the whole string as set can have
    // different order based on which set is used and we shouldn't
    // tie the test to implementation.
    assertThat(htdString, containsString("SERVER_SET => ["));
    assertThat(htdString, containsString("'"+hostName+":0'"));
    assertThat(htdString, containsString("'"+hostName+":1'"));
    assertThat(htdString, containsString("'"+hostName+":2'"));
    assertThat(htdString, containsString("'"+hostName+":3'"));
    assertThat(htdString, containsString(" ]"));
  }
}
