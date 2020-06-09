/**
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
package org.apache.hadoop.hbase.zookeeper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.*;

/**
 * Test for HQuorumPeer.
 */
@Category(MediumTests.class)
public class TestHQuorumPeer {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static int PORT_NO = 21818;
  private Path dataDir;


  @Before public void setup() throws IOException {
    // Set it to a non-standard port.
    TEST_UTIL.getConfiguration().setInt(HConstants.ZOOKEEPER_CLIENT_PORT,
        PORT_NO);
    this.dataDir = TEST_UTIL.getDataTestDir(this.getClass().getName());
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    if (fs.exists(this.dataDir)) {
      if (!fs.delete(this.dataDir, true)) {
        throw new IOException("Failed cleanup of " + this.dataDir);
      }
    }
    if (!fs.mkdirs(this.dataDir)) {
      throw new IOException("Failed create of " + this.dataDir);
    }
  }

  @Test public void testMakeZKProps() {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.set(HConstants.ZOOKEEPER_DATA_DIR, this.dataDir.toString());
    Properties properties = ZKConfig.makeZKProps(conf);
    assertEquals(dataDir.toString(), (String)properties.get("dataDir"));
    assertEquals(Integer.valueOf(PORT_NO),
      Integer.valueOf(properties.getProperty("clientPort")));
    assertEquals("localhost:2888:3888", properties.get("server.0"));
    assertEquals(null, properties.get("server.1"));

    String oldValue = conf.get(HConstants.ZOOKEEPER_QUORUM);
    conf.set(HConstants.ZOOKEEPER_QUORUM, "a.foo.bar,b.foo.bar,c.foo.bar");
    properties = ZKConfig.makeZKProps(conf);
    assertEquals(dataDir.toString(), properties.get("dataDir"));
    assertEquals(Integer.valueOf(PORT_NO),
      Integer.valueOf(properties.getProperty("clientPort")));
    assertEquals("a.foo.bar:2888:3888", properties.get("server.0"));
    assertEquals("b.foo.bar:2888:3888", properties.get("server.1"));
    assertEquals("c.foo.bar:2888:3888", properties.get("server.2"));
    assertEquals(null, properties.get("server.3"));
    conf.set(HConstants.ZOOKEEPER_QUORUM, oldValue);
  }

  @Test public void testConfigInjection() throws Exception {
    String s =
      "dataDir=" + this.dataDir.toString() + "\n" +
      "clientPort=2181\n" +
      "initLimit=2\n" +
      "syncLimit=2\n" +
      "server.0=${hbase.master.hostname}:2888:3888\n" +
      "server.1=server1:2888:3888\n" +
      "server.2=server2:2888:3888\n";

    System.setProperty("hbase.master.hostname", "localhost");
    InputStream is = new ByteArrayInputStream(s.getBytes());
    Configuration conf = TEST_UTIL.getConfiguration();
    Properties properties = ZKConfig.parseZooCfg(conf, is);

    assertEquals(this.dataDir.toString(), properties.get("dataDir"));
    assertEquals(Integer.valueOf(2181),
      Integer.valueOf(properties.getProperty("clientPort")));
    assertEquals("localhost:2888:3888", properties.get("server.0"));

    HQuorumPeer.writeMyID(properties);
    QuorumPeerConfig config = new QuorumPeerConfig();
    config.parseProperties(properties);

    assertEquals(this.dataDir.toString(), config.getDataDir().toString());
    assertEquals(2181, config.getClientPortAddress().getPort());
    Map<Long,QuorumServer> servers = config.getServers();
    assertEquals(3, servers.size());
    assertTrue(servers.containsKey(Long.valueOf(0)));
    QuorumServer server = servers.get(Long.valueOf(0));
    assertEquals("localhost", getHostName(server));

    // Override with system property.
    System.setProperty("hbase.master.hostname", "foo.bar");
    is = new ByteArrayInputStream(s.getBytes());
    properties = ZKConfig.parseZooCfg(conf, is);
    assertEquals("foo.bar:2888:3888", properties.get("server.0"));
    config.parseProperties(properties);

    servers = config.getServers();
    server = servers.get(Long.valueOf(0));
    assertEquals("foo.bar", getHostName(server));
  }

  // The reflection in this method is needed to smooth over internal differences
  // between ZK 3.4 and 3.6. The type of 'addr' in 3.4 is InetSocketAddress. In
  // 3.6 it becomes org.apache.zookeeper.server.quorum.MultipleAddresses, a set
  // of InetSocketAddress.
  private static String getHostName(QuorumServer server) throws Exception {
    String hostname;
    switch (server.addr.getClass().getName()) {
      case "org.apache.zookeeper.server.quorum.MultipleAddresses":
        // ZK 3.6 and up
        Method m = server.addr.getClass().getDeclaredMethod("getOne");
        hostname = ((InetSocketAddress)m.invoke(server.addr)).getHostName();
        break;
      default:
        // ZK <= 3.5
        Field f = server.getClass().getField("addr");
        hostname = ((InetSocketAddress)f.get(server)).getHostName();
        break;
    }
    return hostname;
  }

  @Test public void testShouldAssignDefaultZookeeperClientPort() {
    Configuration config = HBaseConfiguration.create();
    config.clear();
    Properties p = ZKConfig.makeZKProps(config);
    assertNotNull(p);
    assertEquals(2181, p.get("clientPort"));
  }

  @Test
  public void testGetZKQuorumServersString() {
    Configuration config = new Configuration(TEST_UTIL.getConfiguration());
    config.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, 8888);
    config.set(HConstants.ZOOKEEPER_QUORUM, "foo:1234,bar:5678,baz,qux:9012");

    String s = ZKConfig.getZKQuorumServersString(config);
    assertEquals("foo:1234,bar:5678,baz:8888,qux:9012", s);
  }
}

