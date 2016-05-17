/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.zookeeper;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({MiscTests.class, SmallTests.class})
public class TestZKConfig {

  @Test
  public void testZKConfigLoading() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    // Test that we read only from the config instance
    // (i.e. via hbase-default.xml and hbase-site.xml)
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181);
    Properties props = ZKConfig.makeZKProps(conf);
    assertEquals("Property client port should have been default from the HBase config",
                        "2181",
                        props.getProperty("clientPort"));
  }

  @Test
  public void testGetZooKeeperClusterKey() {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.ZOOKEEPER_QUORUM, "\tlocalhost\n");
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "3333");
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "hbase");
    String clusterKey = ZKConfig.getZooKeeperClusterKey(conf, "test");
    assertTrue(!clusterKey.contains("\t") && !clusterKey.contains("\n"));
    assertEquals("localhost:3333:hbase,test", clusterKey);
  }

  @Test
  public void testClusterKey() throws Exception {
    testKey("server", 2181, "/hbase");
    testKey("server1,server2,server3", 2181, "/hbase");
    try {
      ZKConfig.validateClusterKey("2181:/hbase");
    } catch (IOException ex) {
      // OK
    }
  }

  @Test
  public void testClusterKeyWithMultiplePorts() throws Exception {
    // server has different port than the default port
    testKey("server1:2182", 2181, "/hbase", true);
    // multiple servers have their own port
    testKey("server1:2182,server2:2183,server3:2184", 2181, "/hbase", true);
    // one server has no specified port, should use default port
    testKey("server1:2182,server2,server3:2184", 2181, "/hbase", true);
    // the last server has no specified port, should use default port
    testKey("server1:2182,server2:2183,server3", 2181, "/hbase", true);
    // multiple servers have no specified port, should use default port for those servers
    testKey("server1:2182,server2,server3:2184,server4", 2181, "/hbase", true);
    // same server, different ports
    testKey("server1:2182,server1:2183,server1", 2181, "/hbase", true);
    // mix of same server/different port and different server
    testKey("server1:2182,server2:2183,server1", 2181, "/hbase", true);
  }

  private void testKey(String ensemble, int port, String znode)
      throws IOException {
    testKey(ensemble, port, znode, false); // not support multiple client ports
  }

  private void testKey(String ensemble, int port, String znode, Boolean multiplePortSupport)
      throws IOException {
    Configuration conf = new Configuration();
    String key = ensemble+":"+port+":"+znode;
    String ensemble2 = null;
    ZKConfig.ZKClusterKey zkClusterKey = ZKConfig.transformClusterKey(key);
    if (multiplePortSupport) {
      ensemble2 = ZKConfig.standardizeZKQuorumServerString(ensemble,
          Integer.toString(port));
      assertEquals(ensemble2, zkClusterKey.getQuorumString());
    }
    else {
      assertEquals(ensemble, zkClusterKey.getQuorumString());
    }
    assertEquals(port, zkClusterKey.getClientPort());
    assertEquals(znode, zkClusterKey.getZnodeParent());

    conf = HBaseConfiguration.createClusterConf(conf, key);
    assertEquals(zkClusterKey.getQuorumString(), conf.get(HConstants.ZOOKEEPER_QUORUM));
    assertEquals(zkClusterKey.getClientPort(), conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, -1));
    assertEquals(zkClusterKey.getZnodeParent(), conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));

    String reconstructedKey = ZKConfig.getZooKeeperClusterKey(conf);
    if (multiplePortSupport) {
      String key2 = ensemble2 + ":" + port + ":" + znode;
      assertEquals(key2, reconstructedKey);
    }
    else {
      assertEquals(key, reconstructedKey);
    }
  }
}
