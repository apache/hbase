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
package org.apache.hadoop.hbase.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseZKTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.ZKTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test for HQuorumPeer.
 */
@Category({ ZKTests.class, SmallTests.class })
public class TestHQuorumPeer {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHQuorumPeer.class);

  private static final HBaseZKTestingUtility TEST_UTIL = new HBaseZKTestingUtility();
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
    assertEquals("127.0.0.1:2888:3888", properties.get("server.0"));
    assertNull(properties.get("server.1"));

    String oldValue = conf.get(HConstants.ZOOKEEPER_QUORUM);
    conf.set(HConstants.ZOOKEEPER_QUORUM, "a.foo.bar,b.foo.bar,c.foo.bar");
    properties = ZKConfig.makeZKProps(conf);
    assertEquals(dataDir.toString(), properties.get("dataDir"));
    assertEquals(Integer.valueOf(PORT_NO),
      Integer.valueOf(properties.getProperty("clientPort")));
    assertEquals("a.foo.bar:2888:3888", properties.get("server.0"));
    assertEquals("b.foo.bar:2888:3888", properties.get("server.1"));
    assertEquals("c.foo.bar:2888:3888", properties.get("server.2"));
    assertNull(properties.get("server.3"));
    conf.set(HConstants.ZOOKEEPER_QUORUM, oldValue);
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
