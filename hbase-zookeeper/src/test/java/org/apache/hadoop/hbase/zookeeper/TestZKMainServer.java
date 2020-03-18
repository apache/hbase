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
import static org.junit.Assert.assertTrue;
import java.security.Permission;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseZKTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.ZKTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ZKTests.class, SmallTests.class })
public class TestZKMainServer {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestZKMainServer.class);

  // ZKMS calls System.exit.  Catch the call and prevent exit using trick described up in
  // http://stackoverflow.com/questions/309396/java-how-to-test-methods-that-call-system-exit
  protected static class ExitException extends SecurityException {
    private static final long serialVersionUID = 1L;

    ExitException() {
      super("There is no escape!");
    }
  }

  private static class NoExitSecurityManager extends SecurityManager {
    @Override
    public void checkPermission(Permission perm) {
      // allow anything.
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
      // allow anything.
    }

    @Override
    public void checkExit(int status) {
      super.checkExit(status);
      throw new ExitException();
    }
  }

  /**
   * We need delete of a znode to work at least.
   */
  @Test
  public void testCommandLineWorks() throws Exception {
    System.setSecurityManager(new NoExitSecurityManager());
    HBaseZKTestingUtility htu = new HBaseZKTestingUtility();
    // Make it long so for sure succeeds.
    htu.getConfiguration().setInt(HConstants.ZK_SESSION_TIMEOUT, 30000);
    htu.startMiniZKCluster();
    try {
      ZKWatcher zkw = htu.getZooKeeperWatcher();
      String znode = "/testCommandLineWorks";
      ZKUtil.createWithParents(zkw, znode, HConstants.EMPTY_BYTE_ARRAY);
      ZKUtil.checkExists(zkw, znode);
      boolean exception = false;
      try {
        ZKMainServer.main(new String [] {"-server", htu.getZkCluster().getAddress().toString(),
          "delete", znode});
      } catch (ExitException ee) {
        // ZKMS calls System.exit which should trigger this exception.
        exception = true;
      }
      assertTrue(exception);
      assertEquals(-1, ZKUtil.checkExists(zkw, znode));
    } finally {
      htu.shutdownMiniZKCluster();
      System.setSecurityManager(null); // or save and restore original
    }
  }

  @Test
  public void testHostPortParse() {
    ZKMainServer parser = new ZKMainServer();
    Configuration c = HBaseConfiguration.create();
    assertEquals("127.0.0.1:" + c.get(HConstants.ZOOKEEPER_CLIENT_PORT), parser.parse(c));
    final String port = "1234";
    c.set(HConstants.ZOOKEEPER_CLIENT_PORT, port);
    c.set("hbase.zookeeper.quorum", "example.com");
    assertEquals("example.com:" + port, parser.parse(c));
    c.set("hbase.zookeeper.quorum", "example1.com,example2.com,example3.com");
    String ensemble = parser.parse(c);
    assertTrue(port, ensemble.matches("(example[1-3]\\.com:1234,){2}example[1-3]\\.com:" + port));

    // multiple servers with its own port
    c.set("hbase.zookeeper.quorum", "example1.com:5678,example2.com:9012,example3.com:3456");
    ensemble = parser.parse(c);
    assertEquals("example1.com:5678,example2.com:9012,example3.com:3456", ensemble);

    // some servers without its own port, which will be assigned the default client port
    c.set("hbase.zookeeper.quorum", "example1.com:5678,example2.com:9012,example3.com");
    ensemble = parser.parse(c);
    assertEquals(ensemble, "example1.com:5678,example2.com:9012,example3.com:" + port);
  }
}
