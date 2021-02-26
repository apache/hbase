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
import static org.junit.Assert.fail;
import java.io.IOException;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.net.BoundSocketMaker;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MiscTests.class, MediumTests.class})
public class TestJMXListener {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestJMXListener.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestJMXListener.class);
  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static int CONNECTOR_PORT;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // Default RMI timeouts are too long. Make them short for test.
    System.setProperty("sun.rmi.transport.connectionTimeout", Integer.toString(5000));
    System.setProperty("sun.rmi.transport.tcp.handshakeTimeout", Integer.toString(5000));
    System.setProperty("sun.rmi.transport.tcp.responseTimeout", Integer.toString(5000));
    System.setProperty("sun.rmi.transport.tcp.readTimeout", Integer.toString(5000));
    Configuration conf = UTIL.getConfiguration();
    // To test what happens when the jmx listener can't put up its port, uncomment the below.
    BoundSocketMaker bsm = null; // new BoundSocketMaker(HBaseTestingUtility::randomFreePort);
    conf.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY, JMXListener.class.getName());
    CONNECTOR_PORT = bsm == null? HBaseTestingUtility.randomFreePort(): bsm.getPort();
    // Make sure the JMX listener is up before we proceed. If it is not up, retry. It may not
    // come up if there is a port clash/bind exception except its called something else in rmi.
    for (int i = 0; i < 10; i++) {
      conf.setInt("regionserver.rmi.registry.port", CONNECTOR_PORT);
      UTIL.startMiniCluster();
      // Make sure we can get make a JMX connection before proceeding. It may have failed setup
      // because of port clash/bind-exception. Deal with it here.
      JMXConnector connector = null;
      try {
        connector = JMXConnectorFactory.
          connect(JMXListener.buildJMXServiceURL(CONNECTOR_PORT, CONNECTOR_PORT));
        break;
      } catch (IOException ioe) {
        UTIL.shutdownMiniCluster();
        CONNECTOR_PORT = HBaseTestingUtility.randomFreePort();
      } finally {
        if (connector != null) {
          connector.close();
        }
      }
    }
    if (bsm != null) {
      bsm.close();
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testStart() throws Exception {
    JMXConnector connector = JMXConnectorFactory.connect(
      JMXListener.buildJMXServiceURL(CONNECTOR_PORT, CONNECTOR_PORT));

    MBeanServerConnection mb = connector.getMBeanServerConnection();
    String domain = mb.getDefaultDomain();
    Assert.assertTrue("default domain is not correct",
      !domain.isEmpty());
    connector.close();

  }

  //shutdown hbase only. then try connect, IOException expected
  @Rule
  public ExpectedException expectedEx = ExpectedException.none();
  @Test
  public void testStop() throws Exception {
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    LOG.info("shutdown hbase cluster...");
    cluster.shutdown();
    LOG.info("wait for the hbase cluster shutdown...");
    cluster.waitUntilShutDown();

    JMXConnector connector = JMXConnectorFactory.newJMXConnector(
      JMXListener.buildJMXServiceURL(CONNECTOR_PORT, CONNECTOR_PORT), null);
    expectedEx.expect(IOException.class);
    connector.connect();

  }

  @Test
  public void testGetRegionServerCoprocessors() throws Exception {
    for (JVMClusterUtil.RegionServerThread rs : UTIL.getHBaseCluster().getRegionServerThreads()) {
      boolean find = false;
      for (String s : rs.getRegionServer().getRegionServerCoprocessors()) {
        if (s.equals(JMXListener.class.getSimpleName())) {
          find = true;
          break;
        }
      }
      if (!find) {
        fail("where is the JMXListener?");
      }
    }
  }
}
