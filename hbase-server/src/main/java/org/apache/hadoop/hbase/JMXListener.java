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
package org.apache.hadoop.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.*;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.util.HashMap;

import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import javax.rmi.ssl.SslRMIServerSocketFactory;

/**
 * Pluggable JMX Agent for HBase(to fix the 2 random TCP ports issue
 * of the out-of-the-box JMX Agent):
 * 1)connector port can share with the registry port if SSL is OFF
 * 2)support password authentication
 * 3)support subset of SSL (with default configuration)
 */
public class JMXListener implements Coprocessor {

  public static final Log LOG = LogFactory.getLog(JMXListener.class);
  public static final String RMI_REGISTRY_PORT_CONF_KEY = ".rmi.registry.port";
  public static final String RMI_CONNECTOR_PORT_CONF_KEY = ".rmi.connector.port";
  public static final int defMasterRMIRegistryPort = 10101;
  public static final int defRegionserverRMIRegistryPort = 10102;

  /**
   * workaround for HBASE-11146
   * master and regionserver are in 1 JVM in standalone mode
   * only 1 JMX instance is allowed, otherwise there is port conflict even if
   * we only load regionserver coprocessor on master
   */
  private static JMXConnectorServer jmxCS = null;

  public static JMXServiceURL buildJMXServiceURL(int rmiRegistryPort,
      int rmiConnectorPort) throws IOException {
    // Build jmxURL
    StringBuilder url = new StringBuilder();
    url.append("service:jmx:rmi://localhost:");
    url.append(rmiConnectorPort);
    url.append("/jndi/rmi://localhost:");
    url.append(rmiRegistryPort);
    url.append("/jmxrmi");

    return new JMXServiceURL(url.toString());

  }

  public void startConnectorServer(int rmiRegistryPort, int rmiConnectorPort)
              throws IOException {
    boolean rmiSSL = false;
    boolean authenticate = true;
    String passwordFile = null;
    String accessFile = null;

    System.setProperty("java.rmi.server.randomIDs", "true");

    String rmiSSLValue = System.getProperty("com.sun.management.jmxremote.ssl",
                                            "false");
    rmiSSL = Boolean.parseBoolean(rmiSSLValue);

    String authenticateValue =
        System.getProperty("com.sun.management.jmxremote.authenticate", "false");
    authenticate = Boolean.parseBoolean(authenticateValue);

    passwordFile = System.getProperty("com.sun.management.jmxremote.password.file");
    accessFile = System.getProperty("com.sun.management.jmxremote.access.file");

    LOG.info("rmiSSL:" + rmiSSLValue + ",authenticate:" + authenticateValue
              + ",passwordFile:" + passwordFile + ",accessFile:" + accessFile);

    // Environment map
    HashMap<String, Object> jmxEnv = new HashMap<String, Object>();

    RMIClientSocketFactory csf = null;
    RMIServerSocketFactory ssf = null;

    if (rmiSSL) {
      if (rmiRegistryPort == rmiConnectorPort) {
        throw new IOException("SSL is enabled. " +
            "rmiConnectorPort cannot share with the rmiRegistryPort!");
      }
      csf = new SslRMIClientSocketFactory();
      ssf = new SslRMIServerSocketFactory();
    }

    if (csf != null) {
      jmxEnv.put(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE, csf);
    }
    if (ssf != null) {
      jmxEnv.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE, ssf);
    }

    // Configure authentication
    if (authenticate) {
      jmxEnv.put("jmx.remote.x.password.file", passwordFile);
      jmxEnv.put("jmx.remote.x.access.file", accessFile);
    }

    // Create the RMI registry
    LocateRegistry.createRegistry(rmiRegistryPort);
    // Retrieve the PlatformMBeanServer.
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

    // Build jmxURL
    JMXServiceURL serviceUrl = buildJMXServiceURL(rmiRegistryPort, rmiConnectorPort);

    try {
      // Start the JMXListener with the connection string
      jmxCS = JMXConnectorServerFactory.newJMXConnectorServer(serviceUrl, jmxEnv, mbs);
      jmxCS.start();
      LOG.info("ConnectorServer started!");
    } catch (IOException e) {
      LOG.error("fail to start connector server!", e);
    }

  }

  public void stopConnectorServer() throws IOException {
    synchronized(JMXListener.class) {
      if (jmxCS != null) {
        jmxCS.stop();
        LOG.info("ConnectorServer stopped!");
        jmxCS = null;
      }
    }
  }


  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    int rmiRegistryPort = -1;
    int rmiConnectorPort = -1;
    Configuration conf = env.getConfiguration();

    if (env instanceof MasterCoprocessorEnvironment) {
      // running on Master
      rmiRegistryPort =
          conf.getInt("master" + RMI_REGISTRY_PORT_CONF_KEY, defMasterRMIRegistryPort);
      rmiConnectorPort = conf.getInt("master" + RMI_CONNECTOR_PORT_CONF_KEY, rmiRegistryPort);
      LOG.info("Master rmiRegistryPort:" + rmiRegistryPort + ",Master rmiConnectorPort:"
          + rmiConnectorPort);
    } else if (env instanceof RegionServerCoprocessorEnvironment) {
      // running on RegionServer
      rmiRegistryPort =
        conf.getInt("regionserver" + RMI_REGISTRY_PORT_CONF_KEY,
        defRegionserverRMIRegistryPort);
      rmiConnectorPort =
        conf.getInt("regionserver" + RMI_CONNECTOR_PORT_CONF_KEY, rmiRegistryPort);
      LOG.info("RegionServer rmiRegistryPort:" + rmiRegistryPort
        + ",RegionServer rmiConnectorPort:" + rmiConnectorPort);

    } else if (env instanceof RegionCoprocessorEnvironment) {
      LOG.error("JMXListener should not be loaded in Region Environment!");
      return;
    }

    synchronized(JMXListener.class) {
      if (jmxCS != null) {
        LOG.info("JMXListener has been started at Registry port " + rmiRegistryPort);
      }
      else {
        startConnectorServer(rmiRegistryPort, rmiConnectorPort);
      }
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    stopConnectorServer();
  }

}
