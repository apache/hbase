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
package org.apache.hadoop.hbase.util;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.net.BoundSocketMaker;
import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Utility for running {@link SimpleKdcServer}. Kerby KDC server is favored over Hadoop
 * org.apache.hadoop.minikdc server which has support in the HBaseTestingUtility at
 * #setupMiniKdc. The Kerby KDC Server came in with HBASE-5291. Its preferred. Less baggage.
 * @see #getRunningSimpleKdcServer(File, Supplier)
 */
public final class SimpleKdcServerUtil {
  protected static final Logger LOG = LoggerFactory.getLogger(SimpleKdcServerUtil.class);

  private SimpleKdcServerUtil() {}

  /**
   * Returns a running kdc server. Use this method rather than start the SimpleKdcServer
   * yourself because it takes care of BindExceptions which can happen even though port-picking
   * is random (between the choice of port number and bind, it could have been used elsewhere).
   * @return A SimpleKdcServer on which 'start' has been called; be sure to call stop on this
   *   instance when done.
   */
  public static SimpleKdcServer getRunningSimpleKdcServer(File testDir,
      Supplier<Integer> randomPortGenerator) throws KrbException, IOException {
    return getRunningSimpleKdcServer(testDir, randomPortGenerator, false);
  }

  /**
   * Internal method for testing.
   * @param portClash True if we want to generate BindException (for testing).
   * @return A running SimpleKdcServer on loopback/'localhost' on a random port
   * @see #getRunningSimpleKdcServer(File, Supplier)
   */
  static SimpleKdcServer getRunningSimpleKdcServer(File testDir,
      Supplier<Integer> randomPortGenerator, final boolean portClash)
        throws KrbException, IOException {
    File kdcDir = new File(testDir, SimpleKdcServer.class.getSimpleName());
    Preconditions.checkArgument(kdcDir.mkdirs(), "Failed create of " + kdcDir);
    String hostName = InetAddress.getLoopbackAddress().getHostName();
    BoundSocketMaker bsm = portClash? new BoundSocketMaker(randomPortGenerator): null;
    final int retries = 10;
    for (int i = 0; i < retries; i++) {
      SimpleKdcServer kdc = new SimpleKdcServer();
      kdc.setWorkDir(kdcDir);
      kdc.setKdcHost(hostName);
      kdc.setAllowTcp(true);
      kdc.setAllowUdp(false);
      int kdcPort = bsm != null? bsm.getPort(): randomPortGenerator.get();
      try {
        kdc.setKdcTcpPort(kdcPort);
        LOG.info("Starting KDC server at {}:{}", hostName, kdcPort);
        kdc.init();
        kdc.start();
        return kdc;
      } catch (KrbException ke) {
        if (kdc != null) {
          kdc.stop();
        }
        if (ke.getCause() != null && ke.getCause() instanceof BindException) {
          LOG.info("Clashed using port {}; getting a new random port", kdcPort);
          continue;
        } else {
          throw ke;
        }
      } finally {
        if (bsm != null) {
          bsm.close();
          bsm = null;
        }
      }
    }
    // If we get here, we exhausted our retries. Fail.
    throw new KrbException("Failed create of SimpleKdcServer after " + retries + " attempts");
  }
}
