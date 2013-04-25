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

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This tests whether ServerSocketChannel works over ipv6, which Zookeeper
 * depends on. On Windows Oracle JDK 6, creating a ServerSocketChannel throws
 * java.net.SocketException: Address family not supported by protocol family
 * exception. It is a known JVM bug, seems to be only resolved for JDK7:
 * http://bugs.sun.com/view_bug.do?bug_id=6230761
 *
 * For this test, we check that whether we are effected by this bug, and if so
 * the test ensures that we are running with java.net.preferIPv4Stack=true, so
 * that ZK will not fail to bind to ipv6 address using ClientCnxnSocketNIO.
 */
@Category(SmallTests.class)
public class TestIPv6NIOServerSocketChannel {

  private static final Log LOG = LogFactory.getLog(TestIPv6NIOServerSocketChannel.class);

  /**
   * Creates and binds a regular ServerSocket.
   */
  private void bindServerSocket(InetAddress inetAddr) throws IOException {
    while(true) {
      int port = HBaseTestingUtility.randomFreePort();
      InetSocketAddress addr = new InetSocketAddress(inetAddr, port);
      ServerSocket serverSocket = null;
      try {
        serverSocket = new ServerSocket();
        serverSocket.bind(addr);
        break;
      } catch (BindException ex) {
        //continue
      } finally {
        if (serverSocket != null) {
          serverSocket.close();
        }
      }
    }
  }

  /**
   * Creates a NIO ServerSocketChannel, and gets the ServerSocket from
   * there. Then binds the obtained socket.
   * This fails on Windows with Oracle JDK1.6.0u33, if the passed InetAddress is a
   * IPv6 address. Works on Oracle JDK 1.7.
   */
  private void bindNIOServerSocket(InetAddress inetAddr) throws IOException {
    while (true) {
      int port = HBaseTestingUtility.randomFreePort();
      InetSocketAddress addr = new InetSocketAddress(inetAddr, port);
      ServerSocketChannel channel = null;
      ServerSocket serverSocket = null;
      try {
        channel = ServerSocketChannel.open();
        serverSocket = channel.socket();
        serverSocket.bind(addr); // This does not work
        break;
      } catch (BindException ex) {
        //continue
      } finally {
        if (serverSocket != null) {
          serverSocket.close();
        }
        if (channel != null) {
          channel.close();
        }
      }  
    }
  }

  /**
   * Checks whether we are effected by the JDK issue on windows, and if so
   * ensures that we are running with preferIPv4Stack=true.
   */
  @Test
  public void testServerSocket() throws IOException {
    byte[] addr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 };
    InetAddress inetAddr = InetAddress.getByAddress(addr);

    try {
      bindServerSocket(inetAddr);
      bindNIOServerSocket(inetAddr);
      //if on *nix or windows JDK7, both will pass
    } catch(java.net.SocketException ex) {
      //On Windows JDK6, we will get expected exception:
      //java.net.SocketException: Address family not supported by protocol family
      //or java.net.SocketException: Protocol family not supported
      Assert.assertFalse(ex.getClass().isInstance(BindException.class));
      Assert.assertTrue(ex.getMessage().toLowerCase().contains("protocol family"));
      LOG.info("Received expected exception:");
      LOG.info(ex);

      //if this is the case, ensure that we are running on preferIPv4=true
      ensurePreferIPv4();
    }
  }

  /**
   * Checks whether we are running with java.net.preferIPv4Stack=true
   */
  public void ensurePreferIPv4() throws IOException {
    InetAddress[] addrs = InetAddress.getAllByName("localhost");
    for (InetAddress addr : addrs) {
      LOG.info("resolved localhost as:" + addr);
      Assert.assertEquals(4, addr.getAddress().length); //ensure 4 byte ipv4 address
    }
  }

  /**
   * Tests whether every InetAddress we obtain by resolving can open a
   * ServerSocketChannel.
   */
  @Test
  public void testServerSocketFromLocalhostResolution() throws IOException {
    InetAddress[] addrs = InetAddress.getAllByName("localhost");
    for (InetAddress addr : addrs) {
      LOG.info("resolved localhost as:" + addr);
      bindServerSocket(addr);
      bindNIOServerSocket(addr);
    }
  }

  public static void main(String[] args) throws Exception {
    TestIPv6NIOServerSocketChannel test = new TestIPv6NIOServerSocketChannel();
    test.testServerSocket();
    test.testServerSocketFromLocalhostResolution();
  }
}
