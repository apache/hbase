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

package org.apache.hadoop.hbase.client;


import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.util.Threads;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.jboss.netty.channel.socket.DatagramChannelFactory;
import org.jboss.netty.channel.socket.oio.OioDatagramChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;


/**
 * A class that receives the cluster status, and provide it as a set of service to the client.
 * Today, manages only the dead server list.
 * The class is abstract to allow multiple implementations, from ZooKeeper to multicast based.
 */
@InterfaceAudience.Private
class ClusterStatusListener implements Closeable {
  private static final Log LOG = LogFactory.getLog(ClusterStatusListener.class);
  private final List<ServerName> deadServers = new ArrayList<ServerName>();
  protected final DeadServerHandler deadServerHandler;
  private final Listener listener;

  /**
   * The implementation class to use to read the status.
   */
  public static final String STATUS_LISTENER_CLASS = "hbase.status.listener.class";
  public static final Class<? extends Listener> DEFAULT_STATUS_LISTENER_CLASS =
      MulticastListener.class;

  /**
   * Class to be extended to manage a new dead server.
   */
  public interface DeadServerHandler {

    /**
     * Called when a server is identified as dead. Called only once even if we receive the
     * information multiple times.
     *
     * @param sn - the server name
     */
    void newDead(ServerName sn);
  }


  /**
   * The interface to be implemented by a listener of a cluster status event.
   */
  interface Listener extends Closeable {
    /**
     * Called to close the resources, if any. Cannot throw an exception.
     */
    @Override
    void close();

    /**
     * Called to connect.
     *
     * @param conf Configuration to use.
     * @throws IOException
     */
    void connect(Configuration conf) throws IOException;
  }

  public ClusterStatusListener(DeadServerHandler dsh, Configuration conf,
                               Class<? extends Listener> listenerClass) throws IOException {
    this.deadServerHandler = dsh;
    try {
      Constructor<? extends Listener> ctor =
          listenerClass.getConstructor(ClusterStatusListener.class);
      this.listener = ctor.newInstance(this);
    } catch (InstantiationException e) {
      throw new IOException("Can't create listener " + listenerClass.getName(), e);
    } catch (IllegalAccessException e) {
      throw new IOException("Can't create listener " + listenerClass.getName(), e);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException();
    } catch (InvocationTargetException e) {
      throw new IllegalStateException();
    }

    this.listener.connect(conf);
  }

  /**
   * Acts upon the reception of a new cluster status.
   *
   * @param ncs the cluster status
   */
  public void receive(ClusterStatus ncs) {
    if (ncs.getDeadServerNames() != null) {
      for (ServerName sn : ncs.getDeadServerNames()) {
        if (!isDeadServer(sn)) {
          LOG.info("There is a new dead server: " + sn);
          deadServers.add(sn);
          if (deadServerHandler != null) {
            deadServerHandler.newDead(sn);
          }
        }
      }
    }
  }

  @Override
  public void close() {
    listener.close();
  }

  /**
   * Check if we know if a server is dead.
   *
   * @param sn the server name to check.
   * @return true if we know for sure that the server is dead, false otherwise.
   */
  public boolean isDeadServer(ServerName sn) {
    if (sn.getStartcode() <= 0) {
      return false;
    }

    for (ServerName dead : deadServers) {
      if (dead.getStartcode() >= sn.getStartcode() &&
          dead.getPort() == sn.getPort() &&
          dead.getHostname().equals(sn.getHostname())) {
        return true;
      }
    }

    return false;
  }


  /**
   * An implementation using a multicast message between the master & the client.
   */
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
  class MulticastListener implements Listener {
    private DatagramChannel channel;
    private final ExecutorService service = Executors.newSingleThreadExecutor(
        Threads.newDaemonThreadFactory("hbase-client-clusterStatus-multiCastListener"));


    public MulticastListener() {
    }

    @Override
    public void connect(Configuration conf) throws IOException {
      // Can't be NiO with Netty today => not implemented in Netty.
      DatagramChannelFactory f = new OioDatagramChannelFactory(service);

      ConnectionlessBootstrap b = new ConnectionlessBootstrap(f);
      b.setPipeline(Channels.pipeline(
          new ProtobufDecoder(ClusterStatusProtos.ClusterStatus.getDefaultInstance()),
          new ClusterStatusHandler()));

      String mcAddress = conf.get(HConstants.STATUS_MULTICAST_ADDRESS,
          HConstants.DEFAULT_STATUS_MULTICAST_ADDRESS);
      String bindAddress = conf.get(HConstants.STATUS_MULTICAST_BIND_ADDRESS,
        HConstants.DEFAULT_STATUS_MULTICAST_BIND_ADDRESS);
      int port = conf.getInt(HConstants.STATUS_MULTICAST_PORT,
          HConstants.DEFAULT_STATUS_MULTICAST_PORT);

      channel = (DatagramChannel) b.bind(new InetSocketAddress(bindAddress, port));

      channel.getConfig().setReuseAddress(true);

      InetAddress ina;
      try {
        ina = InetAddress.getByName(mcAddress);
      } catch (UnknownHostException e) {
        throw new IOException("Can't connect to " + mcAddress, e);
      }
      channel.joinGroup(ina);
    }

    @Override
    public void close() {
      if (channel != null) {
        channel.close();
        channel = null;
      }
      service.shutdown();
    }


    /**
     * Class, conforming to the Netty framework, that manages the message received.
     */
    private class ClusterStatusHandler extends SimpleChannelUpstreamHandler {

      @Override
      public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        ClusterStatusProtos.ClusterStatus csp = (ClusterStatusProtos.ClusterStatus) e.getMessage();
        ClusterStatus ncs = ClusterStatus.convert(csp);
        receive(ncs);
      }

      /**
       * Invoked when an exception was raised by an I/O thread or a
       * {@link org.jboss.netty.channel.ChannelHandler}.
       */
      @Override
      public void exceptionCaught(
          ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        LOG.error("Unexpected exception, continuing.", e.getCause());
      }
    }
  }
}
