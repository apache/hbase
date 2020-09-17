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


package org.apache.hadoop.hbase.master;

import java.io.Closeable;
import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetricsBuilder;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.io.netty.bootstrap.Bootstrap;
import org.apache.hbase.thirdparty.io.netty.buffer.Unpooled;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelException;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelFactory;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelOption;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.DatagramChannel;
import org.apache.hbase.thirdparty.io.netty.channel.socket.DatagramPacket;
import org.apache.hbase.thirdparty.io.netty.channel.socket.InternetProtocolFamily;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioDatagramChannel;
import org.apache.hbase.thirdparty.io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.hbase.thirdparty.io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to publish the cluster status to the client. This allows them to know immediately
 *  the dead region servers, hence to cut the connection they have with them, eventually stop
 *  waiting on the socket. This improves the mean time to recover, and as well allows to increase
 *  on the client the different timeouts, as the dead servers will be detected separately.
 */
@InterfaceAudience.Private
public class ClusterStatusPublisher extends ScheduledChore {
  private static Logger LOG = LoggerFactory.getLogger(ClusterStatusPublisher.class);
  /**
   * The implementation class used to publish the status. Default is null (no publish).
   * Use org.apache.hadoop.hbase.master.ClusterStatusPublisher.MulticastPublisher to multicast the
   * status.
   */
  public static final String STATUS_PUBLISHER_CLASS = "hbase.status.publisher.class";
  public static final Class<? extends ClusterStatusPublisher.Publisher>
      DEFAULT_STATUS_PUBLISHER_CLASS =
      org.apache.hadoop.hbase.master.ClusterStatusPublisher.MulticastPublisher.class;

  /**
   * The minimum time between two status messages, in milliseconds.
   */
  public static final String STATUS_PUBLISH_PERIOD = "hbase.status.publish.period";
  public static final int DEFAULT_STATUS_PUBLISH_PERIOD = 10000;

  private long lastMessageTime = 0;
  private final HMaster master;
  private final int messagePeriod; // time between two message
  private final ConcurrentMap<ServerName, Integer> lastSent = new ConcurrentHashMap<>();
  private Publisher publisher;
  private boolean connected = false;

  /**
   * We want to limit the size of the protobuf message sent, do fit into a single packet.
   * a reasonable size for ip / ethernet is less than 1Kb.
   */
  public final static int MAX_SERVER_PER_MESSAGE = 10;

  /**
   * If a server dies, we're sending the information multiple times in case a receiver misses the
   * message.
   */
  public final static int NB_SEND = 5;

  public ClusterStatusPublisher(HMaster master, Configuration conf,
                                Class<? extends Publisher> publisherClass)
      throws IOException {
    super("ClusterStatusPublisher for=" + master.getName(), master, conf.getInt(
      STATUS_PUBLISH_PERIOD, DEFAULT_STATUS_PUBLISH_PERIOD));
    this.master = master;
    this.messagePeriod = conf.getInt(STATUS_PUBLISH_PERIOD, DEFAULT_STATUS_PUBLISH_PERIOD);
    try {
      this.publisher = publisherClass.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new IOException("Can't create publisher " + publisherClass.getName(), e);
    }
    this.publisher.connect(conf);
    connected = true;
  }

  @Override
  public String toString() {
    return super.toString() + ", publisher=" + this.publisher + ", connected=" + this.connected;
  }

  // For tests only
  protected ClusterStatusPublisher() {
    master = null;
    messagePeriod = 0;
  }

  @Override
  protected void chore() {
    if (!isConnected()) {
      return;
    }

    List<ServerName> sns = generateDeadServersListToSend();
    if (sns.isEmpty()) {
      // Nothing to send. Done.
      return;
    }

    final long curTime = EnvironmentEdgeManager.currentTime();
    if (lastMessageTime > curTime - messagePeriod) {
      // We already sent something less than 10 second ago. Done.
      return;
    }

    // Ok, we're going to send something then.
    lastMessageTime = curTime;

    // We're reusing an existing protobuf message, but we don't send everything.
    // This could be extended in the future, for example if we want to send stuff like the
    //  hbase:meta server name.
    publisher.publish(ClusterMetricsBuilder.newBuilder()
      .setHBaseVersion(VersionInfo.getVersion())
      .setClusterId(master.getMasterFileSystem().getClusterId().toString())
      .setMasterName(master.getServerName())
      .setDeadServerNames(sns)
      .build());
  }

  @Override
  protected synchronized void cleanup() {
    connected = false;
    publisher.close();
  }

  private synchronized boolean isConnected() {
    return this.connected;
  }

  /**
   * Create the dead server to send. A dead server is sent NB_SEND times. We send at max
   * MAX_SERVER_PER_MESSAGE at a time. if there are too many dead servers, we send the newly
   * dead first.
   */
  protected List<ServerName> generateDeadServersListToSend() {
    // We're getting the message sent since last time, and add them to the list
    long since = EnvironmentEdgeManager.currentTime() - messagePeriod * 2;
    for (Pair<ServerName, Long> dead : getDeadServers(since)) {
      lastSent.putIfAbsent(dead.getFirst(), 0);
    }

    // We're sending the new deads first.
    List<Map.Entry<ServerName, Integer>> entries = new ArrayList<>(lastSent.entrySet());
    Collections.sort(entries, new Comparator<Map.Entry<ServerName, Integer>>() {
      @Override
      public int compare(Map.Entry<ServerName, Integer> o1, Map.Entry<ServerName, Integer> o2) {
        return o1.getValue().compareTo(o2.getValue());
      }
    });

    // With a limit of MAX_SERVER_PER_MESSAGE
    int max = entries.size() > MAX_SERVER_PER_MESSAGE ? MAX_SERVER_PER_MESSAGE : entries.size();
    List<ServerName> res = new ArrayList<>(max);

    for (int i = 0; i < max; i++) {
      Map.Entry<ServerName, Integer> toSend = entries.get(i);
      if (toSend.getValue() >= (NB_SEND - 1)) {
        lastSent.remove(toSend.getKey());
      } else {
        lastSent.replace(toSend.getKey(), toSend.getValue(), toSend.getValue() + 1);
      }

      res.add(toSend.getKey());
    }

    return res;
  }

  /**
   * Get the servers which died since a given timestamp.
   * protected because it can be subclassed by the tests.
   */
  protected List<Pair<ServerName, Long>> getDeadServers(long since) {
    if (master.getServerManager() == null) {
      return Collections.emptyList();
    }

    return master.getServerManager().getDeadServers().copyDeadServersSince(since);
  }


  public interface Publisher extends Closeable {

    void connect(Configuration conf) throws IOException;

    void publish(ClusterMetrics cs);

    @Override
    void close();
  }

  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
  public static class MulticastPublisher implements Publisher {
    private DatagramChannel channel;
    private final EventLoopGroup group = new NioEventLoopGroup(1,
      new ThreadFactoryBuilder().setNameFormat("hbase-master-clusterStatusPublisher-pool-%d")
        .setDaemon(true).setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());

    public MulticastPublisher() {
    }

    @Override
    public String toString() {
      return "channel=" + this.channel;
    }

    @Override
    public void connect(Configuration conf) throws IOException {
      String mcAddress = conf.get(HConstants.STATUS_MULTICAST_ADDRESS,
          HConstants.DEFAULT_STATUS_MULTICAST_ADDRESS);
      int port = conf.getInt(HConstants.STATUS_MULTICAST_PORT,
          HConstants.DEFAULT_STATUS_MULTICAST_PORT);
      String bindAddress = conf.get(HConstants.STATUS_MULTICAST_PUBLISHER_BIND_ADDRESS,
        HConstants.DEFAULT_STATUS_MULTICAST_PUBLISHER_BIND_ADDRESS);
      String niName = conf.get(HConstants.STATUS_MULTICAST_NI_NAME);

      final InetAddress ina;
      try {
        ina = InetAddress.getByName(mcAddress);
      } catch (UnknownHostException e) {
        close();
        throw new IOException("Can't connect to " + mcAddress, e);
      }
      final InetSocketAddress isa = new InetSocketAddress(mcAddress, port);

      InternetProtocolFamily family;
      NetworkInterface ni;
      if (niName != null) {
        if (ina instanceof Inet6Address) {
          family = InternetProtocolFamily.IPv6;
        } else {
          family = InternetProtocolFamily.IPv4;
        }
        ni = NetworkInterface.getByName(niName);
      } else {
        InetAddress localAddress;
        if (ina instanceof Inet6Address) {
          localAddress = Addressing.getIp6Address();
          family = InternetProtocolFamily.IPv6;
        } else {
          localAddress = Addressing.getIp4Address();
          family = InternetProtocolFamily.IPv4;
        }
        ni = NetworkInterface.getByInetAddress(localAddress);
      }
      Bootstrap b = new Bootstrap();
      b.group(group)
        .channelFactory(new HBaseDatagramChannelFactory<Channel>(NioDatagramChannel.class, family))
        .option(ChannelOption.SO_REUSEADDR, true)
        .handler(new ClusterMetricsEncoder(isa));
      try {
        LOG.debug("Channel bindAddress={}, networkInterface={}, INA={}", bindAddress, ni, ina);
        channel = (DatagramChannel) b.bind(bindAddress, 0).sync().channel();
        channel.joinGroup(ina, ni, null, channel.newPromise()).sync();
        channel.connect(isa).sync();
        // Set into configuration in case many networks available. Do this for tests so that
        // server and client use same Interface (presuming share same Configuration).
        // TestAsyncTableRSCrashPublish was failing when connected to VPN because extra networks
        // available with Master binding on one Interface and client on another so test failed.
        if (ni != null) {
          conf.set(HConstants.STATUS_MULTICAST_NI_NAME, ni.getName());
        }
      } catch (InterruptedException e) {
        close();
        throw ExceptionUtil.asInterrupt(e);
      }
    }

    private static final class HBaseDatagramChannelFactory<T extends Channel>
        implements ChannelFactory<T> {
      private final Class<? extends T> clazz;
      private final InternetProtocolFamily family;

      HBaseDatagramChannelFactory(Class<? extends T> clazz, InternetProtocolFamily family) {
        this.clazz = clazz;
        this.family = family;
      }

      @Override
      public T newChannel() {
        try {
          return ReflectionUtils.instantiateWithCustomCtor(clazz.getName(),
            new Class[] { InternetProtocolFamily.class }, new Object[] { family });

        } catch (Throwable t) {
          throw new ChannelException("Unable to create Channel from class " + clazz, t);
        }
      }

      @Override
      public String toString() {
        return StringUtil.simpleClassName(clazz) + ".class";
      }
    }

    private static final class ClusterMetricsEncoder
        extends MessageToMessageEncoder<ClusterMetrics> {
      final private InetSocketAddress isa;

      private ClusterMetricsEncoder(InetSocketAddress isa) {
        this.isa = isa;
      }

      @Override
      protected void encode(ChannelHandlerContext channelHandlerContext,
        ClusterMetrics clusterStatus, List<Object> objects) {
        objects.add(new DatagramPacket(Unpooled.wrappedBuffer(
          ClusterMetricsBuilder.toClusterStatus(clusterStatus).toByteArray()), isa));
      }
    }

    @Override
    public void publish(ClusterMetrics cs) {
      LOG.info("PUBLISH {}", cs);
      channel.writeAndFlush(cs).syncUninterruptibly();
    }

    @Override
    public void close() {
      if (channel != null) {
        channel.close();
      }
      group.shutdownGracefully();
    }
  }
}
