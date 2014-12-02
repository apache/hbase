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


package org.apache.hadoop.hbase.master;


import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ChannelFactory;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.InternetProtocolFamily;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import io.netty.util.internal.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;

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


/**
 * Class to publish the cluster status to the client. This allows them to know immediately
 *  the dead region servers, hence to cut the connection they have with them, eventually stop
 *  waiting on the socket. This improves the mean time to recover, and as well allows to increase
 *  on the client the different timeouts, as the dead servers will be detected separately.
 */
@InterfaceAudience.Private
public class ClusterStatusPublisher extends Chore {
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
  private final ConcurrentMap<ServerName, Integer> lastSent =
      new ConcurrentHashMap<ServerName, Integer>();
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
    super("HBase clusterStatusPublisher for " + master.getName(),
        conf.getInt(STATUS_PUBLISH_PERIOD, DEFAULT_STATUS_PUBLISH_PERIOD), master);
    this.master = master;
    this.messagePeriod = conf.getInt(STATUS_PUBLISH_PERIOD, DEFAULT_STATUS_PUBLISH_PERIOD);
    try {
      this.publisher = publisherClass.newInstance();
    } catch (InstantiationException e) {
      throw new IOException("Can't create publisher " + publisherClass.getName(), e);
    } catch (IllegalAccessException e) {
      throw new IOException("Can't create publisher " + publisherClass.getName(), e);
    }
    this.publisher.connect(conf);
    connected = true;
  }

  // For tests only
  protected ClusterStatusPublisher() {
    master = null;
    messagePeriod = 0;
  }

  @Override
  protected void chore() {
    if (!connected) {
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
    ClusterStatus cs = new ClusterStatus(VersionInfo.getVersion(),
        master.getMasterFileSystem().getClusterId().toString(),
        null,
        sns,
        master.getServerName(),
        null,
        null,
        null,
        null);


    publisher.publish(cs);
  }

  protected void cleanup() {
    connected = false;
    publisher.close();
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
    List<Map.Entry<ServerName, Integer>> entries = new ArrayList<Map.Entry<ServerName, Integer>>();
    entries.addAll(lastSent.entrySet());
    Collections.sort(entries, new Comparator<Map.Entry<ServerName, Integer>>() {
      @Override
      public int compare(Map.Entry<ServerName, Integer> o1, Map.Entry<ServerName, Integer> o2) {
        return o1.getValue().compareTo(o2.getValue());
      }
    });

    // With a limit of MAX_SERVER_PER_MESSAGE
    int max = entries.size() > MAX_SERVER_PER_MESSAGE ? MAX_SERVER_PER_MESSAGE : entries.size();
    List<ServerName> res = new ArrayList<ServerName>(max);

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

    void publish(ClusterStatus cs);

    @Override
    void close();
  }

  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
  public static class MulticastPublisher implements Publisher {
    private DatagramChannel channel;
    private final EventLoopGroup group = new NioEventLoopGroup(
        1, Threads.newDaemonThreadFactory("hbase-master-clusterStatusPublisher"));

    public MulticastPublisher() {
    }

    @Override
    public void connect(Configuration conf) throws IOException {
      NetworkInterface ni = NetworkInterface.getByInetAddress(Addressing.getIpAddress());

      String mcAddress = conf.get(HConstants.STATUS_MULTICAST_ADDRESS,
          HConstants.DEFAULT_STATUS_MULTICAST_ADDRESS);
      int port = conf.getInt(HConstants.STATUS_MULTICAST_PORT,
          HConstants.DEFAULT_STATUS_MULTICAST_PORT);

      final InetAddress ina;
      try {
        ina = InetAddress.getByName(mcAddress);
      } catch (UnknownHostException e) {
        close();
        throw new IOException("Can't connect to " + mcAddress, e);
      }

      final InetSocketAddress isa = new InetSocketAddress(mcAddress, port);
      InternetProtocolFamily family = InternetProtocolFamily.IPv4;
      if (ina instanceof Inet6Address) {
        family = InternetProtocolFamily.IPv6;
      }

      Bootstrap b = new Bootstrap();

      b.group(group)
      .channelFactory(new HBaseDatagramChannelFactory<Channel>(NioDatagramChannel.class, family))
      .option(ChannelOption.SO_REUSEADDR, true)
      .handler(new ClusterStatusEncoder(isa));

      try {
        channel = (DatagramChannel) b.bind(new InetSocketAddress(0)).sync().channel();
        channel.joinGroup(ina, ni, null, channel.newPromise()).sync();
        channel.connect(isa).sync();
      } catch (InterruptedException e) {
        close();
        throw ExceptionUtil.asInterrupt(e);
      }
    }

    private static final class HBaseDatagramChannelFactory<T extends Channel> implements ChannelFactory<T> {
      private final Class<? extends T> clazz;
      private InternetProtocolFamily family;

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

    private static class ClusterStatusEncoder extends MessageToMessageEncoder<ClusterStatus> {
      final private InetSocketAddress isa;

      private ClusterStatusEncoder(InetSocketAddress isa) {
        this.isa = isa;
      }

      @Override
      protected void encode(ChannelHandlerContext channelHandlerContext,
                            ClusterStatus clusterStatus, List<Object> objects) {
        ClusterStatusProtos.ClusterStatus csp = clusterStatus.convert();
        objects.add(new DatagramPacket(Unpooled.wrappedBuffer(csp.toByteArray()), isa));
      }
    }

    @Override
    public void publish(ClusterStatus cs) {
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
