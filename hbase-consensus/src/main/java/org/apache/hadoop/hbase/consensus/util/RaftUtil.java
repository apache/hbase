package org.apache.hadoop.hbase.consensus.util;

import com.facebook.nifty.client.NettyClientConfig;
import com.facebook.nifty.client.NettyClientConfigBuilder;
import com.facebook.nifty.client.NiftyClient;
import com.facebook.swift.codec.ThriftCodecManager;
import com.facebook.swift.service.ThriftClientEventHandler;
import com.facebook.swift.service.ThriftClientManager;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.consensus.quorum.AggregateTimer;
import org.apache.hadoop.hbase.consensus.quorum.RepeatingTimer;
import org.apache.hadoop.hbase.consensus.quorum.TimeoutEventHandler;
import org.apache.hadoop.hbase.consensus.quorum.Timer;
import org.apache.hadoop.hbase.util.Bytes;
import org.jboss.netty.channel.socket.nio.NioSocketChannelConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RaftUtil {
  private static final Logger LOG = LoggerFactory.getLogger(RaftUtil.class);
  private static ThriftClientManager clientManager;

  static {
    try {
      NettyClientConfigBuilder clientConfigBuilder =
        NettyClientConfig.newBuilder();
      final NioSocketChannelConfig socketConfig =
        clientConfigBuilder.getSocketChannelConfig();

      socketConfig.setKeepAlive(true);
      socketConfig.setTcpNoDelay(true);
      clientConfigBuilder.setBossThreadCount(2);

      clientManager = new ThriftClientManager(
        new ThriftCodecManager(),
        new NiftyClient(clientConfigBuilder.build()),
        ImmutableSet.<ThriftClientEventHandler>of());
    } catch (Throwable t) {
      LOG.error("Unable to initialize ThriftClientManager.", t);
      throw t;
    }
  }

  public static HRegionInfo createDummyRegionInfo(String region) {
    return createDummyRegionInfo(region, null);
  }

  public static HRegionInfo createDummyRegionInfo(String region, Map<HServerAddress,
    Integer> peers) {
    HRegionInfo regionInfo = new HRegionInfo(new HTableDescriptor(region),
        Bytes.toBytes("00000000"), Bytes.toBytes("ffffffff"), false, 1000,
        null, null);
    Map<String, Map<HServerAddress, Integer>> peerMap = new HashMap<>();
    peerMap.put(HRegionInfo.LOCAL_DC_KEY, peers);
    regionInfo.setPeers(peerMap);
    return regionInfo;
  }

  public static <T> String listToString(List<T> list) {
    if (list == null) {
      return null;
    }
    return Joiner.on(", ").useForNull("null").join(list);
  }

  public static HServerAddress getHRegionServerAddress(HServerAddress
                                                            localConsensusServerAddress) {
    return new HServerAddress(localConsensusServerAddress.getBindAddress(),
      localConsensusServerAddress.getPort() - HConstants.CONSENSUS_SERVER_PORT_JUMP);
  }

  public static HServerAddress getLocalConsensusAddress(HServerAddress
                                                         regionServerAddress) {
    return new HServerAddress(regionServerAddress.getBindAddress(),
      regionServerAddress.getPort() + HConstants.CONSENSUS_SERVER_PORT_JUMP);
  }

  public static ThriftClientManager getThriftClientManager() {
    return clientManager;
  }

  public static Timer createTimer(boolean useAggregateTimer, final String name,
                                  final long delay, TimeUnit unit,
                                  final TimeoutEventHandler callback,
                                  final AggregateTimer aggregateTimer) {
    if (useAggregateTimer) {
      return aggregateTimer.createTimer(name, delay, unit, callback);
    }
    return new RepeatingTimer(name, delay, unit, callback);
  }

  public static boolean isNetworkError(Throwable e) {
    return 
      e instanceof org.apache.thrift.transport.TTransportException
      || e instanceof com.facebook.swift.service.RuntimeTTransportException;
  }
}
