package org.apache.hadoop.hbase.consensus.server.peer;

import com.facebook.nifty.client.FramedClientConnector;
import com.facebook.nifty.duplex.TDuplexProtocolFactory;
import com.facebook.swift.service.ThriftClient;
import com.facebook.swift.service.ThriftClientConfig;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;
import org.apache.hadoop.hbase.consensus.server.ConsensusService;
import org.apache.hadoop.hbase.consensus.util.RaftUtil;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@NotThreadSafe
public class PeerConsensusServer extends AbstractPeer {
  private static Logger LOG = LoggerFactory.getLogger(PeerConsensusServer.class);

  private ThriftClientConfig thriftClientConf;
  private ThriftClient<ConsensusService> thriftClient;
  private ListenableFuture<ConsensusService> futureConnection;
  private ConsensusService agent;

  public PeerConsensusServer(final HServerAddress address,
                             final int rank,
                             final MutableRaftContext replica,
                             final Configuration conf) {
    super(address, rank, replica, conf);
    int connectionTimeout = conf.getInt(
            HConstants.RAFT_PEERSERVER_CONNECTION_TIMEOUT_MS, 1000);
    int readTimeout = conf.getInt(
            HConstants.RAFT_PEERSERVER_READ_TIMEOUT_MS, 10000);
    int writeTimeout = conf.getInt(
            HConstants.RAFT_PEERSERVER_WRITE_TIMEOUT_MS, 10000);

    thriftClientConf = new ThriftClientConfig()
            .setConnectTimeout(new Duration(connectionTimeout,
                    TimeUnit.MILLISECONDS))
            .setReadTimeout(new Duration(readTimeout, TimeUnit.MILLISECONDS))
            .setWriteTimeout(new Duration(writeTimeout, TimeUnit.MILLISECONDS));
  }

  @Override
  public void stop() {
    super.stop();
    synchronized (this) {
      if (futureConnection != null) {
        futureConnection.cancel(true);
      }
      if (agent != null) {
        try {
          agent.close();
        } catch (Exception e) {
          LOG.error("Cannot close the agent. Error: ", e);
        }
      }
    }
  }

  @Override
  protected void resetConnection()
    throws ExecutionException, InterruptedException, TimeoutException {
    LOG.debug("Resetting the peer connection to " + this + "(" + getAddress() + ")");
    thriftClient =
            new ThriftClient<>(RaftUtil.getThriftClientManager(), ConsensusService.class,
                    thriftClientConf, this.toString());
    ListenableFuture<ConsensusService> futureConnection = thriftClient.open(
      new FramedClientConnector(getAddress().getInetSocketAddress(),
        TDuplexProtocolFactory.fromSingleFactory(
          new TCompactProtocol.Factory())));
    setAgent(futureConnection, futureConnection.get(
      1000,
      TimeUnit.MILLISECONDS));
  }

  @Override
  protected boolean clearConsensusServiceAgent(ConsensusService localAgent) {
    // TODO: double check with Nifty community to better handler exceptions
    if (LOG.isDebugEnabled()) {
      LOG.debug(getRaftContext().getMyAddress() + " attempting to close "
        + localAgent + "(" + getAddress() + ")");
    }
    try {
      localAgent.close();
    } catch (Exception e) {
      LOG.error("Unable to close the connection: " + e);
    } finally {
      return resetAgentIfEqual(localAgent);
    }
  }

  private synchronized ConsensusService getAgent() {
    return this.agent;
  }

  private synchronized void setAgent(ListenableFuture<ConsensusService> futureConnection, ConsensusService agent) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(getRaftContext().getMyAddress() + " setting agent to " +
        agent + "(" + getAddress() + ")" +  " using " + futureConnection);
    }
    this.futureConnection = futureConnection;
    this.agent = agent;
  }

  private synchronized boolean resetAgentIfEqual(ConsensusService localAgent) {
    boolean success = false;
    if (agent == localAgent) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(getRaftContext().getMyAddress() + " resetting agent "
          + agent + " to null" + "(" + getAddress() + ")");
      }
      agent = null;
      success = true;
    }
    return success;
  }

  @Override
  protected ConsensusService getConsensusServiceAgent() {
    if (getAgent() == null) {
      synchronized (this) {
        if (getAgent() == null) {
          for (int i = 0; i < connectionRetry; i++) {
            try {
              resetConnection();
              if (LOG.isDebugEnabled()) {
                LOG.debug("New connection established from " + getRaftContext().getMyAddress()
                  + " to " + this + ": agent = " + agent + "(" + getAddress() + ")");
              }
            } catch (Throwable t) {
              LOG.error("Failed to reset the connection to " + this +  " to " +
                getAddress() + " due to " + t);
              try {
                Thread.sleep(connectionRetryInterval * i);
              } catch (InterruptedException e) {}
            }
          }
        }
      }
    }
    return getAgent();
  }
}
