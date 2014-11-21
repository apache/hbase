package org.apache.hadoop.hbase.consensus.client;

import com.facebook.nifty.client.FramedClientConnector;
import com.facebook.nifty.duplex.TDuplexProtocolFactory;
import com.facebook.swift.service.ThriftClient;
import com.facebook.swift.service.ThriftClientConfig;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.consensus.log.LogFileInfo;
import org.apache.hadoop.hbase.consensus.rpc.PeerStatus;
import org.apache.hadoop.hbase.consensus.server.ConsensusService;
import org.apache.hadoop.hbase.consensus.util.RaftUtil;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.net.NoRouteToHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Maintains a connection to a single peer in a quorum.
 */
@NotThreadSafe
public class QuorumThriftClientAgent {
  private final static Logger LOG = LoggerFactory.getLogger(QuorumThriftClientAgent.class);
  private final HServerAddress address;

  private ThriftClient<ConsensusService> thriftClient;
  private ListenableFuture<ConsensusService> futureConnection;
  private ThriftClientConfig thriftClientConf;
  private volatile ConsensusService agent = null;
  private  int connectionRetry = 1;

  public QuorumThriftClientAgent(String address, int connectionTimeout,
                                 int readTimeout, int writeTimeout,
                                 int connectionRetry) throws IOException {
    this.address = new HServerAddress(address);
    this.thriftClientConf = new ThriftClientConfig()
      .setConnectTimeout(new Duration(connectionTimeout, TimeUnit.MILLISECONDS))
      .setReadTimeout(new Duration(readTimeout, TimeUnit.MILLISECONDS))
      .setWriteTimeout(new Duration(writeTimeout, TimeUnit.MILLISECONDS));
    this.connectionRetry = connectionRetry;
  }

  /**
   * Return the current term if the remote consensus server is the leader,
   * otherwise return -1;
   * @param regionId
   * @return the current term if the remote consensus server is the leader,
   *         otherwise return -1;
   */
  public synchronized String getLeader(final String regionId) throws Exception {
    final ConsensusService localAgent = getConsensusServiceAgent();

    final SettableFuture<String> result = SettableFuture.create();
    final Runnable runnable = new Runnable() {
      @Override public void run() {
        try {
          Futures.addCallback(localAgent.getLeader(regionId),
            new FutureCallback<String>() {
              @Override
              public void onSuccess(@Nullable String r) {
                result.set(r);
              }
              @Override
              public void onFailure(Throwable t) {
                handleFailure(localAgent, t, result);
              }
            });
        } catch (Exception e) {
          LOG.error(String.format("%s. Cannot send replicate commit to %s",
            regionId, address.getHostAddressWithPort()));
          handleFailure(localAgent, e, result);
        }
      }
    };
    executeRequest(localAgent, runnable, result);
    return result.get();
  }

  /**
   * Replicates a list of WALEdits, on a given quorum.
   * @param regionId The region where we want to replicate these edits.
   * @param txns The actual edits
   * @return The commit index of the committed edits
   * @throws Exception
   */
  public synchronized long replicateCommit(final String regionId,
                                           final List<WALEdit> txns)
    throws Exception {
    final ConsensusService localAgent = getConsensusServiceAgent();

    final SettableFuture<Long> result = SettableFuture.create();
    final Runnable runnable = new Runnable() {
      @Override
      public void run() {
        try {
          Futures.addCallback(localAgent.replicateCommit(regionId, txns),
            new FutureCallback<Long>() {
              @Override
              public void onSuccess(@Nullable Long r) {
                result.set(r);
              }
              @Override
              public void onFailure(Throwable t) {
                handleFailure(localAgent, t, result);
              }
            });
        } catch (Exception e) {
          LOG.error(String.format("%s. Cannot send replicate commit to %s",
            regionId, address.getHostAddressWithPort()));
          handleFailure(localAgent, e, result);
        }
      }
    };
    executeRequest(localAgent, runnable, result);
    return result.get();
  }

  public synchronized boolean changeQuorum(final String regionId,
                                           final ByteBuffer config)
    throws Exception {
    final ConsensusService localAgent = getConsensusServiceAgent();
    final SettableFuture<Boolean> result = SettableFuture.create();
    final Runnable runnable = new Runnable() {
      @Override public void run() {
        try {
          Futures.addCallback(localAgent.changeQuorum(regionId, config),
            new FutureCallback<Boolean>() {
              @Override
              public void onSuccess(@Nullable Boolean r) {
                result.set(r);
              }
              @Override
              public void onFailure(Throwable t) {
                handleFailure(localAgent, t, result);
              }
            });
        } catch (Exception e) {
          LOG.error(String.format(
            "%s Cannot send the request to change the " +
            " quorum to server %s", regionId,
            address.getHostAddressWithPort()));
          handleFailure(localAgent, e, result);
        }
      }
    };
    executeRequest(localAgent, runnable, result);
    return result.get();
  }

  public synchronized PeerStatus getPeerStatus(final String regionId) throws Exception {
    final ConsensusService localAgent = getConsensusServiceAgent();
    final SettableFuture<PeerStatus> result = SettableFuture.create();
    final Runnable runnable = new Runnable() {
      @Override public void run() {
        try {
          Futures.addCallback(localAgent.getPeerStatus(regionId),
            new FutureCallback<PeerStatus>() {
              @Override
              public void onSuccess(@Nullable PeerStatus r) {
                result.set(r);
              }
              @Override
              public void onFailure(Throwable t) {
                handleFailure(localAgent, t, result);
              }
            });
        } catch (Exception e) {
          LOG.error(String.format("%s. Cannot send replicate commit to %s",
            regionId, address.getHostAddressWithPort()));
          handleFailure(localAgent, e, result);
        }
      }
    };
    executeRequest(localAgent, runnable, result);
    return result.get();
  }

  public synchronized List<LogFileInfo> getCommittedLogStatus(final String quorumName,
                                                 final long minIndex)
    throws Exception {
    final ConsensusService localAgent = getConsensusServiceAgent();
    final SettableFuture<List<LogFileInfo>> result = SettableFuture.create();
    final Runnable runnable = new Runnable() {
      @Override public void run() {
        try {
          Futures.addCallback(localAgent.getCommittedLogStatus(quorumName, minIndex),
            new FutureCallback<List<LogFileInfo>>() {
              @Override
              public void onSuccess(@Nullable List<LogFileInfo> r) {
                result.set(r);
              }
              @Override
              public void onFailure(Throwable t) {
                handleFailure(localAgent, t, result);
              }
            });
        } catch (Exception e) {
          LOG.error(String.format("%s. Cannot send replicate commit to %s",
            quorumName, address.getHostAddressWithPort()));
          handleFailure(localAgent, e, result);
        }
      }
    };
    executeRequest(localAgent, runnable, result);
    return result.get();
  }

  public synchronized List<PeerStatus> getAllPeerStatuses() throws Exception {
    final ConsensusService localAgent = getConsensusServiceAgent();
    final SettableFuture<List<PeerStatus>> result = SettableFuture.create();
    final Runnable runnable = new Runnable() {
      @Override public void run() {
        try {
          Futures.addCallback(localAgent.getAllPeerStatuses(),
            new FutureCallback<List<PeerStatus>>() {
              @Override
              public void onSuccess(@Nullable List<PeerStatus> r) {
                result.set(r);
              }
              @Override
              public void onFailure(Throwable t) {
                handleFailure(localAgent, t, result);
              }
            });
        } catch (Exception e) {
          LOG.error(String.format("Cannot send replicate commit to %s",
            address.getHostAddressWithPort()));
          handleFailure(localAgent, e, result);
        }
      }
    };
    executeRequest(localAgent, runnable, result);
    return result.get();
  }

  public synchronized String getServerAddress() {
    return this.address.getHostNameWithPort();
  }

  private ConsensusService getConsensusServiceAgent()
    throws NoRouteToHostException {
    if (agent == null) {
      synchronized (this) {
        if (agent == null) {
          for (int i = 0; i < connectionRetry; i++) {
            try {
              resetConnection();
              if (agent != null) {
                LOG.debug(String
                  .format("New connection established to server %s.",
                    getServerAddress()));
                return agent;
              }
            } catch (Throwable t) {
              LOG.debug(String.format("Exception occurred while resetting the" +
                " connection to server %s. Error %s", getServerAddress(), t));
              if (i == connectionRetry - 1) {
                agent = null;
                throw new NoRouteToHostException(String.format("Exception" +
                  "occurred while resetting the connection to server %s." +
                  " Error %s", getServerAddress(), t));
              }
            }
          }
        }
      }
    }
    return agent;
  }

  private void resetConnection()
    throws ExecutionException, InterruptedException, TimeoutException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Resetting connection to %s", getServerAddress()));
    }

    close();
    thriftClient = new ThriftClient<>(
      RaftUtil.getThriftClientManager(), ConsensusService.class, thriftClientConf,
      this.toString());
    futureConnection = thriftClient.open(new FramedClientConnector(
      address.getInetSocketAddress(),
      TDuplexProtocolFactory.fromSingleFactory(
        new TCompactProtocol.Factory())));
    agent = futureConnection.get();
    if (agent == null) {
      LOG.error(String.format("Failed to resetConnection() to %s.",
        getServerAddress()));
    }
  }

  public void close() {
    if (agent != null) {
      try {
        agent.close();
      } catch (Exception e) {
        LOG.error(String.format("Unable to close the agent for server %s.",
          getServerAddress()), e);
      }
    }
  }

  private void executeRequest(final ConsensusService localAgent, final Runnable call,
                              final SettableFuture<?> result) {
    try {
      RaftUtil.getThriftClientManager().getNiftyChannel(localAgent)
        .executeInIoThread(call);
    } catch (Exception e) {
      handleFailure(localAgent, e, result);
    }
  }

  private void handleFailure(final ConsensusService localAgent, final Throwable t,
                             final SettableFuture<?> future) {
    future.setException(t);

    if (!RaftUtil.isNetworkError(t)) {
      return;
    }
    LOG.error(String.format("Ran into error while talking to %s.",
      address.getHostAddressWithPort()), t);
    synchronized (this) {
      if (agent == localAgent) {
        agent = null;
      }
    }
  }
}
