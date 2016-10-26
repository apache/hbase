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
package org.apache.hadoop.hbase.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;

/**
 * Utility used by client connections.
 */
@InterfaceAudience.Private
public final class ConnectionUtils {

  private static final Log LOG = LogFactory.getLog(ConnectionUtils.class);

  private ConnectionUtils() {}

  /**
   * Calculate pause time.
   * Built on {@link HConstants#RETRY_BACKOFF}.
   * @param pause time to pause
   * @param tries amount of tries
   * @return How long to wait after <code>tries</code> retries
   */
  public static long getPauseTime(final long pause, final int tries) {
    int ntries = tries;
    if (ntries >= HConstants.RETRY_BACKOFF.length) {
      ntries = HConstants.RETRY_BACKOFF.length - 1;
    }
    if (ntries < 0) {
      ntries = 0;
    }

    long normalPause = pause * HConstants.RETRY_BACKOFF[ntries];
    // 1% possible jitter
    long jitter = (long) (normalPause * ThreadLocalRandom.current().nextFloat() * 0.01f);
    return normalPause + jitter;
  }


  /**
   * Adds / subs an up to 50% jitter to a pause time. Minimum is 1.
   * @param pause the expected pause.
   * @param jitter the jitter ratio, between 0 and 1, exclusive.
   */
  public static long addJitter(final long pause, final float jitter) {
    float lag = pause * (ThreadLocalRandom.current().nextFloat() - 0.5f) * jitter;
    long newPause = pause + (long) lag;
    if (newPause <= 0) {
      return 1;
    }
    return newPause;
  }

  /**
   * @param conn The connection for which to replace the generator.
   * @param cnm Replaces the nonce generator used, for testing.
   * @return old nonce generator.
   */
  public static NonceGenerator injectNonceGeneratorForTesting(
      ClusterConnection conn, NonceGenerator cnm) {
    return ConnectionImplementation.injectNonceGeneratorForTesting(conn, cnm);
  }

  /**
   * Changes the configuration to set the number of retries needed when using Connection
   * internally, e.g. for  updating catalog tables, etc.
   * Call this method before we create any Connections.
   * @param c The Configuration instance to set the retries into.
   * @param log Used to log what we set in here.
   */
  public static void setServerSideHConnectionRetriesConfig(
      final Configuration c, final String sn, final Log log) {
    // TODO: Fix this. Not all connections from server side should have 10 times the retries.
    int hcRetries = c.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    // Go big.  Multiply by 10.  If we can't get to meta after this many retries
    // then something seriously wrong.
    int serversideMultiplier = c.getInt("hbase.client.serverside.retries.multiplier", 10);
    int retries = hcRetries * serversideMultiplier;
    c.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, retries);
    log.info(sn + " server-side Connection retries=" + retries);
  }

  /**
   * Creates a short-circuit connection that can bypass the RPC layer (serialization,
   * deserialization, networking, etc..) when talking to a local server.
   * @param conf the current configuration
   * @param pool the thread pool to use for batch operations
   * @param user the user the connection is for
   * @param serverName the local server name
   * @param admin the admin interface of the local server
   * @param client the client interface of the local server
   * @return an short-circuit connection.
   * @throws IOException if IO failure occurred
   */
  public static ClusterConnection createShortCircuitConnection(final Configuration conf,
    ExecutorService pool, User user, final ServerName serverName,
    final AdminService.BlockingInterface admin, final ClientService.BlockingInterface client)
    throws IOException {
    if (user == null) {
      user = UserProvider.instantiate(conf).getCurrent();
    }
    return new ConnectionImplementation(conf, pool, user) {
      @Override
      public AdminService.BlockingInterface getAdmin(ServerName sn) throws IOException {
        return serverName.equals(sn) ? admin : super.getAdmin(sn);
      }

      @Override
      public ClientService.BlockingInterface getClient(ServerName sn) throws IOException {
        return serverName.equals(sn) ? client : super.getClient(sn);
      }
    };
  }

  /**
   * Setup the connection class, so that it will not depend on master being online. Used for testing
   * @param conf configuration to set
   */
  @VisibleForTesting
  public static void setupMasterlessConnection(Configuration conf) {
    conf.set(ClusterConnection.HBASE_CLIENT_CONNECTION_IMPL,
      MasterlessConnection.class.getName());
  }

  /**
   * Some tests shut down the master. But table availability is a master RPC which is performed on
   * region re-lookups.
   */
  static class MasterlessConnection extends ConnectionImplementation {
    MasterlessConnection(Configuration conf,
      ExecutorService pool, User user) throws IOException {
      super(conf, pool, user);
    }

    @Override
    public boolean isTableDisabled(TableName tableName) throws IOException {
      // treat all tables as enabled
      return false;
    }
  }

  /**
   * Return retires + 1. The returned value will be in range [1, Integer.MAX_VALUE].
   */
  static int retries2Attempts(int retries) {
    return Math.max(1, retries == Integer.MAX_VALUE ? Integer.MAX_VALUE : retries + 1);
  }

  /**
   * Get a unique key for the rpc stub to the given server.
   */
  static String getStubKey(String serviceName, ServerName serverName,
      boolean hostnameCanChange) {
    // Sometimes, servers go down and they come back up with the same hostname but a different
    // IP address. Force a resolution of the rsHostname by trying to instantiate an
    // InetSocketAddress, and this way we will rightfully get a new stubKey.
    // Also, include the hostname in the key so as to take care of those cases where the
    // DNS name is different but IP address remains the same.
    String hostname = serverName.getHostname();
    int port = serverName.getPort();
    if (hostnameCanChange) {
      try {
        InetAddress ip = InetAddress.getByName(hostname);
        return serviceName + "@" + hostname + "-" + ip.getHostAddress() + ":" + port;
      } catch (UnknownHostException e) {
        LOG.warn("Can not resolve " + hostname + ", please check your network", e);
      }
    }
    return serviceName + "@" + hostname + ":" + port;
  }

  static void checkHasFamilies(Mutation mutation) {
    Preconditions.checkArgument(mutation.numFamilies() > 0,
      "Invalid arguments to %s, zero columns specified", mutation.toString());
  }

  /** Dummy nonce generator for disabled nonces. */
  static final NonceGenerator NO_NONCE_GENERATOR = new NonceGenerator() {

    @Override
    public long newNonce() {
      return HConstants.NO_NONCE;
    }

    @Override
    public long getNonceGroup() {
      return HConstants.NO_NONCE;
    }
  };
}
