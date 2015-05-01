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

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.security.User;

import com.google.common.annotations.VisibleForTesting;

/**
 * Utility used by client connections.
 */
@InterfaceAudience.Private
public final class ConnectionUtils {

  private ConnectionUtils() {}

  private static final Random RANDOM = new Random();
  /**
   * Calculate pause time.
   * Built on {@link HConstants#RETRY_BACKOFF}.
   * @param pause
   * @param tries
   * @return How long to wait after <code>tries</code> retries
   */
  public static long getPauseTime(final long pause, final int tries) {
    int ntries = tries;
    if (ntries >= HConstants.RETRY_BACKOFF.length) {
      ntries = HConstants.RETRY_BACKOFF.length - 1;
    }

    long normalPause = pause * HConstants.RETRY_BACKOFF[ntries];
    long jitter =  (long)(normalPause * RANDOM.nextFloat() * 0.01f); // 1% possible jitter
    return normalPause + jitter;
  }


  /**
   * Adds / subs a 10% jitter to a pause time. Minimum is 1.
   * @param pause the expected pause.
   * @param jitter the jitter ratio, between 0 and 1, exclusive.
   */
  public static long addJitter(final long pause, final float jitter) {
    float lag = pause * (RANDOM.nextFloat() - 0.5f) * jitter;
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
   * Changes the configuration to set the number of retries needed when using HConnection
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
    log.info(sn + " server-side HConnection retries=" + retries);
  }

  /**
   * Adapt a HConnection so that it can bypass the RPC layer (serialization,
   * deserialization, networking, etc..) -- i.e. short-circuit -- when talking to a local server.
   * @param conn the connection to adapt
   * @param serverName the local server name
   * @param admin the admin interface of the local server
   * @param client the client interface of the local server
   * @return an adapted/decorated HConnection
   */
  public static ClusterConnection createShortCircuitHConnection(final Connection conn,
      final ServerName serverName, final AdminService.BlockingInterface admin,
      final ClientService.BlockingInterface client) {
    return new ConnectionAdapter(conn) {
      @Override
      public AdminService.BlockingInterface getAdmin(
          ServerName sn, boolean getMaster) throws IOException {
        return serverName.equals(sn) ? admin : super.getAdmin(sn, getMaster);
      }

      @Override
      public ClientService.BlockingInterface getClient(
          ServerName sn) throws IOException {
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
    conf.set(HConnection.HBASE_CLIENT_CONNECTION_IMPL,
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
}
