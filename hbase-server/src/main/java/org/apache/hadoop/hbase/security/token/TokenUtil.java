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
package org.apache.hadoop.hbase.security.token;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.token.Token;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AuthenticationProtos;

/**
 * Utility methods for obtaining authentication tokens.
 */
@InterfaceAudience.Public
public class TokenUtil {
  // This class is referenced indirectly by User out in common; instances are created by reflection
  private static final Logger LOG = LoggerFactory.getLogger(TokenUtil.class);

    /**
     * See {@link ClientTokenUtil#obtainToken(org.apache.hadoop.hbase.client.AsyncConnection)}.
     * @deprecated External users should not use this method. Please post on
     *   the HBase dev mailing list if you need this method. Internal
     *   HBase code should use {@link ClientTokenUtil} instead.
     */
  @Deprecated
  public static CompletableFuture<Token<AuthenticationTokenIdentifier>> obtainToken(
      AsyncConnection conn) {
    return ClientTokenUtil.obtainToken(conn);
  }

  /**
   * It was removed in HBase-2.0 but added again as spark code relies on this method to obtain
   * delegation token
   * @deprecated Since 2.0.0.
   */
  @Deprecated
  public static Token<AuthenticationTokenIdentifier> obtainToken(Configuration conf)
      throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      return obtainToken(connection);
    }
  }

  /**
   * See {@link ClientTokenUtil#obtainToken(org.apache.hadoop.hbase.client.Connection)}.
   * @deprecated External users should not use this method. Please post on
   *   the HBase dev mailing list if you need this method. Internal
   *   HBase code should use {@link ClientTokenUtil} instead.
   */
  @Deprecated
  public static Token<AuthenticationTokenIdentifier> obtainToken(Connection conn)
      throws IOException {
    return ClientTokenUtil.obtainToken(conn);
  }


  /**
   * See {@link ClientTokenUtil#toToken(Token)}.
   * @deprecated External users should not use this method. Please post on
   *   the HBase dev mailing list if you need this method. Internal
   *   HBase code should use {@link ClientTokenUtil} instead.
   */
  @Deprecated
  public static AuthenticationProtos.Token toToken(Token<AuthenticationTokenIdentifier> token) {
    return ClientTokenUtil.toToken(token);
  }

  /**
   * See {@link ClientTokenUtil#obtainToken(Connection, User)}.
   * @deprecated External users should not use this method. Please post on
   *   the HBase dev mailing list if you need this method. Internal
   *   HBase code should use {@link ClientTokenUtil} instead.
   */
  @Deprecated
  public static Token<AuthenticationTokenIdentifier> obtainToken(
      final Connection conn, User user) throws IOException, InterruptedException {
    return ClientTokenUtil.obtainToken(conn, user);
  }

  /**
   * See {@link ClientTokenUtil#obtainAndCacheToken(Connection, User)}.
   */
  public static void obtainAndCacheToken(final Connection conn,
      User user)
      throws IOException, InterruptedException {
    ClientTokenUtil.obtainAndCacheToken(conn, user);
  }

  /**
   * See {@link ClientTokenUtil#toToken(org.apache.hadoop.hbase.shaded.protobuf.generated.AuthenticationProtos.Token)}.
   * @deprecated External users should not use this method. Please post on
   *   the HBase dev mailing list if you need this method. Internal
   *   HBase code should use {@link ClientTokenUtil} instead.
   */
  @Deprecated
  public static Token<AuthenticationTokenIdentifier> toToken(AuthenticationProtos.Token proto) {
    return ClientTokenUtil.toToken(proto);
  }

  private static Text getClusterId(Token<AuthenticationTokenIdentifier> token)
      throws IOException {
    return token.getService() != null
        ? token.getService() : new Text("default");
  }

  /**
   * Obtain an authentication token on behalf of the given user and add it to
   * the credentials for the given map reduce job.
   * @param conn The HBase cluster connection
   * @param user The user for whom to obtain the token
   * @param job The job instance in which the token should be stored
   * @throws IOException If making a remote call to the authentication service fails
   * @throws InterruptedException If executing as the given user is interrupted
   */
  public static void obtainTokenForJob(final Connection conn,
      User user, Job job)
      throws IOException, InterruptedException {
    try {
      Token<AuthenticationTokenIdentifier> token = ClientTokenUtil.obtainToken(conn, user);

      if (token == null) {
        throw new IOException("No token returned for user " + user.getName());
      }
      Text clusterId = getClusterId(token);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Obtained token " + token.getKind().toString() + " for user " +
            user.getName() + " on cluster " + clusterId.toString());
      }
      job.getCredentials().addToken(clusterId, token);
    } catch (IOException ioe) {
      throw ioe;
    } catch (InterruptedException ie) {
      throw ie;
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new UndeclaredThrowableException(e,
          "Unexpected exception obtaining token for user " + user.getName());
    }
  }

  /**
   * Obtain an authentication token on behalf of the given user and add it to
   * the credentials for the given map reduce job.
   * @param conn The HBase cluster connection
   * @param user The user for whom to obtain the token
   * @param job The job configuration in which the token should be stored
   * @throws IOException If making a remote call to the authentication service fails
   * @throws InterruptedException If executing as the given user is interrupted
   */
  public static void obtainTokenForJob(final Connection conn, final JobConf job, User user)
      throws IOException, InterruptedException {
    try {
      Token<AuthenticationTokenIdentifier> token = ClientTokenUtil.obtainToken(conn, user);

      if (token == null) {
        throw new IOException("No token returned for user " + user.getName());
      }
      Text clusterId = getClusterId(token);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Obtained token " + token.getKind().toString() + " for user " +
            user.getName() + " on cluster " + clusterId.toString());
      }
      job.getCredentials().addToken(clusterId, token);
    } catch (IOException ioe) {
      throw ioe;
    } catch (InterruptedException ie) {
      throw ie;
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new UndeclaredThrowableException(e,
          "Unexpected exception obtaining token for user "+user.getName());
    }
  }

  /**
   * Checks for an authentication token for the given user, obtaining a new token if necessary,
   * and adds it to the credentials for the given map reduce job.
   *
   * @param conn The HBase cluster connection
   * @param user The user for whom to obtain the token
   * @param job The job configuration in which the token should be stored
   * @throws IOException If making a remote call to the authentication service fails
   * @throws InterruptedException If executing as the given user is interrupted
   */
  public static void addTokenForJob(final Connection conn, final JobConf job, User user)
      throws IOException, InterruptedException {

    Token<AuthenticationTokenIdentifier> token = getAuthToken(conn, user);
    if (token == null) {
      token = ClientTokenUtil.obtainToken(conn, user);
    }
    job.getCredentials().addToken(token.getService(), token);
  }

  /**
   * Checks for an authentication token for the given user, obtaining a new token if necessary,
   * and adds it to the credentials for the given map reduce job.
   *
   * @param conn The HBase cluster connection
   * @param user The user for whom to obtain the token
   * @param job The job instance in which the token should be stored
   * @throws IOException If making a remote call to the authentication service fails
   * @throws InterruptedException If executing as the given user is interrupted
   */
  public static void addTokenForJob(final Connection conn, User user, Job job)
      throws IOException, InterruptedException {
    Token<AuthenticationTokenIdentifier> token = getAuthToken(conn, user);
    if (token == null) {
      token = ClientTokenUtil.obtainToken(conn, user);
    }
    job.getCredentials().addToken(token.getService(), token);
  }

  /**
   * Checks if an authentication tokens exists for the connected cluster,
   * obtaining one if needed and adding it to the user's credentials.
   *
   * @param conn The HBase cluster connection
   * @param user The user for whom to obtain the token
   * @throws IOException If making a remote call to the authentication service fails
   * @throws InterruptedException If executing as the given user is interrupted
   * @return true if the token was added, false if it already existed
   */
  public static boolean addTokenIfMissing(Connection conn, User user)
      throws IOException, InterruptedException {
    Token<AuthenticationTokenIdentifier> token = getAuthToken(conn, user);
    if (token == null) {
      token = ClientTokenUtil.obtainToken(conn, user);
      user.getUGI().addToken(token.getService(), token);
      return true;
    }
    return false;
  }

  /**
   * Get the authentication token of the user for the cluster specified in the configuration
   * @return null if the user does not have the token, otherwise the auth token for the cluster.
   */
  private static Token<AuthenticationTokenIdentifier> getAuthToken(Connection conn, User user)
      throws IOException {
    final String clusterId = conn.getClusterId();
    if (clusterId == null) {
      throw new IOException("Failed to get cluster ID");
    }
    return new AuthenticationTokenSelector().selectToken(new Text(clusterId), user.getTokens());
  }
}
