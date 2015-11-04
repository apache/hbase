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

package org.apache.hadoop.hbase.security.token;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;

import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.zookeeper.KeeperException;

/**
 * Utility methods for obtaining authentication tokens.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TokenUtil {
  // This class is referenced indirectly by User out in common; instances are created by reflection
  private static final Log LOG = LogFactory.getLog(TokenUtil.class);

  /**
   * Obtain and return an authentication token for the current user.
   * @param conn The HBase cluster connection
   * @return the authentication token instance
   */
  public static Token<AuthenticationTokenIdentifier> obtainToken(
      Connection conn) throws IOException {
    Table meta = null;
    try {
      meta = conn.getTable(TableName.META_TABLE_NAME);
      CoprocessorRpcChannel rpcChannel = meta.coprocessorService(HConstants.EMPTY_START_ROW);
      AuthenticationProtos.AuthenticationService.BlockingInterface service =
          AuthenticationProtos.AuthenticationService.newBlockingStub(rpcChannel);
      AuthenticationProtos.GetAuthenticationTokenResponse response = service.getAuthenticationToken(null,
          AuthenticationProtos.GetAuthenticationTokenRequest.getDefaultInstance());

      return ProtobufUtil.toToken(response.getToken());
    } catch (ServiceException se) {
      ProtobufUtil.toIOException(se);
    } finally {
      if (meta != null) {
        meta.close();
      }
    }
    // dummy return for ServiceException block
    return null;
  }

  /**
   * Obtain and return an authentication token for the current user.
   * @param conn The HBase cluster connection
   * @return the authentication token instance
   */
  public static Token<AuthenticationTokenIdentifier> obtainToken(
      final Connection conn, User user) throws IOException, InterruptedException {
    return user.runAs(new PrivilegedExceptionAction<Token<AuthenticationTokenIdentifier>>() {
      @Override
      public Token<AuthenticationTokenIdentifier> run() throws Exception {
        return obtainToken(conn);
      }
    });
  }


  private static Text getClusterId(Token<AuthenticationTokenIdentifier> token)
      throws IOException {
    return token.getService() != null
        ? token.getService() : new Text("default");
  }

  /**
   * Obtain an authentication token for the given user and add it to the
   * user's credentials.
   * @param conn The HBase cluster connection
   * @param user The user for whom to obtain the token
   * @throws IOException If making a remote call to the authentication service fails
   * @throws InterruptedException If executing as the given user is interrupted
   */
  public static void obtainAndCacheToken(final Connection conn,
      User user)
      throws IOException, InterruptedException {
    try {
      Token<AuthenticationTokenIdentifier> token = obtainToken(conn, user);

      if (token == null) {
        throw new IOException("No token returned for user " + user.getName());
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Obtained token " + token.getKind().toString() + " for user " +
            user.getName());
      }
      user.addToken(token);
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
   * @param job The job instance in which the token should be stored
   * @throws IOException If making a remote call to the authentication service fails
   * @throws InterruptedException If executing as the given user is interrupted
   */
  public static void obtainTokenForJob(final Connection conn,
      User user, Job job)
      throws IOException, InterruptedException {
    try {
      Token<AuthenticationTokenIdentifier> token = obtainToken(conn, user);

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
      Token<AuthenticationTokenIdentifier> token = obtainToken(conn, user);

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

    Token<AuthenticationTokenIdentifier> token = getAuthToken(conn.getConfiguration(), user);
    if (token == null) {
      token = obtainToken(conn, user);
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
    Token<AuthenticationTokenIdentifier> token = getAuthToken(conn.getConfiguration(), user);
    if (token == null) {
      token = obtainToken(conn, user);
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
    Token<AuthenticationTokenIdentifier> token = getAuthToken(conn.getConfiguration(), user);
    if (token == null) {
      token = obtainToken(conn, user);
      user.getUGI().addToken(token.getService(), token);
      return true;
    }
    return false;
  }

  /**
   * Get the authentication token of the user for the cluster specified in the configuration
   * @return null if the user does not have the token, otherwise the auth token for the cluster.
   */
  private static Token<AuthenticationTokenIdentifier> getAuthToken(Configuration conf, User user)
      throws IOException, InterruptedException {
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "TokenUtil-getAuthToken", null);
    try {
      String clusterId = ZKClusterId.readClusterIdZNode(zkw);
      if (clusterId == null) {
        throw new IOException("Failed to get cluster ID");
      }
      return new AuthenticationTokenSelector().selectToken(new Text(clusterId), user.getTokens());
    } catch (KeeperException e) {
      throw new IOException(e);
    } finally {
      zkw.close();
    }
  }
}
