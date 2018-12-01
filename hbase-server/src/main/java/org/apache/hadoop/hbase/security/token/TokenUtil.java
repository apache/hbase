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

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos.AuthenticationService;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos.GetAuthenticationTokenRequest;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos.GetAuthenticationTokenResponse;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.token.Token;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * Utility methods for obtaining authentication tokens.
 */
@InterfaceAudience.Public
public class TokenUtil {
  // This class is referenced indirectly by User out in common; instances are created by reflection
  private static final Logger LOG = LoggerFactory.getLogger(TokenUtil.class);

  // Set in TestTokenUtil via reflection
  private static ServiceException injectedException;

  private static void injectFault() throws ServiceException {
    if (injectedException != null) {
      throw injectedException;
    }
  }

  /**
   * Obtain and return an authentication token for the current user.
   * @param conn The async HBase cluster connection
   * @return the authentication token instance, wrapped by a {@link CompletableFuture}.
   */
  public static CompletableFuture<Token<AuthenticationTokenIdentifier>> obtainToken(
      AsyncConnection conn) {
    CompletableFuture<Token<AuthenticationTokenIdentifier>> future = new CompletableFuture<>();
    if (injectedException != null) {
      future.completeExceptionally(injectedException);
      return future;
    }
    AsyncTable<?> table = conn.getTable(TableName.META_TABLE_NAME);
    table.<AuthenticationService.Interface, GetAuthenticationTokenResponse> coprocessorService(
      AuthenticationProtos.AuthenticationService::newStub,
      (s, c, r) -> s.getAuthenticationToken(c,
        AuthenticationProtos.GetAuthenticationTokenRequest.getDefaultInstance(), r),
      HConstants.EMPTY_START_ROW).whenComplete((resp, error) -> {
        if (error != null) {
          future.completeExceptionally(ProtobufUtil.handleRemoteException(error));
        } else {
          future.complete(toToken(resp.getToken()));
        }
      });
    return future;
  }

  /**
   * Obtain and return an authentication token for the current user.
   * @param conn The HBase cluster connection
   * @throws IOException if a remote error or serialization problem occurs.
   * @return the authentication token instance
   */
  public static Token<AuthenticationTokenIdentifier> obtainToken(Connection conn)
      throws IOException {
    Table meta = null;
    try {
      injectFault();

      meta = conn.getTable(TableName.META_TABLE_NAME);
      CoprocessorRpcChannel rpcChannel = meta.coprocessorService(HConstants.EMPTY_START_ROW);
      AuthenticationProtos.AuthenticationService.BlockingInterface service =
        AuthenticationService.newBlockingStub(rpcChannel);
      GetAuthenticationTokenResponse response =
        service.getAuthenticationToken(null, GetAuthenticationTokenRequest.getDefaultInstance());

      return toToken(response.getToken());
    } catch (ServiceException se) {
      throw ProtobufUtil.handleRemoteException(se);
    } finally {
      if (meta != null) {
        meta.close();
      }
    }
  }


  /**
   * Converts a Token instance (with embedded identifier) to the protobuf representation.
   *
   * @param token the Token instance to copy
   * @return the protobuf Token message
   */
  public static AuthenticationProtos.Token toToken(Token<AuthenticationTokenIdentifier> token) {
    AuthenticationProtos.Token.Builder builder = AuthenticationProtos.Token.newBuilder();
    builder.setIdentifier(ByteString.copyFrom(token.getIdentifier()));
    builder.setPassword(ByteString.copyFrom(token.getPassword()));
    if (token.getService() != null) {
      builder.setService(ByteString.copyFromUtf8(token.getService().toString()));
    }
    return builder.build();
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
    ZKWatcher zkw = new ZKWatcher(conf, "TokenUtil-getAuthToken", null);
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

  /**
   * Converts a protobuf Token message back into a Token instance.
   *
   * @param proto the protobuf Token message
   * @return the Token instance
   */
  public static Token<AuthenticationTokenIdentifier> toToken(AuthenticationProtos.Token proto) {
    return new Token<>(
        proto.hasIdentifier() ? proto.getIdentifier().toByteArray() : null,
        proto.hasPassword() ? proto.getPassword().toByteArray() : null,
        AuthenticationTokenIdentifier.AUTH_TOKEN_TYPE,
        proto.hasService() ? new Text(proto.getService().toStringUtf8()) : null);
  }
}
