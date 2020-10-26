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
package org.apache.hadoop.hbase.ipc;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.UserInformation;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.hbase.security.SecurityInfo;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hadoop.util.Time;

/**
 * Base class for ipc connection.
 */
@InterfaceAudience.Private
abstract class RpcConnection {

  private static final Log LOG = LogFactory.getLog(RpcConnection.class);

  protected final ConnectionId remoteId;

  protected final AuthMethod authMethod;

  protected final boolean useSasl;

  protected final Token<? extends TokenIdentifier> token;

  protected final String serverPrincipal; // server's krb5 principal name

  protected final int reloginMaxBackoff; // max pause before relogin on sasl failure

  protected final Codec codec;

  protected final CompressionCodec compressor;

  protected final HashedWheelTimer timeoutTimer;

  // the last time we were picked up from connection pool.
  protected long lastTouched;

  // Determines whether we need to do an explicit clearing of kerberos tickets with relogin
  private boolean forceReloginEnabled;
  // Minimum time between two force re-login attempts
  private int minTimeBeforeForceRelogin;

  // Time when last forceful re-login was attempted
  private long lastForceReloginAttempt = -1;

  protected RpcConnection(Configuration conf, HashedWheelTimer timeoutTimer, ConnectionId remoteId,
      String clusterId, boolean isSecurityEnabled, Codec codec, CompressionCodec compressor)
      throws IOException {
    if (remoteId.getAddress().isUnresolved()) {
      throw new UnknownHostException("unknown host: " + remoteId.getAddress().getHostName());
    }
    this.timeoutTimer = timeoutTimer;
    this.codec = codec;
    this.compressor = compressor;

    UserGroupInformation ticket = remoteId.getTicket().getUGI();
    SecurityInfo securityInfo = SecurityInfo.getInfo(remoteId.getServiceName());
    this.useSasl = isSecurityEnabled;
    Token<? extends TokenIdentifier> token = null;
    String serverPrincipal = null;
    if (useSasl && securityInfo != null) {
      AuthenticationProtos.TokenIdentifier.Kind tokenKind = securityInfo.getTokenKind();
      if (tokenKind != null) {
        TokenSelector<? extends TokenIdentifier> tokenSelector = AbstractRpcClient.TOKEN_HANDLERS
            .get(tokenKind);
        if (tokenSelector != null) {
          token = tokenSelector.selectToken(new Text(clusterId), ticket.getTokens());
        } else if (LOG.isDebugEnabled()) {
          LOG.debug("No token selector found for type " + tokenKind);
        }
      }
      String serverKey = securityInfo.getServerPrincipal();
      if (serverKey == null) {
        throw new IOException("Can't obtain server Kerberos config key from SecurityInfo");
      }
      serverPrincipal = SecurityUtil.getServerPrincipal(conf.get(serverKey),
        remoteId.address.getAddress().getCanonicalHostName().toLowerCase());
      if (LOG.isDebugEnabled()) {
        LOG.debug("RPC Server Kerberos principal name for service=" + remoteId.getServiceName()
            + " is " + serverPrincipal);
      }
    }
    this.token = token;
    this.serverPrincipal = serverPrincipal;
    if (!useSasl) {
      authMethod = AuthMethod.SIMPLE;
    } else if (token != null) {
      authMethod = AuthMethod.DIGEST;
    } else {
      authMethod = AuthMethod.KERBEROS;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Use " + authMethod + " authentication for service " + remoteId.serviceName
          + ", sasl=" + useSasl);
    }

    reloginMaxBackoff = conf.getInt(HConstants.HBASE_RELOGIN_MAXBACKOFF, 5000);
    this.remoteId = remoteId;

    forceReloginEnabled = conf.getBoolean(HConstants.HBASE_FORCE_RELOGIN_ENABLED, true);
    // Default minimum time between force relogin attempts is 10 minutes
    this.minTimeBeforeForceRelogin =
        conf.getInt(HConstants.HBASE_MINTIME_BEFORE_FORCE_RELOGIN, 10 * 60 * 1000);
  }

  private UserInformation getUserInfo(UserGroupInformation ugi) {
    if (ugi == null || authMethod == AuthMethod.DIGEST) {
      // Don't send user for token auth
      return null;
    }
    UserInformation.Builder userInfoPB = UserInformation.newBuilder();
    if (authMethod == AuthMethod.KERBEROS) {
      // Send effective user for Kerberos auth
      userInfoPB.setEffectiveUser(ugi.getUserName());
    } else if (authMethod == AuthMethod.SIMPLE) {
      // Send both effective user and real user for simple auth
      userInfoPB.setEffectiveUser(ugi.getUserName());
      if (ugi.getRealUser() != null) {
        userInfoPB.setRealUser(ugi.getRealUser().getUserName());
      }
    }
    return userInfoPB.build();
  }

  protected UserGroupInformation getUGI() {
    UserGroupInformation ticket = remoteId.getTicket().getUGI();
    if (authMethod == AuthMethod.KERBEROS) {
      if (ticket != null && ticket.getRealUser() != null) {
        ticket = ticket.getRealUser();
      }
    }
    return ticket;
  }

  protected boolean shouldAuthenticateOverKrb() throws IOException {
    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    UserGroupInformation realUser = currentUser.getRealUser();
    return authMethod == AuthMethod.KERBEROS && loginUser != null &&
    // Make sure user logged in using Kerberos either keytab or TGT
        loginUser.hasKerberosCredentials() &&
        // relogin only in case it is the login user (e.g. JT)
        // or superuser (like oozie).
        (loginUser.equals(currentUser) || loginUser.equals(realUser));
  }

  protected void relogin() throws IOException {
    if (UserGroupInformation.isLoginKeytabBased()) {
      if (shouldForceRelogin()) {
        LOG.debug(
          "SASL Authentication failure. Attempting a forceful re-login for "
              + UserGroupInformation.getLoginUser().getUserName());
        Method logoutUserFromKeytab;
        Method forceReloginFromKeytab;
        try {
          logoutUserFromKeytab = UserGroupInformation.class.getMethod("logoutUserFromKeytab");
          forceReloginFromKeytab = UserGroupInformation.class.getMethod("forceReloginFromKeytab");
        } catch (NoSuchMethodException e) {
          // This shouldn't happen as we already check for the existence of these methods before
          // entering this block
          throw new RuntimeException("Cannot find forceReloginFromKeytab method in UGI");
        }
        logoutUserFromKeytab.setAccessible(true);
        forceReloginFromKeytab.setAccessible(true);
        try {
          logoutUserFromKeytab.invoke(UserGroupInformation.getLoginUser());
          forceReloginFromKeytab.invoke(UserGroupInformation.getLoginUser());
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
          throw new RuntimeException(e.getCause());
        }
      } else {
        UserGroupInformation.getLoginUser().reloginFromKeytab();
      }
    } else {
      UserGroupInformation.getLoginUser().reloginFromTicketCache();
    }
  }

  private boolean shouldForceRelogin() {
    if (!forceReloginEnabled) {
      return false;
    }
    long now = Time.now();
    // If the last force relogin attempted is less than the configured minimum time, revert to the
    // default relogin method of UGI
    if (lastForceReloginAttempt != -1
        && (now - lastForceReloginAttempt < minTimeBeforeForceRelogin)) {
      LOG.debug("Not attempting to force re-login since the last attempt is less than "
          + minTimeBeforeForceRelogin + " millis");
      return false;
    }
    try {
      // Check if forceRelogin method is available in UGI using reflection
      UserGroupInformation.class.getMethod("forceReloginFromKeytab");
      UserGroupInformation.class.getMethod("logoutUserFromKeytab");
    } catch (NoSuchMethodException e) {
      LOG.debug(
        "forceReloginFromKeytab method not available in UGI. Skipping to attempt force relogin");
      return false;
    }
    lastForceReloginAttempt = now;
    return true;
  }

  protected void scheduleTimeoutTask(final Call call) {
    if (call.timeout > 0) {
      call.timeoutTask = timeoutTimer.newTimeout(new TimerTask() {

        @Override
        public void run(Timeout timeout) throws Exception {
          call.setTimeout(new CallTimeoutException("Call id=" + call.id + ", waitTime="
              + (EnvironmentEdgeManager.currentTime() - call.getStartTime()) + ", rpcTimetout="
              + call.timeout));
          callTimeout(call);
        }
      }, call.timeout, TimeUnit.MILLISECONDS);
    }
  }

  protected byte[] getConnectionHeaderPreamble() {
    // Assemble the preamble up in a buffer first and then send it. Writing individual elements,
    // they are getting sent across piecemeal according to wireshark and then server is messing
    // up the reading on occasion (the passed in stream is not buffered yet).

    // Preamble is six bytes -- 'HBas' + VERSION + AUTH_CODE
    int rpcHeaderLen = HConstants.RPC_HEADER.length;
    byte[] preamble = new byte[rpcHeaderLen + 2];
    System.arraycopy(HConstants.RPC_HEADER, 0, preamble, 0, rpcHeaderLen);
    preamble[rpcHeaderLen] = HConstants.RPC_CURRENT_VERSION;
    synchronized (this) {
      preamble[rpcHeaderLen + 1] = authMethod.code;
    }
    return preamble;
  }

  protected ConnectionHeader getConnectionHeader() {
    ConnectionHeader.Builder builder = ConnectionHeader.newBuilder();
    builder.setServiceName(remoteId.getServiceName());
    UserInformation userInfoPB;
    if ((userInfoPB = getUserInfo(remoteId.ticket.getUGI())) != null) {
      builder.setUserInfo(userInfoPB);
    }
    if (this.codec != null) {
      builder.setCellBlockCodecClass(this.codec.getClass().getCanonicalName());
    }
    if (this.compressor != null) {
      builder.setCellBlockCompressorClass(this.compressor.getClass().getCanonicalName());
    }
    builder.setVersionInfo(ProtobufUtil.getVersionInfo());
    return builder.build();
  }

  protected abstract void callTimeout(Call call);

  public ConnectionId remoteId() {
    return remoteId;
  }

  public long getLastTouched() {
    return lastTouched;
  }

  public void setLastTouched(long lastTouched) {
    this.lastTouched = lastTouched;
  }

  /**
   * Tell the idle connection sweeper whether we could be swept.
   */
  public abstract boolean isActive();

  /**
   * Just close connection. Do not need to remove from connection pool.
   */
  public abstract void shutdown();

  public abstract void sendRequest(Call call, HBaseRpcController hrc) throws IOException;

  /**
   * Does the clean up work after the connection is removed from the connection pool
   */
  public abstract void cleanupConnection();
}
