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
package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.security.SecurityInfo;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProviders;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.io.netty.util.HashedWheelTimer;
import org.apache.hbase.thirdparty.io.netty.util.Timeout;
import org.apache.hbase.thirdparty.io.netty.util.TimerTask;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.UserInformation;

/**
 * Base class for ipc connection.
 */
@InterfaceAudience.Private
abstract class RpcConnection {

  private static final Logger LOG = LoggerFactory.getLogger(RpcConnection.class);

  protected final ConnectionId remoteId;

  protected final boolean useSasl;

  protected final Token<? extends TokenIdentifier> token;

  protected final InetAddress serverAddress;

  protected final SecurityInfo securityInfo;

  protected final int reloginMaxBackoff; // max pause before relogin on sasl failure

  protected final Codec codec;

  protected final CompressionCodec compressor;

  protected final HashedWheelTimer timeoutTimer;

  protected final Configuration conf;

  protected static String CRYPTO_AES_ENABLED_KEY = "hbase.rpc.crypto.encryption.aes.enabled";

  protected static boolean CRYPTO_AES_ENABLED_DEFAULT = false;

  // the last time we were picked up from connection pool.
  protected long lastTouched;

  protected SaslClientAuthenticationProvider provider;

  protected RpcConnection(Configuration conf, HashedWheelTimer timeoutTimer, ConnectionId remoteId,
      String clusterId, boolean isSecurityEnabled, Codec codec, CompressionCodec compressor)
      throws IOException {
    if (remoteId.getAddress().isUnresolved()) {
      throw new UnknownHostException("unknown host: " + remoteId.getAddress().getHostName());
    }
    this.serverAddress = remoteId.getAddress().getAddress();
    this.timeoutTimer = timeoutTimer;
    this.codec = codec;
    this.compressor = compressor;
    this.conf = conf;

    User ticket = remoteId.getTicket();
    this.securityInfo = SecurityInfo.getInfo(remoteId.getServiceName());
    this.useSasl = isSecurityEnabled;

    // Choose the correct Token and AuthenticationProvider for this client to use
    SaslClientAuthenticationProviders providers =
        SaslClientAuthenticationProviders.getInstance(conf);
    Pair<SaslClientAuthenticationProvider, Token<? extends TokenIdentifier>> pair;
    if (useSasl && securityInfo != null) {
      pair = providers.selectProvider(clusterId, ticket);
      if (pair == null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Found no valid authentication method from providers={} with tokens={}",
              providers.toString(), ticket.getTokens());
        }
        throw new RuntimeException("Found no valid authentication method from options");
      }
    } else if (!useSasl) {
      // Hack, while SIMPLE doesn't go via SASL.
      pair = providers.getSimpleProvider();
    } else {
      throw new RuntimeException("Could not compute valid client authentication provider");
    }

    this.provider = pair.getFirst();
    this.token = pair.getSecond();

    LOG.debug("Using {} authentication for service={}, sasl={}",
        provider.getSaslAuthMethod().getName(), remoteId.serviceName, useSasl);
    reloginMaxBackoff = conf.getInt("hbase.security.relogin.maxbackoff", 5000);
    this.remoteId = remoteId;
  }

  protected void scheduleTimeoutTask(final Call call) {
    if (call.timeout > 0) {
      call.timeoutTask = timeoutTimer.newTimeout(new TimerTask() {

        @Override
        public void run(Timeout timeout) throws Exception {
          call.setTimeout(new CallTimeoutException(call.toShortString() + ", waitTime="
              + (EnvironmentEdgeManager.currentTime() - call.getStartTime()) + "ms, rpcTimeout="
              + call.timeout + "ms"));
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
      preamble[rpcHeaderLen + 1] = provider.getSaslAuthMethod().getCode();
    }
    return preamble;
  }

  protected ConnectionHeader getConnectionHeader() {
    final ConnectionHeader.Builder builder = ConnectionHeader.newBuilder();
    builder.setServiceName(remoteId.getServiceName());
    final UserInformation userInfoPB  = provider.getUserInfo(remoteId.ticket);
    if (userInfoPB != null) {
      builder.setUserInfo(userInfoPB);
    }
    if (this.codec != null) {
      builder.setCellBlockCodecClass(this.codec.getClass().getCanonicalName());
    }
    if (this.compressor != null) {
      builder.setCellBlockCompressorClass(this.compressor.getClass().getCanonicalName());
    }
    builder.setVersionInfo(ProtobufUtil.getVersionInfo());
    boolean isCryptoAESEnable = conf.getBoolean(CRYPTO_AES_ENABLED_KEY, CRYPTO_AES_ENABLED_DEFAULT);
    // if Crypto AES enable, setup Cipher transformation
    if (isCryptoAESEnable) {
      builder.setRpcCryptoCipherTransformation(
          conf.get("hbase.rpc.crypto.encryption.aes.cipher.transform", "AES/CTR/NoPadding"));
    }
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
