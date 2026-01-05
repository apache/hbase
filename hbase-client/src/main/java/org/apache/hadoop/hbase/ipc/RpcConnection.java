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

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.security.sasl.SaslException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ExtendedCellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.MetricsConnection;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.hbase.security.SecurityConstants;
import org.apache.hadoop.hbase.security.SecurityInfo;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProviders;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hbase.thirdparty.io.netty.util.HashedWheelTimer;
import org.apache.hbase.thirdparty.io.netty.util.Timeout;
import org.apache.hbase.thirdparty.io.netty.util.TimerTask;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ExceptionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ResponseHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.SecurityPreamableResponse;
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

  protected final SecurityInfo securityInfo;

  protected final int reloginMaxBackoff; // max pause before relogin on sasl failure

  protected final Codec codec;

  protected final CompressionCodec compressor;

  protected final CellBlockBuilder cellBlockBuilder;

  protected final MetricsConnection metrics;
  private final Map<String, byte[]> connectionAttributes;

  protected final HashedWheelTimer timeoutTimer;

  protected final Configuration conf;

  protected static String CRYPTO_AES_ENABLED_KEY = "hbase.rpc.crypto.encryption.aes.enabled";

  protected static boolean CRYPTO_AES_ENABLED_DEFAULT = false;

  // the last time we were picked up from connection pool.
  protected long lastTouched;

  protected SaslClientAuthenticationProvider provider;

  // Record the server principal which we have successfully authenticated with the remote server
  // this is used to save the extra round trip with server when there are multiple candidate server
  // principals for a given rpc service, like ClientMetaService.
  // See HBASE-28321 for more details.
  private String lastSucceededServerPrincipal;

  protected RpcConnection(Configuration conf, HashedWheelTimer timeoutTimer, ConnectionId remoteId,
    String clusterId, boolean isSecurityEnabled, Codec codec, CompressionCodec compressor,
    CellBlockBuilder cellBlockBuilder, MetricsConnection metrics,
    Map<String, byte[]> connectionAttributes) throws IOException {
    this.timeoutTimer = timeoutTimer;
    this.codec = codec;
    this.compressor = compressor;
    this.cellBlockBuilder = cellBlockBuilder;
    this.conf = conf;
    this.metrics = metrics;
    this.connectionAttributes = connectionAttributes;
    User ticket = remoteId.getTicket();
    this.securityInfo = SecurityInfo.getInfo(remoteId.getServiceName());
    this.useSasl = isSecurityEnabled;

    // Choose the correct Token and AuthenticationProvider for this client to use
    SaslClientAuthenticationProviders providers =
      SaslClientAuthenticationProviders.getInstance(conf);
    Pair<SaslClientAuthenticationProvider, Token<? extends TokenIdentifier>> pair;
    if (useSasl && securityInfo != null) {
      pair = providers.selectProvider(conf, clusterId, ticket);
      if (pair == null) {
        LOG.trace("Falling back to selectProvider using the cached configuration");
        pair = providers.selectProvider(clusterId, ticket);
      }
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

  protected final void scheduleTimeoutTask(final Call call) {
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

  // will be overridden in tests
  protected byte[] getConnectionHeaderPreamble() {
    // Assemble the preamble up in a buffer first and then send it. Writing individual elements,
    // they are getting sent across piecemeal according to wireshark and then server is messing
    // up the reading on occasion (the passed in stream is not buffered yet).
    int rpcHeaderLen = HConstants.RPC_HEADER.length;
    // Preamble is six bytes -- 'HBas' + VERSION + AUTH_CODE
    byte[] preamble = new byte[rpcHeaderLen + 2];
    System.arraycopy(HConstants.RPC_HEADER, 0, preamble, 0, rpcHeaderLen);
    preamble[rpcHeaderLen] = HConstants.RPC_CURRENT_VERSION;
    synchronized (this) {
      preamble[preamble.length - 1] = provider.getSaslAuthMethod().getCode();
    }
    return preamble;
  }

  protected final ConnectionHeader getConnectionHeader() {
    final ConnectionHeader.Builder builder = ConnectionHeader.newBuilder();
    builder.setServiceName(remoteId.getServiceName());
    final UserInformation userInfoPB = provider.getUserInfo(remoteId.ticket);
    if (userInfoPB != null) {
      builder.setUserInfo(userInfoPB);
    }
    if (this.codec != null) {
      builder.setCellBlockCodecClass(this.codec.getClass().getCanonicalName());
    }
    if (this.compressor != null) {
      builder.setCellBlockCompressorClass(this.compressor.getClass().getCanonicalName());
    }
    if (connectionAttributes != null && !connectionAttributes.isEmpty()) {
      HBaseProtos.NameBytesPair.Builder attributeBuilder = HBaseProtos.NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute : connectionAttributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(UnsafeByteOperations.unsafeWrap(attribute.getValue()));
        builder.addAttribute(attributeBuilder.build());
      }
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

  protected final InetSocketAddress getRemoteInetAddress(MetricsConnection metrics)
    throws UnknownHostException {
    if (metrics != null) {
      metrics.incrNsLookups();
    }
    InetSocketAddress remoteAddr = Address.toSocketAddress(remoteId.getAddress());
    if (remoteAddr.isUnresolved()) {
      if (metrics != null) {
        metrics.incrNsLookupsFailed();
      }
      throw new UnknownHostException(remoteId.getAddress() + " could not be resolved");
    }
    return remoteAddr;
  }

  private static boolean useCanonicalHostname(Configuration conf) {
    return !conf.getBoolean(
      SecurityConstants.UNSAFE_HBASE_CLIENT_KERBEROS_HOSTNAME_DISABLE_REVERSEDNS,
      SecurityConstants.DEFAULT_UNSAFE_HBASE_CLIENT_KERBEROS_HOSTNAME_DISABLE_REVERSEDNS);
  }

  private static String getHostnameForServerPrincipal(Configuration conf, InetAddress addr) {
    final String hostname;
    if (useCanonicalHostname(conf)) {
      hostname = addr.getCanonicalHostName();
      if (hostname.equals(addr.getHostAddress())) {
        LOG.warn("Canonical hostname for SASL principal is the same with IP address: " + hostname
          + ", " + addr.getHostName() + ". Check DNS configuration or consider "
          + SecurityConstants.UNSAFE_HBASE_CLIENT_KERBEROS_HOSTNAME_DISABLE_REVERSEDNS + "=true");
      }
    } else {
      hostname = addr.getHostName();
    }

    return hostname.toLowerCase();
  }

  private static String getServerPrincipal(Configuration conf, String serverKey, InetAddress server)
    throws IOException {
    String hostname = getHostnameForServerPrincipal(conf, server);
    return SecurityUtil.getServerPrincipal(conf.get(serverKey), hostname);
  }

  protected final boolean isKerberosAuth() {
    return provider.getSaslAuthMethod().getCode() == AuthMethod.KERBEROS.code;
  }

  protected final Set<String> getServerPrincipals() throws IOException {
    // for authentication method other than kerberos, we do not need to know the server principal
    if (!isKerberosAuth()) {
      return Collections.singleton(HConstants.EMPTY_STRING);
    }
    // if we have successfully authenticated last time, just return the server principal we use last
    // time
    if (lastSucceededServerPrincipal != null) {
      return Collections.singleton(lastSucceededServerPrincipal);
    }
    InetAddress server =
      new InetSocketAddress(remoteId.address.getHostName(), remoteId.address.getPort())
        .getAddress();
    // Even if we have multiple config key in security info, it is still possible that we configured
    // the same principal for them, so here we use a Set
    Set<String> serverPrincipals = new TreeSet<>();
    for (String serverPrincipalKey : securityInfo.getServerPrincipals()) {
      serverPrincipals.add(getServerPrincipal(conf, serverPrincipalKey, server));
    }
    return serverPrincipals;
  }

  protected final <T> T randomSelect(Collection<T> c) {
    int select = ThreadLocalRandom.current().nextInt(c.size());
    int index = 0;
    for (T t : c) {
      if (index == select) {
        return t;
      }
      index++;
    }
    return null;
  }

  protected final String chooseServerPrincipal(Set<String> candidates, Call securityPreambleCall)
    throws SaslException {
    String principal =
      ((SecurityPreamableResponse) securityPreambleCall.response).getServerPrincipal();
    if (!candidates.contains(principal)) {
      // this means the server returns principal which is not in our candidates, it could be a
      // malicious server, stop connecting
      throw new SaslException(remoteId.address + " tells us to use server principal " + principal
        + " which is not expected, should be one of " + candidates);
    }
    return principal;
  }

  protected final void saslNegotiationDone(String serverPrincipal, boolean succeed) {
    LOG.debug("sasl negotiation done with serverPrincipal = {}, succeed = {}", serverPrincipal,
      succeed);
    if (succeed) {
      this.lastSucceededServerPrincipal = serverPrincipal;
    } else {
      // clear the recorded principal if authentication failed
      this.lastSucceededServerPrincipal = null;
    }
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

  protected final Call createSecurityPreambleCall(RpcCallback<Call> callback) {
    return new Call(-1, null, null, null, SecurityPreamableResponse.getDefaultInstance(), 0, 0,
      Collections.emptyMap(), callback, MetricsConnection.newCallStats());
  }

  private <T extends InputStream & DataInput> void finishCall(ResponseHeader responseHeader, T in,
    Call call) throws IOException {
    Message value;
    if (call.responseDefaultType != null) {
      Message.Builder builder = call.responseDefaultType.newBuilderForType();
      if (!builder.mergeDelimitedFrom(in)) {
        // The javadoc of mergeDelimitedFrom says returning false means the stream reaches EOF
        // before reading any bytes out, so here we need to manually finish create the EOFException
        // and finish the call
        call.setException(new EOFException("EOF while reading response with type: "
          + call.responseDefaultType.getClass().getName()));
        return;
      }
      value = builder.build();
    } else {
      value = null;
    }
    ExtendedCellScanner cellBlockScanner;
    if (responseHeader.hasCellBlockMeta()) {
      int size = responseHeader.getCellBlockMeta().getLength();
      // Maybe we could read directly from the ByteBuf.
      // The problem here is that we do not know when to release it.
      byte[] cellBlock = new byte[size];
      in.readFully(cellBlock);
      cellBlockScanner = cellBlockBuilder.createCellScanner(this.codec, this.compressor, cellBlock);
    } else {
      cellBlockScanner = null;
    }
    call.setResponse(value, cellBlockScanner);
  }

  <T extends InputStream & DataInput> void readResponse(T in, Map<Integer, Call> id2Call,
    Call preambleCall, Consumer<RemoteException> fatalConnectionErrorConsumer) throws IOException {
    int totalSize = in.readInt();
    ResponseHeader responseHeader = ResponseHeader.parseDelimitedFrom(in);
    int id = responseHeader.getCallId();
    if (LOG.isTraceEnabled()) {
      LOG.trace("got response header " + TextFormat.shortDebugString(responseHeader)
        + ", totalSize: " + totalSize + " bytes");
    }
    RemoteException remoteExc;
    if (responseHeader.hasException()) {
      ExceptionResponse exceptionResponse = responseHeader.getException();
      remoteExc = IPCUtil.createRemoteException(exceptionResponse);
      if (IPCUtil.isFatalConnectionException(exceptionResponse)) {
        // Here we will cleanup all calls so do not need to fall back, just return.
        fatalConnectionErrorConsumer.accept(remoteExc);
        if (preambleCall != null) {
          preambleCall.setException(remoteExc);
        }
        return;
      }
    } else {
      remoteExc = null;
    }
    if (id < 0) {
      LOG.debug("process preamble call response with response type {}",
        preambleCall != null
          ? preambleCall.responseDefaultType.getDescriptorForType().getName()
          : "null");
      if (preambleCall == null) {
        // fall through so later we will skip this response
        LOG.warn("Got a negative call id {} but there is no preamble call", id);
      } else {
        if (remoteExc != null) {
          preambleCall.setException(remoteExc);
        } else {
          finishCall(responseHeader, in, preambleCall);
        }
        return;
      }
    }
    Call call = id2Call.remove(id);
    if (call == null) {
      // So we got a response for which we have no corresponding 'call' here on the client-side.
      // We probably timed out waiting, cleaned up all references, and now the server decides
      // to return a response. There is nothing we can do w/ the response at this stage. Clean
      // out the wire of the response so its out of the way and we can get other responses on
      // this connection.
      if (LOG.isDebugEnabled()) {
        int readSoFar = IPCUtil.getTotalSizeWhenWrittenDelimited(responseHeader);
        int whatIsLeftToRead = totalSize - readSoFar;
        LOG.debug("Unknown callId: " + id + ", skipping over this response of " + whatIsLeftToRead
          + " bytes");
      }
      return;
    }
    call.callStats.setResponseSizeBytes(totalSize);
    if (remoteExc != null) {
      call.setException(remoteExc);
      return;
    }
    try {
      finishCall(responseHeader, in, call);
    } catch (IOException e) {
      // As the call has been removed from id2Call map, if we hit an exception here, the
      // exceptionCaught method can not help us finish the call, so here we need to catch the
      // exception and finish it
      call.setException(e);
      // throw the exception out, the upper layer should determine whether this is a critical
      // problem
      throw e;
    }
  }
}
