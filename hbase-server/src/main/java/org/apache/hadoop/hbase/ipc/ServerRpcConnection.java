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

import static org.apache.hadoop.hbase.HConstants.RPC_HEADER;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.commons.crypto.cipher.CryptoCipherFactory;
import org.apache.commons.crypto.random.CryptoRandom;
import org.apache.commons.crypto.random.CryptoRandomFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ExtendedCellScanner;
import org.apache.hadoop.hbase.client.ConnectionRegistryEndpoint;
import org.apache.hadoop.hbase.client.VersionInfoUtil;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.ByteBufferOutputStream;
import org.apache.hadoop.hbase.io.crypto.aes.CryptoAES;
import org.apache.hadoop.hbase.ipc.RpcServer.CallCleanup;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.regionserver.RegionServerAbortedException;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer;
import org.apache.hadoop.hbase.security.SaslStatus;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.provider.SaslServerAuthenticationProvider;
import org.apache.hadoop.hbase.security.provider.SaslServerAuthenticationProviders;
import org.apache.hadoop.hbase.security.provider.SimpleSaslServerAuthenticationProvider;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteInput;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.VersionInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ResponseHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.SecurityPreamableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.UserInformation;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetConnectionRegistryResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.TracingProtos.RPCTInfo;

/** Reads calls from a connection and queues them for handling. */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "VO_VOLATILE_INCREMENT",
    justification = "False positive according to http://sourceforge.net/p/findbugs/bugs/1032/")
@InterfaceAudience.Private
abstract class ServerRpcConnection implements Closeable {

  private static final TextMapGetter<RPCTInfo> getter = new RPCTInfoGetter();

  protected final RpcServer rpcServer;
  // If the connection header has been read or not.
  protected boolean connectionHeaderRead = false;

  protected CallCleanup callCleanup;

  // Cache the remote host & port info so that even if the socket is
  // disconnected, we can say where it used to connect to.
  protected String hostAddress;
  protected int remotePort;
  protected InetAddress addr;
  protected ConnectionHeader connectionHeader;
  protected Map<String, byte[]> connectionAttributes;

  /**
   * Codec the client asked use.
   */
  protected Codec codec;
  /**
   * Compression codec the client asked us use.
   */
  protected CompressionCodec compressionCodec;
  protected BlockingService service;

  protected SaslServerAuthenticationProvider provider;
  protected boolean skipInitialSaslHandshake;
  protected boolean useSasl;
  protected HBaseSaslRpcServer saslServer;

  // was authentication allowed with a fallback to simple auth
  protected boolean authenticatedWithFallback;

  protected boolean retryImmediatelySupported = false;

  protected User user = null;
  protected UserGroupInformation ugi = null;
  protected SaslServerAuthenticationProviders saslProviders = null;
  protected X509Certificate[] clientCertificateChain = null;

  public ServerRpcConnection(RpcServer rpcServer) {
    this.rpcServer = rpcServer;
    this.callCleanup = null;
    this.saslProviders = SaslServerAuthenticationProviders.getInstance(rpcServer.getConf());
  }

  @Override
  public String toString() {
    return getHostAddress() + ":" + remotePort;
  }

  public String getHostAddress() {
    return hostAddress;
  }

  public InetAddress getHostInetAddress() {
    return addr;
  }

  public int getRemotePort() {
    return remotePort;
  }

  public VersionInfo getVersionInfo() {
    if (connectionHeader != null && connectionHeader.hasVersionInfo()) {
      return connectionHeader.getVersionInfo();
    }
    return null;
  }

  private String getFatalConnectionString(final int version, final byte authByte) {
    return "serverVersion=" + RpcServer.CURRENT_VERSION + ", clientVersion=" + version
      + ", authMethod=" + authByte +
      // The provider may be null if we failed to parse the header of the request
      ", authName=" + (provider == null ? "unknown" : provider.getSaslAuthMethod().getName())
      + " from " + toString();
  }

  /**
   * Set up cell block codecs
   */
  private void setupCellBlockCodecs() throws FatalConnectionException {
    // TODO: Plug in other supported decoders.
    if (!connectionHeader.hasCellBlockCodecClass()) {
      return;
    }
    String className = connectionHeader.getCellBlockCodecClass();
    if (className == null || className.length() == 0) {
      return;
    }
    try {
      this.codec = (Codec) Class.forName(className).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new UnsupportedCellCodecException(className, e);
    }
    if (!connectionHeader.hasCellBlockCompressorClass()) {
      return;
    }
    className = connectionHeader.getCellBlockCompressorClass();
    try {
      this.compressionCodec =
        (CompressionCodec) Class.forName(className).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new UnsupportedCompressionCodecException(className, e);
    }
  }

  /**
   * Set up cipher for rpc encryption with Apache Commons Crypto.
   */
  private Pair<RPCProtos.ConnectionHeaderResponse, CryptoAES> setupCryptoCipher()
    throws FatalConnectionException {
    // If simple auth, return
    if (saslServer == null) {
      return null;
    }
    // check if rpc encryption with Crypto AES
    String qop = saslServer.getNegotiatedQop();
    boolean isEncryption = SaslUtil.QualityOfProtection.PRIVACY.getSaslQop().equalsIgnoreCase(qop);
    boolean isCryptoAesEncryption = isEncryption
      && this.rpcServer.conf.getBoolean("hbase.rpc.crypto.encryption.aes.enabled", false);
    if (!isCryptoAesEncryption) {
      return null;
    }
    if (!connectionHeader.hasRpcCryptoCipherTransformation()) {
      return null;
    }
    String transformation = connectionHeader.getRpcCryptoCipherTransformation();
    if (transformation == null || transformation.length() == 0) {
      return null;
    }
    // Negotiates AES based on complete saslServer.
    // The Crypto metadata need to be encrypted and send to client.
    Properties properties = new Properties();
    // the property for SecureRandomFactory
    properties.setProperty(CryptoRandomFactory.CLASSES_KEY,
      this.rpcServer.conf.get("hbase.crypto.sasl.encryption.aes.crypto.random",
        "org.apache.commons.crypto.random.JavaCryptoRandom"));
    // the property for cipher class
    properties.setProperty(CryptoCipherFactory.CLASSES_KEY,
      this.rpcServer.conf.get("hbase.rpc.crypto.encryption.aes.cipher.class",
        "org.apache.commons.crypto.cipher.JceCipher"));

    int cipherKeyBits =
      this.rpcServer.conf.getInt("hbase.rpc.crypto.encryption.aes.cipher.keySizeBits", 128);
    // generate key and iv
    if (cipherKeyBits % 8 != 0) {
      throw new IllegalArgumentException(
        "The AES cipher key size in bits" + " should be a multiple of byte");
    }
    int len = cipherKeyBits / 8;
    byte[] inKey = new byte[len];
    byte[] outKey = new byte[len];
    byte[] inIv = new byte[len];
    byte[] outIv = new byte[len];

    CryptoAES cryptoAES;
    try {
      // generate the cipher meta data with SecureRandom
      CryptoRandom secureRandom = CryptoRandomFactory.getCryptoRandom(properties);
      secureRandom.nextBytes(inKey);
      secureRandom.nextBytes(outKey);
      secureRandom.nextBytes(inIv);
      secureRandom.nextBytes(outIv);

      // create CryptoAES for server
      cryptoAES = new CryptoAES(transformation, properties, inKey, outKey, inIv, outIv);
    } catch (GeneralSecurityException | IOException ex) {
      throw new UnsupportedCryptoException(ex.getMessage(), ex);
    }
    // create SaslCipherMeta and send to client,
    // for client, the [inKey, outKey], [inIv, outIv] should be reversed
    RPCProtos.CryptoCipherMeta.Builder ccmBuilder = RPCProtos.CryptoCipherMeta.newBuilder();
    ccmBuilder.setTransformation(transformation);
    ccmBuilder.setInIv(getByteString(outIv));
    ccmBuilder.setInKey(getByteString(outKey));
    ccmBuilder.setOutIv(getByteString(inIv));
    ccmBuilder.setOutKey(getByteString(inKey));
    RPCProtos.ConnectionHeaderResponse resp =
      RPCProtos.ConnectionHeaderResponse.newBuilder().setCryptoCipherMeta(ccmBuilder).build();
    return Pair.newPair(resp, cryptoAES);
  }

  private ByteString getByteString(byte[] bytes) {
    // return singleton to reduce object allocation
    return (bytes.length == 0) ? ByteString.EMPTY : ByteString.copyFrom(bytes);
  }

  private UserGroupInformation createUser(ConnectionHeader head) {
    UserGroupInformation ugi = null;

    if (!head.hasUserInfo()) {
      return null;
    }
    UserInformation userInfoProto = head.getUserInfo();
    String effectiveUser = null;
    if (userInfoProto.hasEffectiveUser()) {
      effectiveUser = userInfoProto.getEffectiveUser();
    }
    String realUser = null;
    if (userInfoProto.hasRealUser()) {
      realUser = userInfoProto.getRealUser();
    }
    if (effectiveUser != null) {
      if (realUser != null) {
        UserGroupInformation realUserUgi = UserGroupInformation.createRemoteUser(realUser);
        ugi = UserGroupInformation.createProxyUser(effectiveUser, realUserUgi);
      } else {
        ugi = UserGroupInformation.createRemoteUser(effectiveUser);
      }
    }
    return ugi;
  }

  protected final void disposeSasl() {
    if (saslServer != null) {
      saslServer.dispose();
      saslServer = null;
    }
  }

  /**
   * No protobuf encoding of raw sasl messages
   */
  protected final void doRawSaslReply(SaslStatus status, Writable rv, String errorClass,
    String error) throws IOException {
    BufferChain bc;
    // In my testing, have noticed that sasl messages are usually
    // in the ballpark of 100-200. That's why the initial capacity is 256.
    try (ByteBufferOutputStream saslResponse = new ByteBufferOutputStream(256);
      DataOutputStream out = new DataOutputStream(saslResponse)) {
      out.writeInt(status.state); // write status
      if (status == SaslStatus.SUCCESS) {
        rv.write(out);
      } else {
        WritableUtils.writeString(out, errorClass);
        WritableUtils.writeString(out, error);
      }
      bc = new BufferChain(saslResponse.getByteBuffer());
    }
    doRespond(() -> bc);
  }

  HBaseSaslRpcServer getOrCreateSaslServer() throws IOException {
    if (saslServer == null) {
      saslServer = new HBaseSaslRpcServer(provider, rpcServer.saslProps, rpcServer.secretManager);
    }
    return saslServer;
  }

  void finishSaslNegotiation() throws IOException {
    String negotiatedQop = saslServer.getNegotiatedQop();
    SaslUtil.verifyNegotiatedQop(saslServer.getRequestedQop(), negotiatedQop);
    ugi = provider.getAuthorizedUgi(saslServer.getAuthorizationID(), this.rpcServer.secretManager);
    RpcServer.LOG.debug(
      "SASL server context established. Authenticated client: {}. Negotiated QoP is {}", ugi,
      negotiatedQop);
    rpcServer.metrics.authenticationSuccess();
    RpcServer.AUDITLOG.info(RpcServer.AUTH_SUCCESSFUL_FOR + ugi);
  }

  public void processOneRpc(ByteBuff buf) throws IOException, InterruptedException {
    if (connectionHeaderRead) {
      processRequest(buf);
    } else {
      processConnectionHeader(buf);
      callCleanupIfNeeded();
      this.connectionHeaderRead = true;
      this.rpcServer.getRpcCoprocessorHost().preAuthorizeConnection(connectionHeader, addr);
      if (rpcServer.needAuthorization() && !authorizeConnection()) {
        // Throw FatalConnectionException wrapping ACE so client does right thing and closes
        // down the connection instead of trying to read non-existent retun.
        throw new AccessDeniedException("Connection from " + this + " for service "
          + connectionHeader.getServiceName() + " is unauthorized for user: " + ugi);
      }
      this.user = this.rpcServer.userProvider.create(this.ugi);
      this.rpcServer.getRpcCoprocessorHost().postAuthorizeConnection(
        this.user != null ? this.user.getName() : null, this.clientCertificateChain);
    }
  }

  private boolean authorizeConnection() throws IOException {
    try {
      // If auth method is DIGEST, the token was obtained by the
      // real user for the effective user, therefore not required to
      // authorize real user. doAs is allowed only for simple or kerberos
      // authentication
      if (ugi != null && ugi.getRealUser() != null && provider.supportsProtocolAuthentication()) {
        ProxyUsers.authorize(ugi, this.getHostAddress(), this.rpcServer.conf);
      }
      this.rpcServer.authorize(ugi, connectionHeader, getHostInetAddress());
      this.rpcServer.metrics.authorizationSuccess();
    } catch (AuthorizationException ae) {
      if (RpcServer.LOG.isDebugEnabled()) {
        RpcServer.LOG.debug("Connection authorization failed: " + ae.getMessage(), ae);
      }
      this.rpcServer.metrics.authorizationFailure();
      doRespond(getErrorResponse(ae.getMessage(), new AccessDeniedException(ae)));
      return false;
    }
    return true;
  }

  private CodedInputStream createCis(ByteBuff buf) {
    // Here we read in the header. We avoid having pb
    // do its default 4k allocation for CodedInputStream. We force it to use
    // backing array.
    CodedInputStream cis;
    if (buf.hasArray()) {
      cis = UnsafeByteOperations
        .unsafeWrap(buf.array(), buf.arrayOffset() + buf.position(), buf.limit()).newCodedInput();
    } else {
      cis = UnsafeByteOperations.unsafeWrap(new ByteBuffByteInput(buf, buf.limit()), 0, buf.limit())
        .newCodedInput();
    }
    cis.enableAliasing(true);
    return cis;
  }

  // Reads the connection header following version
  private void processConnectionHeader(ByteBuff buf) throws IOException {
    this.connectionHeader = ConnectionHeader.parseFrom(createCis(buf));

    // we want to copy the attributes prior to releasing the buffer so that they don't get corrupted
    // eventually
    if (connectionHeader.getAttributeList().isEmpty()) {
      this.connectionAttributes = Collections.emptyMap();
    } else {
      this.connectionAttributes =
        Maps.newHashMapWithExpectedSize(connectionHeader.getAttributeList().size());
      for (HBaseProtos.NameBytesPair nameBytesPair : connectionHeader.getAttributeList()) {
        this.connectionAttributes.put(nameBytesPair.getName(),
          nameBytesPair.getValue().toByteArray());
      }
    }
    String serviceName = connectionHeader.getServiceName();
    if (serviceName == null) {
      throw new EmptyServiceNameException();
    }
    this.service = RpcServer.getService(this.rpcServer.services, serviceName);
    if (this.service == null) {
      throw new UnknownServiceException(serviceName);
    }
    setupCellBlockCodecs();
    sendConnectionHeaderResponseIfNeeded();
    UserGroupInformation protocolUser = createUser(connectionHeader);
    if (!useSasl) {
      ugi = protocolUser;
      if (ugi != null) {
        ugi.setAuthenticationMethod(AuthenticationMethod.SIMPLE);
      }
      // audit logging for SASL authenticated users happens in saslReadAndProcess()
      if (authenticatedWithFallback) {
        RpcServer.LOG.warn("Allowed fallback to SIMPLE auth for {} connecting from {}", ugi,
          getHostAddress());
      }
    } else {
      // user is authenticated
      ugi.setAuthenticationMethod(provider.getSaslAuthMethod().getAuthMethod());
      // Now we check if this is a proxy user case. If the protocol user is
      // different from the 'user', it is a proxy user scenario. However,
      // this is not allowed if user authenticated with DIGEST.
      if ((protocolUser != null) && (!protocolUser.getUserName().equals(ugi.getUserName()))) {
        if (!provider.supportsProtocolAuthentication()) {
          // Not allowed to doAs if token authentication is used
          throw new AccessDeniedException("Authenticated user (" + ugi
            + ") doesn't match what the client claims to be (" + protocolUser + ")");
        } else {
          // Effective user can be different from authenticated user
          // for simple auth or kerberos auth
          // The user is the real user. Now we create a proxy user
          UserGroupInformation realUser = ugi;
          ugi = UserGroupInformation.createProxyUser(protocolUser.getUserName(), realUser);
          // Now the user is a proxy user, set Authentication method Proxy.
          ugi.setAuthenticationMethod(AuthenticationMethod.PROXY);
        }
      }
    }
    String version;
    if (this.connectionHeader.hasVersionInfo()) {
      // see if this connection will support RetryImmediatelyException
      this.retryImmediatelySupported = VersionInfoUtil.hasMinimumVersion(getVersionInfo(), 1, 2);
      version = this.connectionHeader.getVersionInfo().getVersion();
    } else {
      version = "UNKNOWN";
    }
    RpcServer.AUDITLOG.info("Connection from {}:{}, version={}, sasl={}, ugi={}, service={}",
      this.hostAddress, this.remotePort, version, this.useSasl, this.ugi, serviceName);
  }

  /**
   * Send the response for connection header
   */
  private void sendConnectionHeaderResponseIfNeeded() throws FatalConnectionException {
    Pair<RPCProtos.ConnectionHeaderResponse, CryptoAES> pair = setupCryptoCipher();
    // Response the connection header if Crypto AES is enabled
    if (pair == null) {
      return;
    }
    try {
      int size = pair.getFirst().getSerializedSize();
      BufferChain bc;
      try (ByteBufferOutputStream bbOut = new ByteBufferOutputStream(4 + size);
        DataOutputStream out = new DataOutputStream(bbOut)) {
        out.writeInt(size);
        pair.getFirst().writeTo(out);
        bc = new BufferChain(bbOut.getByteBuffer());
      }
      doRespond(new RpcResponse() {

        @Override
        public BufferChain getResponse() {
          return bc;
        }

        @Override
        public void done() {
          // must switch after sending the connection header response, as the client still uses the
          // original SaslClient to unwrap the data we send back
          saslServer.switchToCryptoAES(pair.getSecond());
        }
      });
    } catch (IOException ex) {
      throw new UnsupportedCryptoException(ex.getMessage(), ex);
    }
  }

  protected abstract void doRespond(RpcResponse resp) throws IOException;

  /**
   * Has the request header and the request param and optionally encoded data buffer all in this one
   * array.
   * <p/>
   * Will be overridden in tests.
   */
  protected void processRequest(ByteBuff buf) throws IOException, InterruptedException {
    long totalRequestSize = buf.limit();
    int offset = 0;
    // Here we read in the header. We avoid having pb
    // do its default 4k allocation for CodedInputStream. We force it to use
    // backing array.
    CodedInputStream cis = createCis(buf);
    int headerSize = cis.readRawVarint32();
    offset = cis.getTotalBytesRead();
    Message.Builder builder = RequestHeader.newBuilder();
    ProtobufUtil.mergeFrom(builder, cis, headerSize);
    RequestHeader header = (RequestHeader) builder.build();
    offset += headerSize;
    Context traceCtx = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
      .extract(Context.current(), header.getTraceInfo(), getter);

    // n.b. Management of this Span instance is a little odd. Most exit paths from this try scope
    // are early-exits due to error cases. There's only one success path, the asynchronous call to
    // RpcScheduler#dispatch. The success path assumes ownership of the span, which is represented
    // by null-ing out the reference in this scope. All other paths end the span. Thus, and in
    // order to avoid accidentally orphaning the span, the call to Span#end happens in a finally
    // block iff the span is non-null.
    Span span = TraceUtil.createRemoteSpan("RpcServer.process", traceCtx);
    try (Scope ignored = span.makeCurrent()) {
      int id = header.getCallId();
      // HBASE-28128 - if server is aborting, don't bother trying to process. It will
      // fail at the handler layer, but worse might result in CallQueueTooBigException if the
      // queue is full but server is not properly processing requests. Better to throw an aborted
      // exception here so that the client can properly react.
      if (rpcServer.server != null && rpcServer.server.isAborted()) {
        RegionServerAbortedException serverIsAborted = new RegionServerAbortedException(
          "Server " + rpcServer.server.getServerName() + " aborting");
        this.rpcServer.metrics.exception(serverIsAborted);
        sendErrorResponseForCall(id, totalRequestSize, span, serverIsAborted.getMessage(),
          serverIsAborted);
        return;
      }

      if (RpcServer.LOG.isTraceEnabled()) {
        RpcServer.LOG.trace("RequestHeader " + TextFormat.shortDebugString(header)
          + " totalRequestSize: " + totalRequestSize + " bytes");
      }
      // Enforcing the call queue size, this triggers a retry in the client
      // This is a bit late to be doing this check - we have already read in the
      // total request.
      if (
        (totalRequestSize + this.rpcServer.callQueueSizeInBytes.sum())
            > this.rpcServer.maxQueueSizeInBytes
      ) {
        this.rpcServer.metrics.exception(RpcServer.CALL_QUEUE_TOO_BIG_EXCEPTION);
        sendErrorResponseForCall(id, totalRequestSize, span,
          "Call queue is full on " + this.rpcServer.server.getServerName()
            + ", is hbase.ipc.server.max.callqueue.size too small?",
          RpcServer.CALL_QUEUE_TOO_BIG_EXCEPTION);
        return;
      }
      MethodDescriptor md = null;
      Message param = null;
      ExtendedCellScanner cellScanner = null;
      try {
        if (header.hasRequestParam() && header.getRequestParam()) {
          md = this.service.getDescriptorForType().findMethodByName(header.getMethodName());
          if (md == null) {
            throw new UnsupportedOperationException(header.getMethodName());
          }
          builder = this.service.getRequestPrototype(md).newBuilderForType();
          cis.resetSizeCounter();
          int paramSize = cis.readRawVarint32();
          offset += cis.getTotalBytesRead();
          if (builder != null) {
            ProtobufUtil.mergeFrom(builder, cis, paramSize);
            param = builder.build();
          }
          offset += paramSize;
        } else {
          // currently header must have request param, so we directly throw
          // exception here
          String msg = "Invalid request header: " + TextFormat.shortDebugString(header)
            + ", should have param set in it";
          RpcServer.LOG.warn(msg);
          throw new DoNotRetryIOException(msg);
        }
        if (header.hasCellBlockMeta()) {
          buf.position(offset);
          ByteBuff dup = buf.duplicate();
          dup.limit(offset + header.getCellBlockMeta().getLength());
          cellScanner = this.rpcServer.cellBlockBuilder.createCellScannerReusingBuffers(this.codec,
            this.compressionCodec, dup);
        }
      } catch (Throwable thrown) {
        InetSocketAddress address = this.rpcServer.getListenerAddress();
        String msg = (address != null ? address : "(channel closed)")
          + " is unable to read call parameter from client " + getHostAddress();
        RpcServer.LOG.warn(msg, thrown);

        this.rpcServer.metrics.exception(thrown);

        final Throwable responseThrowable;
        if (thrown instanceof LinkageError) {
          // probably the hbase hadoop version does not match the running hadoop version
          responseThrowable = new DoNotRetryIOException(thrown);
        } else if (thrown instanceof UnsupportedOperationException) {
          // If the method is not present on the server, do not retry.
          responseThrowable = new DoNotRetryIOException(thrown);
        } else {
          responseThrowable = thrown;
        }

        sendErrorResponseForCall(id, totalRequestSize, span,
          msg + "; " + responseThrowable.getMessage(), responseThrowable);
        return;
      }

      int timeout = 0;
      if (header.hasTimeout() && header.getTimeout() > 0) {
        timeout = Math.max(this.rpcServer.minClientRequestTimeout, header.getTimeout());
      }
      ServerCall<?> call = createCall(id, this.service, md, header, param, cellScanner,
        totalRequestSize, this.addr, timeout, this.callCleanup);

      if (this.rpcServer.scheduler.dispatch(new CallRunner(this.rpcServer, call))) {
        // unset span do that it's not closed in the finally block
        span = null;
      } else {
        this.rpcServer.callQueueSizeInBytes.add(-1 * call.getSize());
        this.rpcServer.metrics.exception(RpcServer.CALL_QUEUE_TOO_BIG_EXCEPTION);
        call.setResponse(null, null, RpcServer.CALL_QUEUE_TOO_BIG_EXCEPTION,
          "Call queue is full on " + this.rpcServer.server.getServerName()
            + ", too many items queued ?");
        TraceUtil.setError(span, RpcServer.CALL_QUEUE_TOO_BIG_EXCEPTION);
        call.sendResponseIfReady();
      }
    } finally {
      if (span != null) {
        span.end();
      }
    }
  }

  private void sendErrorResponseForCall(int id, long totalRequestSize, Span span, String msg,
    Throwable responseThrowable) throws IOException {
    ServerCall<?> failedcall = createCall(id, this.service, null, null, null, null,
      totalRequestSize, null, 0, this.callCleanup);
    failedcall.setResponse(null, null, responseThrowable, msg);
    TraceUtil.setError(span, responseThrowable);
    failedcall.sendResponseIfReady();
  }

  protected final RpcResponse getErrorResponse(String msg, Exception e) throws IOException {
    ResponseHeader.Builder headerBuilder = ResponseHeader.newBuilder().setCallId(-1);
    ServerCall.setExceptionResponse(e, msg, headerBuilder);
    ByteBuffer headerBuf =
      ServerCall.createHeaderAndMessageBytes(null, headerBuilder.build(), 0, null);
    BufferChain buf = new BufferChain(headerBuf);
    return () -> buf;
  }

  private void doBadPreambleHandling(String msg) throws IOException {
    doBadPreambleHandling(msg, new FatalConnectionException(msg));
  }

  private void doBadPreambleHandling(String msg, Exception e) throws IOException {
    RpcServer.LOG.warn(msg, e);
    doRespond(getErrorResponse(msg, e));
  }

  private void doPreambleResponse(Message resp) throws IOException {
    ResponseHeader header = ResponseHeader.newBuilder().setCallId(-1).build();
    ByteBuffer buf = ServerCall.createHeaderAndMessageBytes(resp, header, 0, null);
    BufferChain bufChain = new BufferChain(buf);
    doRespond(() -> bufChain);
  }

  private boolean doConnectionRegistryResponse() throws IOException {
    if (!(rpcServer.server instanceof ConnectionRegistryEndpoint)) {
      // should be in tests or some scenarios where we should not reach here
      return false;
    }
    // on backup masters, this request may be blocked since we need to fetch it from filesystem,
    // but since it is just backup master, it is not a critical problem
    String clusterId = ((ConnectionRegistryEndpoint) rpcServer.server).getClusterId();
    RpcServer.LOG.debug("Response connection registry, clusterId = '{}'", clusterId);
    if (clusterId == null) {
      // should be in tests or some scenarios where we should not reach here
      return false;
    }
    GetConnectionRegistryResponse resp =
      GetConnectionRegistryResponse.newBuilder().setClusterId(clusterId).build();
    doPreambleResponse(resp);
    return true;
  }

  private void doSecurityPreambleResponse() throws IOException {
    if (rpcServer.isSecurityEnabled) {
      SecurityPreamableResponse resp = SecurityPreamableResponse.newBuilder()
        .setServerPrincipal(rpcServer.serverPrincipal).build();
      doPreambleResponse(resp);
    } else {
      // security is not enabled, do not need a principal when connecting, throw a special exception
      // to let client know it should just use simple authentication
      doRespond(getErrorResponse("security is not enabled", new SecurityNotEnabledException()));
    }
  }

  protected final void callCleanupIfNeeded() {
    if (callCleanup != null) {
      callCleanup.run();
      callCleanup = null;
    }
  }

  protected enum PreambleResponse {
    SUCCEED, // successfully processed the rpc preamble header
    CONTINUE, // the preamble header is for other purpose, wait for the rpc preamble header
    CLOSE // close the rpc connection
  }

  protected final PreambleResponse processPreamble(ByteBuffer preambleBuffer) throws IOException {
    assert preambleBuffer.remaining() == 6;
    if (
      ByteBufferUtils.equals(preambleBuffer, preambleBuffer.position(), 6,
        RpcClient.REGISTRY_PREAMBLE_HEADER, 0, 6) && doConnectionRegistryResponse()
    ) {
      return PreambleResponse.CLOSE;
    }
    if (
      ByteBufferUtils.equals(preambleBuffer, preambleBuffer.position(), 6,
        RpcClient.SECURITY_PREAMBLE_HEADER, 0, 6)
    ) {
      doSecurityPreambleResponse();
      return PreambleResponse.CONTINUE;
    }
    if (!ByteBufferUtils.equals(preambleBuffer, preambleBuffer.position(), 4, RPC_HEADER, 0, 4)) {
      doBadPreambleHandling(
        "Expected HEADER=" + Bytes.toStringBinary(RPC_HEADER) + " but received HEADER="
          + Bytes.toStringBinary(
            ByteBufferUtils.toBytes(preambleBuffer, preambleBuffer.position(), RPC_HEADER.length),
            0, RPC_HEADER.length)
          + " from " + toString());
      return PreambleResponse.CLOSE;
    }
    int version = preambleBuffer.get(preambleBuffer.position() + 4) & 0xFF;
    byte authByte = preambleBuffer.get(preambleBuffer.position() + 5);
    if (version != RpcServer.CURRENT_VERSION) {
      String msg = getFatalConnectionString(version, authByte);
      doBadPreambleHandling(msg, new WrongVersionException(msg));
      return PreambleResponse.CLOSE;
    }

    this.provider = this.saslProviders.selectProvider(authByte);
    if (this.provider == null) {
      String msg = getFatalConnectionString(version, authByte);
      doBadPreambleHandling(msg, new BadAuthException(msg));
      return PreambleResponse.CLOSE;
    }
    // TODO this is a wart while simple auth'n doesn't go through sasl.
    if (this.rpcServer.isSecurityEnabled && isSimpleAuthentication()) {
      if (this.rpcServer.allowFallbackToSimpleAuth) {
        this.rpcServer.metrics.authenticationFallback();
        authenticatedWithFallback = true;
      } else {
        AccessDeniedException ae = new AccessDeniedException("Authentication is required");
        doRespond(getErrorResponse(ae.getMessage(), ae));
        return PreambleResponse.CLOSE;
      }
    }
    if (!this.rpcServer.isSecurityEnabled && !isSimpleAuthentication()) {
      doRawSaslReply(SaslStatus.SUCCESS, new IntWritable(SaslUtil.SWITCH_TO_SIMPLE_AUTH), null,
        null);
      provider = saslProviders.getSimpleProvider();
      // client has already sent the initial Sasl message and we
      // should ignore it. Both client and server should fall back
      // to simple auth from now on.
      skipInitialSaslHandshake = true;
    }
    useSasl = !(provider instanceof SimpleSaslServerAuthenticationProvider);
    return PreambleResponse.SUCCEED;
  }

  boolean isSimpleAuthentication() {
    return Objects.requireNonNull(provider) instanceof SimpleSaslServerAuthenticationProvider;
  }

  public abstract boolean isConnectionOpen();

  public abstract ServerCall<?> createCall(int id, BlockingService service, MethodDescriptor md,
    RequestHeader header, Message param, ExtendedCellScanner cellScanner, long size,
    InetAddress remoteAddress, int timeout, CallCleanup reqCleanup);

  private static class ByteBuffByteInput extends ByteInput {

    private ByteBuff buf;
    private int length;

    ByteBuffByteInput(ByteBuff buf, int length) {
      this.buf = buf;
      this.length = length;
    }

    @Override
    public byte read(int offset) {
      return this.buf.get(offset);
    }

    @Override
    public int read(int offset, byte[] out, int outOffset, int len) {
      this.buf.get(offset, out, outOffset, len);
      return len;
    }

    @Override
    public int read(int offset, ByteBuffer out) {
      int len = out.remaining();
      this.buf.get(out, offset, len);
      return len;
    }

    @Override
    public int size() {
      return this.length;
    }
  }
}
