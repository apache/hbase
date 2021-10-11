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

import static org.apache.hadoop.hbase.HConstants.RPC_HEADER;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.security.GeneralSecurityException;
import java.util.Objects;
import java.util.Properties;

import org.apache.commons.crypto.cipher.CryptoCipherFactory;
import org.apache.commons.crypto.random.CryptoRandom;
import org.apache.commons.crypto.random.CryptoRandomFactory;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.VersionInfoUtil;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.ByteBufferOutputStream;
import org.apache.hadoop.hbase.io.crypto.aes.CryptoAES;
import org.apache.hadoop.hbase.ipc.RpcServer.CallCleanup;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer;
import org.apache.hadoop.hbase.security.SaslStatus;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.provider.SaslServerAuthenticationProvider;
import org.apache.hadoop.hbase.security.provider.SaslServerAuthenticationProviders;
import org.apache.hadoop.hbase.security.provider.SimpleSaslServerAuthenticationProvider;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteInput;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.VersionInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ResponseHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.UserInformation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;

/** Reads calls from a connection and queues them for handling. */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="VO_VOLATILE_INCREMENT",
    justification="False positive according to http://sourceforge.net/p/findbugs/bugs/1032/")
@InterfaceAudience.Private
abstract class ServerRpcConnection implements Closeable {
  /**  */
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
  protected boolean saslContextEstablished;
  protected boolean skipInitialSaslHandshake;
  private ByteBuffer unwrappedData;
  // When is this set? FindBugs wants to know! Says NP
  private ByteBuffer unwrappedDataLengthBuffer = ByteBuffer.allocate(4);
  protected boolean useSasl;
  protected HBaseSaslRpcServer saslServer;
  protected CryptoAES cryptoAES;
  protected boolean useWrap = false;
  protected boolean useCryptoAesWrap = false;

  // was authentication allowed with a fallback to simple auth
  protected boolean authenticatedWithFallback;

  protected boolean retryImmediatelySupported = false;

  protected User user = null;
  protected UserGroupInformation ugi = null;
  protected SaslServerAuthenticationProviders saslProviders = null;

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
    if (connectionHeader.hasVersionInfo()) {
      return connectionHeader.getVersionInfo();
    }
    return null;
  }

  private String getFatalConnectionString(final int version, final byte authByte) {
    return "serverVersion=" + RpcServer.CURRENT_VERSION +
        ", clientVersion=" + version + ", authMethod=" + authByte +
        // The provider may be null if we failed to parse the header of the request
        ", authName=" + (provider == null ? "unknown" : provider.getSaslAuthMethod().getName()) +
        " from " + toString();
  }

  /**
   * Set up cell block codecs
   * @throws FatalConnectionException
   */
  private void setupCellBlockCodecs(final ConnectionHeader header)
      throws FatalConnectionException {
    // TODO: Plug in other supported decoders.
    if (!header.hasCellBlockCodecClass()) return;
    String className = header.getCellBlockCodecClass();
    if (className == null || className.length() == 0) return;
    try {
      this.codec = (Codec)Class.forName(className).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new UnsupportedCellCodecException(className, e);
    }
    if (!header.hasCellBlockCompressorClass()) return;
    className = header.getCellBlockCompressorClass();
    try {
      this.compressionCodec =
          (CompressionCodec)Class.forName(className).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new UnsupportedCompressionCodecException(className, e);
    }
  }

  /**
   * Set up cipher for rpc encryption with Apache Commons Crypto
   *
   * @throws FatalConnectionException
   */
  private void setupCryptoCipher(final ConnectionHeader header,
      RPCProtos.ConnectionHeaderResponse.Builder chrBuilder)
      throws FatalConnectionException {
    // If simple auth, return
    if (saslServer == null) return;
    // check if rpc encryption with Crypto AES
    String qop = saslServer.getNegotiatedQop();
    boolean isEncryption = SaslUtil.QualityOfProtection.PRIVACY
        .getSaslQop().equalsIgnoreCase(qop);
    boolean isCryptoAesEncryption = isEncryption && this.rpcServer.conf.getBoolean(
        "hbase.rpc.crypto.encryption.aes.enabled", false);
    if (!isCryptoAesEncryption) return;
    if (!header.hasRpcCryptoCipherTransformation()) return;
    String transformation = header.getRpcCryptoCipherTransformation();
    if (transformation == null || transformation.length() == 0) return;
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

    int cipherKeyBits = this.rpcServer.conf.getInt(
        "hbase.rpc.crypto.encryption.aes.cipher.keySizeBits", 128);
    // generate key and iv
    if (cipherKeyBits % 8 != 0) {
      throw new IllegalArgumentException("The AES cipher key size in bits" +
          " should be a multiple of byte");
    }
    int len = cipherKeyBits / 8;
    byte[] inKey = new byte[len];
    byte[] outKey = new byte[len];
    byte[] inIv = new byte[len];
    byte[] outIv = new byte[len];

    try {
      // generate the cipher meta data with SecureRandom
      CryptoRandom secureRandom = CryptoRandomFactory.getCryptoRandom(properties);
      secureRandom.nextBytes(inKey);
      secureRandom.nextBytes(outKey);
      secureRandom.nextBytes(inIv);
      secureRandom.nextBytes(outIv);

      // create CryptoAES for server
      cryptoAES = new CryptoAES(transformation, properties,
          inKey, outKey, inIv, outIv);
      // create SaslCipherMeta and send to client,
      //  for client, the [inKey, outKey], [inIv, outIv] should be reversed
      RPCProtos.CryptoCipherMeta.Builder ccmBuilder = RPCProtos.CryptoCipherMeta.newBuilder();
      ccmBuilder.setTransformation(transformation);
      ccmBuilder.setInIv(getByteString(outIv));
      ccmBuilder.setInKey(getByteString(outKey));
      ccmBuilder.setOutIv(getByteString(inIv));
      ccmBuilder.setOutKey(getByteString(inKey));
      chrBuilder.setCryptoCipherMeta(ccmBuilder);
      useCryptoAesWrap = true;
    } catch (GeneralSecurityException | IOException ex) {
      throw new UnsupportedCryptoException(ex.getMessage(), ex);
    }
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
        UserGroupInformation realUserUgi =
            UserGroupInformation.createRemoteUser(realUser);
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
  protected final void doRawSaslReply(SaslStatus status, Writable rv,
      String errorClass, String error) throws IOException {
    BufferChain bc;
    // In my testing, have noticed that sasl messages are usually
    // in the ballpark of 100-200. That's why the initial capacity is 256.
    try (ByteBufferOutputStream saslResponse = new ByteBufferOutputStream(256);
        DataOutputStream  out = new DataOutputStream(saslResponse)) {
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

  public void saslReadAndProcess(ByteBuff saslToken) throws IOException,
      InterruptedException {
    if (saslContextEstablished) {
      RpcServer.LOG.trace("Read input token of size={} for processing by saslServer.unwrap()",
        saslToken.limit());
      if (!useWrap) {
        processOneRpc(saslToken);
      } else {
        byte[] b = saslToken.hasArray() ? saslToken.array() : saslToken.toBytes();
        byte [] plaintextData;
        if (useCryptoAesWrap) {
          // unwrap with CryptoAES
          plaintextData = cryptoAES.unwrap(b, 0, b.length);
        } else {
          plaintextData = saslServer.unwrap(b, 0, b.length);
        }
        processUnwrappedData(plaintextData);
      }
    } else {
      byte[] replyToken;
      try {
        if (saslServer == null) {
          try {
            saslServer =
              new HBaseSaslRpcServer(provider, rpcServer.saslProps, rpcServer.secretManager);
          } catch (Exception e){
            RpcServer.LOG.error("Error when trying to create instance of HBaseSaslRpcServer "
              + "with sasl provider: " + provider, e);
            throw e;
          }
          RpcServer.LOG.debug("Created SASL server with mechanism={}",
              provider.getSaslAuthMethod().getAuthMethod());
        }
        RpcServer.LOG.debug("Read input token of size={} for processing by saslServer." +
            "evaluateResponse()", saslToken.limit());
        replyToken = saslServer.evaluateResponse(saslToken.hasArray()?
            saslToken.array() : saslToken.toBytes());
      } catch (IOException e) {
        RpcServer.LOG.debug("Failed to execute SASL handshake", e);
        IOException sendToClient = e;
        Throwable cause = e;
        while (cause != null) {
          if (cause instanceof InvalidToken) {
            sendToClient = (InvalidToken) cause;
            break;
          }
          cause = cause.getCause();
        }
        doRawSaslReply(SaslStatus.ERROR, null, sendToClient.getClass().getName(),
          sendToClient.getLocalizedMessage());
        this.rpcServer.metrics.authenticationFailure();
        String clientIP = this.toString();
        // attempting user could be null
        RpcServer.AUDITLOG
            .warn("{} {}: {}", RpcServer.AUTH_FAILED_FOR, clientIP, saslServer.getAttemptingUser());
        throw e;
      }
      if (replyToken != null) {
        if (RpcServer.LOG.isDebugEnabled()) {
          RpcServer.LOG.debug("Will send token of size " + replyToken.length
              + " from saslServer.");
        }
        doRawSaslReply(SaslStatus.SUCCESS, new BytesWritable(replyToken), null,
            null);
      }
      if (saslServer.isComplete()) {
        String qop = saslServer.getNegotiatedQop();
        useWrap = qop != null && !"auth".equalsIgnoreCase(qop);
        ugi = provider.getAuthorizedUgi(saslServer.getAuthorizationID(),
            this.rpcServer.secretManager);
        RpcServer.LOG.debug(
            "SASL server context established. Authenticated client: {}. Negotiated QoP is {}",
            ugi, qop);
        this.rpcServer.metrics.authenticationSuccess();
        RpcServer.AUDITLOG.info(RpcServer.AUTH_SUCCESSFUL_FOR + ugi);
        saslContextEstablished = true;
      }
    }
  }

  private void processUnwrappedData(byte[] inBuf) throws IOException, InterruptedException {
    ReadableByteChannel ch = Channels.newChannel(new ByteArrayInputStream(inBuf));
    // Read all RPCs contained in the inBuf, even partial ones
    while (true) {
      int count;
      if (unwrappedDataLengthBuffer.remaining() > 0) {
        count = this.rpcServer.channelRead(ch, unwrappedDataLengthBuffer);
        if (count <= 0 || unwrappedDataLengthBuffer.remaining() > 0)
          return;
      }

      if (unwrappedData == null) {
        unwrappedDataLengthBuffer.flip();
        int unwrappedDataLength = unwrappedDataLengthBuffer.getInt();

        if (unwrappedDataLength == RpcClient.PING_CALL_ID) {
          if (RpcServer.LOG.isDebugEnabled())
            RpcServer.LOG.debug("Received ping message");
          unwrappedDataLengthBuffer.clear();
          continue; // ping message
        }
        unwrappedData = ByteBuffer.allocate(unwrappedDataLength);
      }

      count = this.rpcServer.channelRead(ch, unwrappedData);
      if (count <= 0 || unwrappedData.remaining() > 0)
        return;

      if (unwrappedData.remaining() == 0) {
        unwrappedDataLengthBuffer.clear();
        unwrappedData.flip();
        processOneRpc(new SingleByteBuff(unwrappedData));
        unwrappedData = null;
      }
    }
  }

  public void processOneRpc(ByteBuff buf) throws IOException,
      InterruptedException {
    if (connectionHeaderRead) {
      processRequest(buf);
    } else {
      processConnectionHeader(buf);
      this.connectionHeaderRead = true;
      if (rpcServer.needAuthorization() && !authorizeConnection()) {
        // Throw FatalConnectionException wrapping ACE so client does right thing and closes
        // down the connection instead of trying to read non-existent retun.
        throw new AccessDeniedException("Connection from " + this + " for service " +
          connectionHeader.getServiceName() + " is unauthorized for user: " + ugi);
      }
      this.user = this.rpcServer.userProvider.create(this.ugi);
    }
  }

  private boolean authorizeConnection() throws IOException {
    try {
      // If auth method is DIGEST, the token was obtained by the
      // real user for the effective user, therefore not required to
      // authorize real user. doAs is allowed only for simple or kerberos
      // authentication
      if (ugi != null && ugi.getRealUser() != null
          && provider.supportsProtocolAuthentication()) {
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

  // Reads the connection header following version
  private void processConnectionHeader(ByteBuff buf) throws IOException {
    if (buf.hasArray()) {
      this.connectionHeader = ConnectionHeader.parseFrom(buf.array());
    } else {
      CodedInputStream cis = UnsafeByteOperations.unsafeWrap(
          new ByteBuffByteInput(buf, 0, buf.limit()), 0, buf.limit()).newCodedInput();
      cis.enableAliasing(true);
      this.connectionHeader = ConnectionHeader.parseFrom(cis);
    }
    String serviceName = connectionHeader.getServiceName();
    if (serviceName == null) throw new EmptyServiceNameException();
    this.service = RpcServer.getService(this.rpcServer.services, serviceName);
    if (this.service == null) throw new UnknownServiceException(serviceName);
    setupCellBlockCodecs(this.connectionHeader);
    RPCProtos.ConnectionHeaderResponse.Builder chrBuilder =
        RPCProtos.ConnectionHeaderResponse.newBuilder();
    setupCryptoCipher(this.connectionHeader, chrBuilder);
    responseConnectionHeader(chrBuilder);
    UserGroupInformation protocolUser = createUser(connectionHeader);
    if (!useSasl) {
      ugi = protocolUser;
      if (ugi != null) {
        ugi.setAuthenticationMethod(AuthenticationMethod.SIMPLE);
      }
      // audit logging for SASL authenticated users happens in saslReadAndProcess()
      if (authenticatedWithFallback) {
        RpcServer.LOG.warn("Allowed fallback to SIMPLE auth for {} connecting from {}",
            ugi, getHostAddress());
      }
    } else {
      // user is authenticated
      ugi.setAuthenticationMethod(provider.getSaslAuthMethod().getAuthMethod());
      //Now we check if this is a proxy user case. If the protocol user is
      //different from the 'user', it is a proxy user scenario. However,
      //this is not allowed if user authenticated with DIGEST.
      if ((protocolUser != null)
          && (!protocolUser.getUserName().equals(ugi.getUserName()))) {
        if (!provider.supportsProtocolAuthentication()) {
          // Not allowed to doAs if token authentication is used
          throw new AccessDeniedException("Authenticated user (" + ugi
              + ") doesn't match what the client claims to be ("
              + protocolUser + ")");
        } else {
          // Effective user can be different from authenticated user
          // for simple auth or kerberos auth
          // The user is the real user. Now we create a proxy user
          UserGroupInformation realUser = ugi;
          ugi = UserGroupInformation.createProxyUser(protocolUser
              .getUserName(), realUser);
          // Now the user is a proxy user, set Authentication method Proxy.
          ugi.setAuthenticationMethod(AuthenticationMethod.PROXY);
        }
      }
    }
    String version;
    if (this.connectionHeader.hasVersionInfo()) {
      // see if this connection will support RetryImmediatelyException
      this.retryImmediatelySupported =
          VersionInfoUtil.hasMinimumVersion(getVersionInfo(), 1, 2);
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
  private void responseConnectionHeader(RPCProtos.ConnectionHeaderResponse.Builder chrBuilder)
      throws FatalConnectionException {
    // Response the connection header if Crypto AES is enabled
    if (!chrBuilder.hasCryptoCipherMeta()) return;
    try {
      byte[] connectionHeaderResBytes = chrBuilder.build().toByteArray();
      // encrypt the Crypto AES cipher meta data with sasl server, and send to client
      byte[] unwrapped = new byte[connectionHeaderResBytes.length + 4];
      Bytes.putBytes(unwrapped, 0, Bytes.toBytes(connectionHeaderResBytes.length), 0, 4);
      Bytes.putBytes(unwrapped, 4, connectionHeaderResBytes, 0, connectionHeaderResBytes.length);
      byte[] wrapped = saslServer.wrap(unwrapped, 0, unwrapped.length);
      BufferChain bc;
      try (ByteBufferOutputStream response = new ByteBufferOutputStream(wrapped.length + 4);
          DataOutputStream out = new DataOutputStream(response)) {
        out.writeInt(wrapped.length);
        out.write(wrapped);
        bc = new BufferChain(response.getByteBuffer());
      }
      doRespond(() -> bc);
    } catch (IOException ex) {
      throw new UnsupportedCryptoException(ex.getMessage(), ex);
    }
  }

  protected abstract void doRespond(RpcResponse resp) throws IOException;

  /**
   * @param buf
   *          Has the request header and the request param and optionally
   *          encoded data buffer all in this one array.
   * @throws IOException
   * @throws InterruptedException
   */
  protected void processRequest(ByteBuff buf) throws IOException,
      InterruptedException {
    long totalRequestSize = buf.limit();
    int offset = 0;
    // Here we read in the header. We avoid having pb
    // do its default 4k allocation for CodedInputStream. We force it to use
    // backing array.
    CodedInputStream cis;
    if (buf.hasArray()) {
      cis = UnsafeByteOperations.unsafeWrap(buf.array(), 0, buf.limit()).newCodedInput();
    } else {
      cis = UnsafeByteOperations
          .unsafeWrap(new ByteBuffByteInput(buf, 0, buf.limit()), 0, buf.limit()).newCodedInput();
    }
    cis.enableAliasing(true);
    int headerSize = cis.readRawVarint32();
    offset = cis.getTotalBytesRead();
    Message.Builder builder = RequestHeader.newBuilder();
    ProtobufUtil.mergeFrom(builder, cis, headerSize);
    RequestHeader header = (RequestHeader) builder.build();
    offset += headerSize;
    int id = header.getCallId();
    if (RpcServer.LOG.isTraceEnabled()) {
      RpcServer.LOG.trace("RequestHeader " + TextFormat.shortDebugString(header)
          + " totalRequestSize: " + totalRequestSize + " bytes");
    }
    // Enforcing the call queue size, this triggers a retry in the client
    // This is a bit late to be doing this check - we have already read in the
    // total request.
    if ((totalRequestSize +
        this.rpcServer.callQueueSizeInBytes.sum()) > this.rpcServer.maxQueueSizeInBytes) {
      final ServerCall<?> callTooBig = createCall(id, this.service, null, null, null, null,
        totalRequestSize, null, 0, this.callCleanup);
      this.rpcServer.metrics.exception(RpcServer.CALL_QUEUE_TOO_BIG_EXCEPTION);
      callTooBig.setResponse(null, null,  RpcServer.CALL_QUEUE_TOO_BIG_EXCEPTION,
        "Call queue is full on " + this.rpcServer.server.getServerName() +
        ", is hbase.ipc.server.max.callqueue.size too small?");
      callTooBig.sendResponseIfReady();
      return;
    }
    MethodDescriptor md = null;
    Message param = null;
    CellScanner cellScanner = null;
    try {
      if (header.hasRequestParam() && header.getRequestParam()) {
        md = this.service.getDescriptorForType().findMethodByName(
            header.getMethodName());
        if (md == null)
          throw new UnsupportedOperationException(header.getMethodName());
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
        String msg = "Invalid request header: "
            + TextFormat.shortDebugString(header)
            + ", should have param set in it";
        RpcServer.LOG.warn(msg);
        throw new DoNotRetryIOException(msg);
      }
      if (header.hasCellBlockMeta()) {
        buf.position(offset);
        ByteBuff dup = buf.duplicate();
        dup.limit(offset + header.getCellBlockMeta().getLength());
        cellScanner = this.rpcServer.cellBlockBuilder.createCellScannerReusingBuffers(
            this.codec, this.compressionCodec, dup);
      }
    } catch (Throwable t) {
      InetSocketAddress address = this.rpcServer.getListenerAddress();
      String msg = (address != null ? address : "(channel closed)")
          + " is unable to read call parameter from client "
          + getHostAddress();
      RpcServer.LOG.warn(msg, t);

      this.rpcServer.metrics.exception(t);

      // probably the hbase hadoop version does not match the running hadoop
      // version
      if (t instanceof LinkageError) {
        t = new DoNotRetryIOException(t);
      }
      // If the method is not present on the server, do not retry.
      if (t instanceof UnsupportedOperationException) {
        t = new DoNotRetryIOException(t);
      }

      ServerCall<?> readParamsFailedCall = createCall(id, this.service, null, null, null, null,
        totalRequestSize, null, 0, this.callCleanup);
      readParamsFailedCall.setResponse(null, null, t, msg + "; " + t.getMessage());
      readParamsFailedCall.sendResponseIfReady();
      return;
    }

    int timeout = 0;
    if (header.hasTimeout() && header.getTimeout() > 0) {
      timeout = Math.max(this.rpcServer.minClientRequestTimeout, header.getTimeout());
    }
    ServerCall<?> call = createCall(id, this.service, md, header, param, cellScanner, totalRequestSize,
      this.addr, timeout, this.callCleanup);

    if (!this.rpcServer.scheduler.dispatch(new CallRunner(this.rpcServer, call))) {
      this.rpcServer.callQueueSizeInBytes.add(-1 * call.getSize());
      this.rpcServer.metrics.exception(RpcServer.CALL_QUEUE_TOO_BIG_EXCEPTION);
      call.setResponse(null, null, RpcServer.CALL_QUEUE_TOO_BIG_EXCEPTION,
        "Call queue is full on " + this.rpcServer.server.getServerName() +
            ", too many items queued ?");
      call.sendResponseIfReady();
    }
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
    SimpleRpcServer.LOG.warn(msg);
    doRespond(getErrorResponse(msg, e));
  }

  protected final boolean processPreamble(ByteBuffer preambleBuffer) throws IOException {
    assert preambleBuffer.remaining() == 6;
    for (int i = 0; i < RPC_HEADER.length; i++) {
      if (RPC_HEADER[i] != preambleBuffer.get()) {
        doBadPreambleHandling(
          "Expected HEADER=" + Bytes.toStringBinary(RPC_HEADER) + " but received HEADER=" +
              Bytes.toStringBinary(preambleBuffer.array(), 0, RPC_HEADER.length) + " from " +
              toString());
        return false;
      }
    }
    int version = preambleBuffer.get() & 0xFF;
    byte authbyte = preambleBuffer.get();

    if (version != SimpleRpcServer.CURRENT_VERSION) {
      String msg = getFatalConnectionString(version, authbyte);
      doBadPreambleHandling(msg, new WrongVersionException(msg));
      return false;
    }
    this.provider = this.saslProviders.selectProvider(authbyte);
    if (this.provider == null) {
      String msg = getFatalConnectionString(version, authbyte);
      doBadPreambleHandling(msg, new BadAuthException(msg));
      return false;
    }
    // TODO this is a wart while simple auth'n doesn't go through sasl.
    if (this.rpcServer.isSecurityEnabled && isSimpleAuthentication()) {
      if (this.rpcServer.allowFallbackToSimpleAuth) {
        this.rpcServer.metrics.authenticationFallback();
        authenticatedWithFallback = true;
      } else {
        AccessDeniedException ae = new AccessDeniedException("Authentication is required");
        doRespond(getErrorResponse(ae.getMessage(), ae));
        return false;
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
    return true;
  }

  boolean isSimpleAuthentication() {
    return Objects.requireNonNull(provider) instanceof SimpleSaslServerAuthenticationProvider;
  }

  public abstract boolean isConnectionOpen();

  public abstract ServerCall<?> createCall(int id, BlockingService service, MethodDescriptor md,
      RequestHeader header, Message param, CellScanner cellScanner, long size,
      InetAddress remoteAddress, int timeout, CallCleanup reqCleanup);

  private static class ByteBuffByteInput extends ByteInput {

    private ByteBuff buf;
    private int offset;
    private int length;

    ByteBuffByteInput(ByteBuff buf, int offset, int length) {
      this.buf = buf;
      this.offset = offset;
      this.length = length;
    }

    @Override
    public byte read(int offset) {
      return this.buf.get(getAbsoluteOffset(offset));
    }

    private int getAbsoluteOffset(int offset) {
      return this.offset + offset;
    }

    @Override
    public int read(int offset, byte[] out, int outOffset, int len) {
      this.buf.get(getAbsoluteOffset(offset), out, outOffset, len);
      return len;
    }

    @Override
    public int read(int offset, ByteBuffer out) {
      int len = out.remaining();
      this.buf.get(out, getAbsoluteOffset(offset), len);
      return len;
    }

    @Override
    public int size() {
      return this.length;
    }
  }
}
