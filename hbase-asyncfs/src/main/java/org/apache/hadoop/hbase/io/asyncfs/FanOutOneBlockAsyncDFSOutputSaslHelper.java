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
package org.apache.hadoop.hbase.io.asyncfs;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY;
import static org.apache.hbase.thirdparty.io.netty.handler.timeout.IdleState.READER_IDLE;

import com.google.protobuf.CodedOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherOption;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.Decryptor;
import org.apache.hadoop.crypto.Encryptor;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProvider.KeyVersion;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.security.SaslRpcServer.QualityOfProtection;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Throwables;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBufOutputStream;
import org.apache.hbase.thirdparty.io.netty.buffer.CompositeByteBuf;
import org.apache.hbase.thirdparty.io.netty.buffer.Unpooled;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelDuplexHandler;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPipeline;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPromise;
import org.apache.hbase.thirdparty.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.hbase.thirdparty.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.hbase.thirdparty.io.netty.handler.codec.MessageToByteEncoder;
import org.apache.hbase.thirdparty.io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.apache.hbase.thirdparty.io.netty.handler.timeout.IdleStateEvent;
import org.apache.hbase.thirdparty.io.netty.handler.timeout.IdleStateHandler;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.Promise;

/**
 * Helper class for adding sasl support for {@link FanOutOneBlockAsyncDFSOutput}.
 */
@InterfaceAudience.Private
public final class FanOutOneBlockAsyncDFSOutputSaslHelper {
  private static final Logger LOG =
      LoggerFactory.getLogger(FanOutOneBlockAsyncDFSOutputSaslHelper.class);

  private FanOutOneBlockAsyncDFSOutputSaslHelper() {
  }

  private static final String SERVER_NAME = "0";
  private static final String PROTOCOL = "hdfs";
  private static final String MECHANISM = "DIGEST-MD5";
  private static final int SASL_TRANSFER_MAGIC_NUMBER = 0xDEADBEEF;
  private static final String NAME_DELIMITER = " ";

  private interface SaslAdaptor {

    TrustedChannelResolver getTrustedChannelResolver(SaslDataTransferClient saslClient);

    SaslPropertiesResolver getSaslPropsResolver(SaslDataTransferClient saslClient);

    AtomicBoolean getFallbackToSimpleAuth(SaslDataTransferClient saslClient);
  }

  private static final SaslAdaptor SASL_ADAPTOR;

  private interface TransparentCryptoHelper {

    Encryptor createEncryptor(Configuration conf, FileEncryptionInfo feInfo, DFSClient client)
        throws IOException;
  }

  private static final TransparentCryptoHelper TRANSPARENT_CRYPTO_HELPER;

  private static SaslAdaptor createSaslAdaptor()
      throws NoSuchFieldException, NoSuchMethodException {
    Field saslPropsResolverField =
        SaslDataTransferClient.class.getDeclaredField("saslPropsResolver");
    saslPropsResolverField.setAccessible(true);
    Field trustedChannelResolverField =
        SaslDataTransferClient.class.getDeclaredField("trustedChannelResolver");
    trustedChannelResolverField.setAccessible(true);
    Field fallbackToSimpleAuthField =
        SaslDataTransferClient.class.getDeclaredField("fallbackToSimpleAuth");
    fallbackToSimpleAuthField.setAccessible(true);
    return new SaslAdaptor() {

      @Override
      public TrustedChannelResolver getTrustedChannelResolver(SaslDataTransferClient saslClient) {
        try {
          return (TrustedChannelResolver) trustedChannelResolverField.get(saslClient);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public SaslPropertiesResolver getSaslPropsResolver(SaslDataTransferClient saslClient) {
        try {
          return (SaslPropertiesResolver) saslPropsResolverField.get(saslClient);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public AtomicBoolean getFallbackToSimpleAuth(SaslDataTransferClient saslClient) {
        try {
          return (AtomicBoolean) fallbackToSimpleAuthField.get(saslClient);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private static TransparentCryptoHelper createTransparentCryptoHelperWithoutHDFS12396()
      throws NoSuchMethodException {
    Method decryptEncryptedDataEncryptionKeyMethod = DFSClient.class
      .getDeclaredMethod("decryptEncryptedDataEncryptionKey", FileEncryptionInfo.class);
    decryptEncryptedDataEncryptionKeyMethod.setAccessible(true);
    return new TransparentCryptoHelper() {

      @Override
      public Encryptor createEncryptor(Configuration conf, FileEncryptionInfo feInfo,
          DFSClient client) throws IOException {
        try {
          KeyVersion decryptedKey =
            (KeyVersion) decryptEncryptedDataEncryptionKeyMethod.invoke(client, feInfo);
          CryptoCodec cryptoCodec = CryptoCodec.getInstance(conf, feInfo.getCipherSuite());
          Encryptor encryptor = cryptoCodec.createEncryptor();
          encryptor.init(decryptedKey.getMaterial(), feInfo.getIV());
          return encryptor;
        } catch (InvocationTargetException e) {
          Throwables.propagateIfPossible(e.getTargetException(), IOException.class);
          throw new RuntimeException(e.getTargetException());
        } catch (GeneralSecurityException e) {
          throw new IOException(e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private static TransparentCryptoHelper createTransparentCryptoHelperWithHDFS12396()
      throws ClassNotFoundException, NoSuchMethodException {
    Class<?> hdfsKMSUtilCls = Class.forName("org.apache.hadoop.hdfs.HdfsKMSUtil");
    Method decryptEncryptedDataEncryptionKeyMethod = hdfsKMSUtilCls.getDeclaredMethod(
      "decryptEncryptedDataEncryptionKey", FileEncryptionInfo.class, KeyProvider.class);
    decryptEncryptedDataEncryptionKeyMethod.setAccessible(true);
    return new TransparentCryptoHelper() {

      @Override
      public Encryptor createEncryptor(Configuration conf, FileEncryptionInfo feInfo,
          DFSClient client) throws IOException {
        try {
          KeyVersion decryptedKey = (KeyVersion) decryptEncryptedDataEncryptionKeyMethod
            .invoke(null, feInfo, client.getKeyProvider());
          CryptoCodec cryptoCodec = CryptoCodec.getInstance(conf, feInfo.getCipherSuite());
          Encryptor encryptor = cryptoCodec.createEncryptor();
          encryptor.init(decryptedKey.getMaterial(), feInfo.getIV());
          return encryptor;
        } catch (InvocationTargetException e) {
          Throwables.propagateIfPossible(e.getTargetException(), IOException.class);
          throw new RuntimeException(e.getTargetException());
        } catch (GeneralSecurityException e) {
          throw new IOException(e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private static TransparentCryptoHelper createTransparentCryptoHelper()
      throws NoSuchMethodException, ClassNotFoundException {
    try {
      return createTransparentCryptoHelperWithoutHDFS12396();
    } catch (NoSuchMethodException e) {
      LOG.debug("No decryptEncryptedDataEncryptionKey method in DFSClient," +
        " should be hadoop version with HDFS-12396", e);
    }
    return createTransparentCryptoHelperWithHDFS12396();
  }

  static {
    try {
      SASL_ADAPTOR = createSaslAdaptor();
      TRANSPARENT_CRYPTO_HELPER = createTransparentCryptoHelper();
    } catch (Exception e) {
      String msg = "Couldn't properly initialize access to HDFS internals. Please "
          + "update your WAL Provider to not make use of the 'asyncfs' provider. See "
          + "HBASE-16110 for more information.";
      LOG.error(msg, e);
      throw new Error(msg, e);
    }
  }

  /**
   * Sets user name and password when asked by the client-side SASL object.
   */
  private static final class SaslClientCallbackHandler implements CallbackHandler {

    private final char[] password;
    private final String userName;

    /**
     * Creates a new SaslClientCallbackHandler.
     * @param userName SASL user name
     * @param password SASL password
     */
    public SaslClientCallbackHandler(String userName, char[] password) {
      this.password = password;
      this.userName = userName;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      RealmCallback rc = null;
      for (Callback callback : callbacks) {
        if (callback instanceof RealmChoiceCallback) {
          continue;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          rc = (RealmCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback, "Unrecognized SASL client callback");
        }
      }
      if (nc != null) {
        nc.setName(userName);
      }
      if (pc != null) {
        pc.setPassword(password);
      }
      if (rc != null) {
        rc.setText(rc.getDefaultText());
      }
    }
  }

  private static final class SaslNegotiateHandler extends ChannelDuplexHandler {

    private final Configuration conf;

    private final Map<String, String> saslProps;

    private final SaslClient saslClient;

    private final int timeoutMs;

    private final Promise<Void> promise;

    private final DFSClient dfsClient;

    private int step = 0;

    public SaslNegotiateHandler(Configuration conf, String username, char[] password,
        Map<String, String> saslProps, int timeoutMs, Promise<Void> promise,
        DFSClient dfsClient) throws SaslException {
      this.conf = conf;
      this.saslProps = saslProps;
      this.saslClient = Sasl.createSaslClient(new String[] { MECHANISM }, username, PROTOCOL,
        SERVER_NAME, saslProps, new SaslClientCallbackHandler(username, password));
      this.timeoutMs = timeoutMs;
      this.promise = promise;
      this.dfsClient = dfsClient;
    }

    private void sendSaslMessage(ChannelHandlerContext ctx, byte[] payload) throws IOException {
      sendSaslMessage(ctx, payload, null);
    }

    private List<CipherOption> getCipherOptions() throws IOException {
      // Negotiate cipher suites if configured. Currently, the only supported
      // cipher suite is AES/CTR/NoPadding, but the protocol allows multiple
      // values for future expansion.
      String cipherSuites = conf.get(DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY);
      if (StringUtils.isBlank(cipherSuites)) {
        return null;
      }
      if (!cipherSuites.equals(CipherSuite.AES_CTR_NOPADDING.getName())) {
        throw new IOException(String.format("Invalid cipher suite, %s=%s",
          DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY, cipherSuites));
      }
      return Collections.singletonList(new CipherOption(CipherSuite.AES_CTR_NOPADDING));
    }

    /**
     * The asyncfs subsystem emulates a HDFS client by sending protobuf messages via netty.
     * After Hadoop 3.3.0, the protobuf classes are relocated to org.apache.hadoop.thirdparty.protobuf.*.
     * Use Reflection to check which ones to use.
     */
    private static class BuilderPayloadSetter {
      private static Method setPayloadMethod;
      private static Constructor<?> constructor;

      /**
       * Create a ByteString from byte array without copying (wrap), and then set it as the payload
       * for the builder.
       *
       * @param builder builder for HDFS DataTransferEncryptorMessage.
       * @param payload byte array of payload.
       * @throws IOException
       */
      static void wrapAndSetPayload(DataTransferEncryptorMessageProto.Builder builder, byte[] payload)
        throws IOException {
        Object byteStringObject;
        try {
          // byteStringObject = new LiteralByteString(payload);
          byteStringObject = constructor.newInstance(payload);
          // builder.setPayload(byteStringObject);
          setPayloadMethod.invoke(builder, constructor.getDeclaringClass().cast(byteStringObject));
        } catch (IllegalAccessException | InstantiationException e) {
          throw new RuntimeException(e);

        } catch (InvocationTargetException e) {
          Throwables.propagateIfPossible(e.getTargetException(), IOException.class);
          throw new RuntimeException(e.getTargetException());
        }
      }

      static {
        Class<?> builderClass = DataTransferEncryptorMessageProto.Builder.class;

        // Try the unrelocated ByteString
        Class<?> byteStringClass = com.google.protobuf.ByteString.class;
        try {
          // See if it can load the relocated ByteString, which comes from hadoop-thirdparty.
          byteStringClass = Class.forName("org.apache.hadoop.thirdparty.protobuf.ByteString");
          LOG.debug("Found relocated ByteString class from hadoop-thirdparty." +
            " Assuming this is Hadoop 3.3.0+.");
        } catch (ClassNotFoundException e) {
          LOG.debug("Did not find relocated ByteString class from hadoop-thirdparty." +
            " Assuming this is below Hadoop 3.3.0", e);
        }

        // LiteralByteString is a package private class in protobuf. Make it accessible.
        Class<?> literalByteStringClass;
        try {
          literalByteStringClass = Class.forName(
            "org.apache.hadoop.thirdparty.protobuf.ByteString$LiteralByteString");
          LOG.debug("Shaded LiteralByteString from hadoop-thirdparty is found.");
        } catch (ClassNotFoundException e) {
          try {
            literalByteStringClass = Class.forName("com.google.protobuf.LiteralByteString");
            LOG.debug("com.google.protobuf.LiteralByteString found.");
          } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
          }
        }

        try {
          constructor = literalByteStringClass.getDeclaredConstructor(byte[].class);
          constructor.setAccessible(true);
        } catch (NoSuchMethodException e) {
          throw new RuntimeException(e);
        }

        try {
          setPayloadMethod = builderClass.getMethod("setPayload", byteStringClass);
        } catch (NoSuchMethodException e) {
          // if either method is not found, we are in big trouble. Abort.
          throw new RuntimeException(e);
        }
      }
    }

    private void sendSaslMessage(ChannelHandlerContext ctx, byte[] payload,
        List<CipherOption> options) throws IOException {
      DataTransferEncryptorMessageProto.Builder builder =
          DataTransferEncryptorMessageProto.newBuilder();
      builder.setStatus(DataTransferEncryptorStatus.SUCCESS);
      if (payload != null) {
        BuilderPayloadSetter.wrapAndSetPayload(builder, payload);
      }
      if (options != null) {
        builder.addAllCipherOption(PBHelperClient.convertCipherOptions(options));
      }
      DataTransferEncryptorMessageProto proto = builder.build();
      int size = proto.getSerializedSize();
      size += CodedOutputStream.computeRawVarint32Size(size);
      ByteBuf buf = ctx.alloc().buffer(size);
      proto.writeDelimitedTo(new ByteBufOutputStream(buf));
      ctx.write(buf);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
      ctx.write(ctx.alloc().buffer(4).writeInt(SASL_TRANSFER_MAGIC_NUMBER));
      sendSaslMessage(ctx, new byte[0]);
      ctx.flush();
      step++;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      saslClient.dispose();
    }

    private void check(DataTransferEncryptorMessageProto proto) throws IOException {
      if (proto.getStatus() == DataTransferEncryptorStatus.ERROR_UNKNOWN_KEY) {
        dfsClient.clearDataEncryptionKey();
        throw new InvalidEncryptionKeyException(proto.getMessage());
      } else if (proto.getStatus() == DataTransferEncryptorStatus.ERROR) {
        throw new IOException(proto.getMessage());
      }
    }

    private String getNegotiatedQop() {
      return (String) saslClient.getNegotiatedProperty(Sasl.QOP);
    }

    private boolean isNegotiatedQopPrivacy() {
      String qop = getNegotiatedQop();
      return qop != null && "auth-conf".equalsIgnoreCase(qop);
    }

    private boolean requestedQopContainsPrivacy() {
      Set<String> requestedQop =
          ImmutableSet.copyOf(Arrays.asList(saslProps.get(Sasl.QOP).split(",")));
      return requestedQop.contains("auth-conf");
    }

    private void checkSaslComplete() throws IOException {
      if (!saslClient.isComplete()) {
        throw new IOException("Failed to complete SASL handshake");
      }
      Set<String> requestedQop =
          ImmutableSet.copyOf(Arrays.asList(saslProps.get(Sasl.QOP).split(",")));
      String negotiatedQop = getNegotiatedQop();
      LOG.debug(
        "Verifying QOP, requested QOP = " + requestedQop + ", negotiated QOP = " + negotiatedQop);
      if (!requestedQop.contains(negotiatedQop)) {
        throw new IOException(String.format("SASL handshake completed, but "
            + "channel does not have acceptable quality of protection, "
            + "requested = %s, negotiated = %s",
          requestedQop, negotiatedQop));
      }
    }

    private boolean useWrap() {
      String qop = (String) saslClient.getNegotiatedProperty(Sasl.QOP);
      return qop != null && !"auth".equalsIgnoreCase(qop);
    }

    private CipherOption unwrap(CipherOption option, SaslClient saslClient) throws IOException {
      byte[] inKey = option.getInKey();
      if (inKey != null) {
        inKey = saslClient.unwrap(inKey, 0, inKey.length);
      }
      byte[] outKey = option.getOutKey();
      if (outKey != null) {
        outKey = saslClient.unwrap(outKey, 0, outKey.length);
      }
      return new CipherOption(option.getCipherSuite(), inKey, option.getInIv(), outKey,
          option.getOutIv());
    }

    private CipherOption getCipherOption(DataTransferEncryptorMessageProto proto,
        boolean isNegotiatedQopPrivacy, SaslClient saslClient) throws IOException {
      List<CipherOption> cipherOptions =
          PBHelperClient.convertCipherOptionProtos(proto.getCipherOptionList());
      if (cipherOptions == null || cipherOptions.isEmpty()) {
        return null;
      }
      CipherOption cipherOption = cipherOptions.get(0);
      return isNegotiatedQopPrivacy ? unwrap(cipherOption, saslClient) : cipherOption;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof DataTransferEncryptorMessageProto) {
        DataTransferEncryptorMessageProto proto = (DataTransferEncryptorMessageProto) msg;
        check(proto);
        byte[] challenge = proto.getPayload().toByteArray();
        byte[] response = saslClient.evaluateChallenge(challenge);
        switch (step) {
          case 1: {
            List<CipherOption> cipherOptions = null;
            if (requestedQopContainsPrivacy()) {
              cipherOptions = getCipherOptions();
            }
            sendSaslMessage(ctx, response, cipherOptions);
            ctx.flush();
            step++;
            break;
          }
          case 2: {
            assert response == null;
            checkSaslComplete();
            CipherOption cipherOption =
                getCipherOption(proto, isNegotiatedQopPrivacy(), saslClient);
            ChannelPipeline p = ctx.pipeline();
            while (p.first() != null) {
              p.removeFirst();
            }
            if (cipherOption != null) {
              CryptoCodec codec = CryptoCodec.getInstance(conf, cipherOption.getCipherSuite());
              p.addLast(new EncryptHandler(codec, cipherOption.getInKey(), cipherOption.getInIv()),
                new DecryptHandler(codec, cipherOption.getOutKey(), cipherOption.getOutIv()));
            } else {
              if (useWrap()) {
                p.addLast(new SaslWrapHandler(saslClient),
                  new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4),
                  new SaslUnwrapHandler(saslClient));
              }
            }
            promise.trySuccess(null);
            break;
          }
          default:
            throw new IllegalArgumentException("Unrecognized negotiation step: " + step);
        }
      } else {
        ctx.fireChannelRead(msg);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      promise.tryFailure(cause);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      if (evt instanceof IdleStateEvent && ((IdleStateEvent) evt).state() == READER_IDLE) {
        promise.tryFailure(new IOException("Timeout(" + timeoutMs + "ms) waiting for response"));
      } else {
        super.userEventTriggered(ctx, evt);
      }
    }
  }

  private static final class SaslUnwrapHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private final SaslClient saslClient;

    public SaslUnwrapHandler(SaslClient saslClient) {
      this.saslClient = saslClient;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      saslClient.dispose();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
      msg.skipBytes(4);
      byte[] b = new byte[msg.readableBytes()];
      msg.readBytes(b);
      ctx.fireChannelRead(Unpooled.wrappedBuffer(saslClient.unwrap(b, 0, b.length)));
    }
  }

  private static final class SaslWrapHandler extends ChannelOutboundHandlerAdapter {

    private final SaslClient saslClient;

    private CompositeByteBuf cBuf;

    public SaslWrapHandler(SaslClient saslClient) {
      this.saslClient = saslClient;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
      cBuf = new CompositeByteBuf(ctx.alloc(), false, Integer.MAX_VALUE);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
        throws Exception {
      if (msg instanceof ByteBuf) {
        ByteBuf buf = (ByteBuf) msg;
        cBuf.addComponent(buf);
        cBuf.writerIndex(cBuf.writerIndex() + buf.readableBytes());
      } else {
        ctx.write(msg);
      }
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
      if (cBuf.isReadable()) {
        byte[] b = new byte[cBuf.readableBytes()];
        cBuf.readBytes(b);
        cBuf.discardReadComponents();
        byte[] wrapped = saslClient.wrap(b, 0, b.length);
        ByteBuf buf = ctx.alloc().ioBuffer(4 + wrapped.length);
        buf.writeInt(wrapped.length);
        buf.writeBytes(wrapped);
        ctx.write(buf);
      }
      ctx.flush();
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
      cBuf.release();
      cBuf = null;
    }
  }

  private static final class DecryptHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private final Decryptor decryptor;

    public DecryptHandler(CryptoCodec codec, byte[] key, byte[] iv)
        throws GeneralSecurityException, IOException {
      this.decryptor = codec.createDecryptor();
      this.decryptor.init(key, Arrays.copyOf(iv, iv.length));
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
      ByteBuf inBuf;
      boolean release = false;
      if (msg.nioBufferCount() == 1) {
        inBuf = msg;
      } else {
        inBuf = ctx.alloc().directBuffer(msg.readableBytes());
        msg.readBytes(inBuf);
        release = true;
      }
      ByteBuffer inBuffer = inBuf.nioBuffer();
      ByteBuf outBuf = ctx.alloc().directBuffer(inBuf.readableBytes());
      ByteBuffer outBuffer = outBuf.nioBuffer(0, inBuf.readableBytes());
      decryptor.decrypt(inBuffer, outBuffer);
      outBuf.writerIndex(inBuf.readableBytes());
      if (release) {
        inBuf.release();
      }
      ctx.fireChannelRead(outBuf);
    }
  }

  private static final class EncryptHandler extends MessageToByteEncoder<ByteBuf> {

    private final Encryptor encryptor;

    public EncryptHandler(CryptoCodec codec, byte[] key, byte[] iv)
        throws GeneralSecurityException, IOException {
      this.encryptor = codec.createEncryptor();
      this.encryptor.init(key, Arrays.copyOf(iv, iv.length));
    }

    @Override
    protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, ByteBuf msg, boolean preferDirect)
        throws Exception {
      if (preferDirect) {
        return ctx.alloc().directBuffer(msg.readableBytes());
      } else {
        return ctx.alloc().buffer(msg.readableBytes());
      }
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out) throws Exception {
      ByteBuf inBuf;
      boolean release = false;
      if (msg.nioBufferCount() == 1) {
        inBuf = msg;
      } else {
        inBuf = ctx.alloc().directBuffer(msg.readableBytes());
        msg.readBytes(inBuf);
        release = true;
      }
      ByteBuffer inBuffer = inBuf.nioBuffer();
      ByteBuffer outBuffer = out.nioBuffer(0, inBuf.readableBytes());
      encryptor.encrypt(inBuffer, outBuffer);
      out.writerIndex(inBuf.readableBytes());
      if (release) {
        inBuf.release();
      }
    }
  }

  private static String getUserNameFromEncryptionKey(DataEncryptionKey encryptionKey) {
    return encryptionKey.keyId + NAME_DELIMITER + encryptionKey.blockPoolId + NAME_DELIMITER
        + Base64.getEncoder().encodeToString(encryptionKey.nonce);
  }

  private static char[] encryptionKeyToPassword(byte[] encryptionKey) {
    return Base64.getEncoder().encodeToString(encryptionKey).toCharArray();
  }

  private static String buildUsername(Token<BlockTokenIdentifier> blockToken) {
    return Base64.getEncoder().encodeToString(blockToken.getIdentifier());
  }

  private static char[] buildClientPassword(Token<BlockTokenIdentifier> blockToken) {
    return Base64.getEncoder().encodeToString(blockToken.getPassword()).toCharArray();
  }

  private static Map<String, String> createSaslPropertiesForEncryption(String encryptionAlgorithm) {
    Map<String, String> saslProps = Maps.newHashMapWithExpectedSize(3);
    saslProps.put(Sasl.QOP, QualityOfProtection.PRIVACY.getSaslQop());
    saslProps.put(Sasl.SERVER_AUTH, "true");
    saslProps.put("com.sun.security.sasl.digest.cipher", encryptionAlgorithm);
    return saslProps;
  }

  private static void doSaslNegotiation(Configuration conf, Channel channel, int timeoutMs,
      String username, char[] password, Map<String, String> saslProps, Promise<Void> saslPromise,
      DFSClient dfsClient) {
    try {
      channel.pipeline().addLast(new IdleStateHandler(timeoutMs, 0, 0, TimeUnit.MILLISECONDS),
        new ProtobufVarint32FrameDecoder(),
        new ProtobufDecoder(DataTransferEncryptorMessageProto.getDefaultInstance()),
        new SaslNegotiateHandler(conf, username, password, saslProps, timeoutMs, saslPromise,
            dfsClient));
    } catch (SaslException e) {
      saslPromise.tryFailure(e);
    }
  }

  static void trySaslNegotiate(Configuration conf, Channel channel, DatanodeInfo dnInfo,
      int timeoutMs, DFSClient client, Token<BlockTokenIdentifier> accessToken,
      Promise<Void> saslPromise) throws IOException {
    SaslDataTransferClient saslClient = client.getSaslDataTransferClient();
    SaslPropertiesResolver saslPropsResolver = SASL_ADAPTOR.getSaslPropsResolver(saslClient);
    TrustedChannelResolver trustedChannelResolver =
        SASL_ADAPTOR.getTrustedChannelResolver(saslClient);
    AtomicBoolean fallbackToSimpleAuth = SASL_ADAPTOR.getFallbackToSimpleAuth(saslClient);
    InetAddress addr = ((InetSocketAddress) channel.remoteAddress()).getAddress();
    if (trustedChannelResolver.isTrusted() || trustedChannelResolver.isTrusted(addr)) {
      saslPromise.trySuccess(null);
      return;
    }
    DataEncryptionKey encryptionKey = client.newDataEncryptionKey();
    if (encryptionKey != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "SASL client doing encrypted handshake for addr = " + addr + ", datanodeId = " + dnInfo);
      }
      doSaslNegotiation(conf, channel, timeoutMs, getUserNameFromEncryptionKey(encryptionKey),
        encryptionKeyToPassword(encryptionKey.encryptionKey),
        createSaslPropertiesForEncryption(encryptionKey.encryptionAlgorithm), saslPromise,
          client);
    } else if (!UserGroupInformation.isSecurityEnabled()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("SASL client skipping handshake in unsecured configuration for addr = " + addr
            + ", datanodeId = " + dnInfo);
      }
      saslPromise.trySuccess(null);
    } else if (dnInfo.getXferPort() < 1024) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("SASL client skipping handshake in secured configuration with "
            + "privileged port for addr = " + addr + ", datanodeId = " + dnInfo);
      }
      saslPromise.trySuccess(null);
    } else if (fallbackToSimpleAuth != null && fallbackToSimpleAuth.get()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("SASL client skipping handshake in secured configuration with "
            + "unsecured cluster for addr = " + addr + ", datanodeId = " + dnInfo);
      }
      saslPromise.trySuccess(null);
    } else if (saslPropsResolver != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "SASL client doing general handshake for addr = " + addr + ", datanodeId = " + dnInfo);
      }
      doSaslNegotiation(conf, channel, timeoutMs, buildUsername(accessToken),
        buildClientPassword(accessToken), saslPropsResolver.getClientProperties(addr), saslPromise,
          client);
    } else {
      // It's a secured cluster using non-privileged ports, but no SASL. The only way this can
      // happen is if the DataNode has ignore.secure.ports.for.testing configured, so this is a rare
      // edge case.
      if (LOG.isDebugEnabled()) {
        LOG.debug("SASL client skipping handshake in secured configuration with no SASL "
            + "protection configured for addr = " + addr + ", datanodeId = " + dnInfo);
      }
      saslPromise.trySuccess(null);
    }
  }

  static Encryptor createEncryptor(Configuration conf, HdfsFileStatus stat, DFSClient client)
      throws IOException {
    FileEncryptionInfo feInfo = stat.getFileEncryptionInfo();
    if (feInfo == null) {
      return null;
    }
    return TRANSPARENT_CRYPTO_HELPER.createEncryptor(conf, feInfo, client);
  }
}
