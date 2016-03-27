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
package org.apache.hadoop.hbase.util;

import static io.netty.handler.timeout.IdleState.READER_IDLE;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Promise;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto.Builder;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.security.SaslRpcServer.QualityOfProtection;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

/**
 * Helper class for adding sasl support for {@link FanOutOneBlockAsyncDFSOutput}.
 */
@InterfaceAudience.Private
public final class FanOutOneBlockAsyncDFSOutputSaslHelper {

  private static final Log LOG = LogFactory.getLog(FanOutOneBlockAsyncDFSOutputSaslHelper.class);

  private FanOutOneBlockAsyncDFSOutputSaslHelper() {
  }

  private static final String SERVER_NAME = "0";
  private static final String PROTOCOL = "hdfs";
  private static final String MECHANISM = "DIGEST-MD5";
  private static final int SASL_TRANSFER_MAGIC_NUMBER = 0xDEADBEEF;
  private static final String NAME_DELIMITER = " ";
  private static final String DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY =
      "dfs.encrypt.data.transfer.cipher.suites";
  private static final String AES_CTR_NOPADDING = "AES/CTR/NoPadding";

  private interface SaslAdaptor {

    SaslPropertiesResolver getSaslPropsResolver(DFSClient client);

    TrustedChannelResolver getTrustedChannelResolver(DFSClient client);

    AtomicBoolean getFallbackToSimpleAuth(DFSClient client);

    DataEncryptionKey createDataEncryptionKey(DFSClient client);
  }

  private static final SaslAdaptor SASL_ADAPTOR;

  private interface CipherHelper {

    List<Object> getCipherOptions(Configuration conf) throws IOException;

    void addCipherOptions(DataTransferEncryptorMessageProto.Builder builder,
        List<Object> cipherOptions);

    Object getCipherOption(DataTransferEncryptorMessageProto proto, boolean isNegotiatedQopPrivacy,
        SaslClient saslClient) throws IOException;

    Object getCipherSuite(Object cipherOption);

    byte[] getInKey(Object cipherOption);

    byte[] getInIv(Object cipherOption);

    byte[] getOutKey(Object cipherOption);

    byte[] getOutIv(Object cipherOption);
  }

  private static final CipherHelper CIPHER_HELPER;

  private static final class CryptoCodec {

    private static final Method CREATE_CODEC;

    private static final Method CREATE_ENCRYPTOR;

    private static final Method CREATE_DECRYPTOR;

    private static final Method INIT_ENCRYPTOR;

    private static final Method INIT_DECRYPTOR;

    private static final Method ENCRYPT;

    private static final Method DECRYPT;

    static {
      Class<?> cryptoCodecClass = null;
      try {
        cryptoCodecClass = Class.forName("org.apache.hadoop.crypto.CryptoCodec");
      } catch (ClassNotFoundException e) {
        LOG.warn("No CryptoCodec class found, should be hadoop 2.5-", e);
      }
      if (cryptoCodecClass != null) {
        Method getInstanceMethod = null;
        for (Method method : cryptoCodecClass.getMethods()) {
          if (method.getName().equals("getInstance") && method.getParameterTypes().length == 2) {
            getInstanceMethod = method;
            break;
          }
        }
        CREATE_CODEC = getInstanceMethod;
        try {
          CREATE_ENCRYPTOR = cryptoCodecClass.getMethod("createEncryptor");
          CREATE_DECRYPTOR = cryptoCodecClass.getMethod("createDecryptor");

          Class<?> encryptorClass = Class.forName("org.apache.hadoop.crypto.Encryptor");
          INIT_ENCRYPTOR = encryptorClass.getMethod("init");
          ENCRYPT = encryptorClass.getMethod("encrypt", ByteBuffer.class, ByteBuffer.class);

          Class<?> decryptorClass = Class.forName("org.apache.hadoop.crypto.Decryptor");
          INIT_DECRYPTOR = decryptorClass.getMethod("init");
          DECRYPT = decryptorClass.getMethod("decrypt", ByteBuffer.class, ByteBuffer.class);
        } catch (NoSuchMethodException | ClassNotFoundException e) {
          throw new Error(e);
        }
      } else {
        LOG.warn("Can not initialize CryptoCodec, should be hadoop 2.5-");
        CREATE_CODEC = null;
        CREATE_ENCRYPTOR = null;
        CREATE_DECRYPTOR = null;
        INIT_ENCRYPTOR = null;
        INIT_DECRYPTOR = null;
        ENCRYPT = null;
        DECRYPT = null;
      }
    }

    private final Object encryptor;

    private final Object decryptor;

    public CryptoCodec(Configuration conf, Object cipherOption) {
      Object codec;
      try {
        codec = CREATE_CODEC.invoke(null, conf, CIPHER_HELPER.getCipherSuite(cipherOption));
        encryptor = CREATE_ENCRYPTOR.invoke(codec);
        byte[] encKey = CIPHER_HELPER.getInKey(cipherOption);
        byte[] encIv = CIPHER_HELPER.getInIv(cipherOption);
        INIT_ENCRYPTOR.invoke(encryptor, encKey, Arrays.copyOf(encIv, encIv.length));

        decryptor = CREATE_DECRYPTOR.invoke(codec);
        byte[] decKey = CIPHER_HELPER.getOutKey(cipherOption);
        byte[] decIv = CIPHER_HELPER.getOutIv(cipherOption);
        INIT_DECRYPTOR.invoke(decryptor, decKey, Arrays.copyOf(decIv, decIv.length));
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }

    public void encrypt(ByteBuffer inBuffer, ByteBuffer outBuffer) {
      try {
        ENCRYPT.invoke(encryptor, inBuffer, outBuffer);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }

    public void decrypt(ByteBuffer inBuffer, ByteBuffer outBuffer) {
      try {
        DECRYPT.invoke(decryptor, inBuffer, outBuffer);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static SaslAdaptor createSaslAdaptor27(Class<?> saslDataTransferClientClass)
      throws NoSuchFieldException, NoSuchMethodException {
    final Field saslPropsResolverField =
        saslDataTransferClientClass.getDeclaredField("saslPropsResolver");
    saslPropsResolverField.setAccessible(true);
    final Field trustedChannelResolverField =
        saslDataTransferClientClass.getDeclaredField("trustedChannelResolver");
    trustedChannelResolverField.setAccessible(true);
    final Field fallbackToSimpleAuthField =
        saslDataTransferClientClass.getDeclaredField("fallbackToSimpleAuth");
    fallbackToSimpleAuthField.setAccessible(true);
    final Method getSaslDataTransferClientMethod =
        DFSClient.class.getMethod("getSaslDataTransferClient");
    final Method newDataEncryptionKeyMethod = DFSClient.class.getMethod("newDataEncryptionKey");
    return new SaslAdaptor() {

      @Override
      public TrustedChannelResolver getTrustedChannelResolver(DFSClient client) {
        try {
          return (TrustedChannelResolver) trustedChannelResolverField
              .get(getSaslDataTransferClientMethod.invoke(client));
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public SaslPropertiesResolver getSaslPropsResolver(DFSClient client) {
        try {
          return (SaslPropertiesResolver) saslPropsResolverField
              .get(getSaslDataTransferClientMethod.invoke(client));
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public AtomicBoolean getFallbackToSimpleAuth(DFSClient client) {
        try {
          return (AtomicBoolean) fallbackToSimpleAuthField.get(getSaslDataTransferClientMethod
              .invoke(client));
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public DataEncryptionKey createDataEncryptionKey(DFSClient client) {
        try {
          return (DataEncryptionKey) newDataEncryptionKeyMethod.invoke(client);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private static SaslAdaptor createSaslAdaptor25() {
    try {
      final Field trustedChannelResolverField =
          DFSClient.class.getDeclaredField("trustedChannelResolver");
      trustedChannelResolverField.setAccessible(true);
      final Method getDataEncryptionKeyMethod = DFSClient.class.getMethod("getDataEncryptionKey");
      return new SaslAdaptor() {

        @Override
        public TrustedChannelResolver getTrustedChannelResolver(DFSClient client) {
          try {
            return (TrustedChannelResolver) trustedChannelResolverField.get(client);
          } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public SaslPropertiesResolver getSaslPropsResolver(DFSClient client) {
          return null;
        }

        @Override
        public AtomicBoolean getFallbackToSimpleAuth(DFSClient client) {
          return null;
        }

        @Override
        public DataEncryptionKey createDataEncryptionKey(DFSClient client) {
          try {
            return (DataEncryptionKey) getDataEncryptionKeyMethod.invoke(client);
          } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
          }
        }
      };
    } catch (NoSuchFieldException | NoSuchMethodException e) {
      throw new Error(e);
    }

  }

  private static SaslAdaptor createSaslAdaptor() {
    Class<?> saslDataTransferClientClass = null;
    try {
      saslDataTransferClientClass =
          Class.forName("org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient");
    } catch (ClassNotFoundException e) {
      LOG.warn("No SaslDataTransferClient class found, should be hadoop 2.5-");
    }
    try {
      return saslDataTransferClientClass != null ? createSaslAdaptor27(saslDataTransferClientClass)
          : createSaslAdaptor25();
    } catch (NoSuchFieldException | NoSuchMethodException e) {
      throw new Error(e);
    }
  }

  private static CipherHelper createCipherHelper25() {
    return new CipherHelper() {

      @Override
      public byte[] getOutKey(Object cipherOption) {
        throw new UnsupportedOperationException();
      }

      @Override
      public byte[] getOutIv(Object cipherOption) {
        throw new UnsupportedOperationException();
      }

      @Override
      public byte[] getInKey(Object cipherOption) {
        throw new UnsupportedOperationException();
      }

      @Override
      public byte[] getInIv(Object cipherOption) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Object getCipherSuite(Object cipherOption) {
        throw new UnsupportedOperationException();
      }

      @Override
      public List<Object> getCipherOptions(Configuration conf) {
        return null;
      }

      @Override
      public Object getCipherOption(DataTransferEncryptorMessageProto proto,
          boolean isNegotiatedQopPrivacy, SaslClient saslClient) {
        return null;
      }

      @Override
      public void addCipherOptions(Builder builder, List<Object> cipherOptions) {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static CipherHelper createCipherHelper27(Class<?> cipherOptionClass)
      throws ClassNotFoundException, NoSuchMethodException {
    @SuppressWarnings("rawtypes")
    Class<? extends Enum> cipherSuiteClass =
        Class.forName("org.apache.hadoop.crypto.CipherSuite").asSubclass(Enum.class);
    @SuppressWarnings("unchecked")
    final Enum<?> aesCipherSuite = Enum.valueOf(cipherSuiteClass, "AES_CTR_NOPADDING");
    final Constructor<?> cipherOptionConstructor =
        cipherOptionClass.getConstructor(cipherSuiteClass);
    final Constructor<?> cipherOptionWithKeyAndIvConstructor =
        cipherOptionClass.getConstructor(cipherSuiteClass, byte[].class, byte[].class,
          byte[].class, byte[].class);

    final Method getCipherSuiteMethod = cipherOptionClass.getMethod("getCipherSuite");
    final Method getInKeyMethod = cipherOptionClass.getMethod("getInKey");
    final Method getInIvMethod = cipherOptionClass.getMethod("getInIv");
    final Method getOutKeyMethod = cipherOptionClass.getMethod("getOutKey");
    final Method getOutIvMethod = cipherOptionClass.getMethod("getOutIv");

    final Method convertCipherOptionsMethod =
        PBHelper.class.getMethod("convertCipherOptions", List.class);
    final Method convertCipherOptionProtosMethod =
        PBHelper.class.getMethod("convertCipherOptionProtos", List.class);
    final Method addAllCipherOptionMethod =
        DataTransferEncryptorMessageProto.Builder.class.getMethod("addAllCipherOption",
          Iterable.class);
    final Method getCipherOptionListMethod =
        DataTransferEncryptorMessageProto.class.getMethod("getCipherOptionList");
    return new CipherHelper() {

      @Override
      public byte[] getOutKey(Object cipherOption) {
        try {
          return (byte[]) getOutKeyMethod.invoke(cipherOption);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public byte[] getOutIv(Object cipherOption) {
        try {
          return (byte[]) getOutIvMethod.invoke(cipherOption);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public byte[] getInKey(Object cipherOption) {
        try {
          return (byte[]) getInKeyMethod.invoke(cipherOption);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public byte[] getInIv(Object cipherOption) {
        try {
          return (byte[]) getInIvMethod.invoke(cipherOption);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public Object getCipherSuite(Object cipherOption) {
        try {
          return getCipherSuiteMethod.invoke(cipherOption);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public List<Object> getCipherOptions(Configuration conf) throws IOException {
        // Negotiate cipher suites if configured. Currently, the only supported
        // cipher suite is AES/CTR/NoPadding, but the protocol allows multiple
        // values for future expansion.
        String cipherSuites = conf.get(DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY);
        if (cipherSuites == null || cipherSuites.isEmpty()) {
          return null;
        }
        if (!cipherSuites.equals(AES_CTR_NOPADDING)) {
          throw new IOException(String.format("Invalid cipher suite, %s=%s",
            DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY, cipherSuites));
        }
        Object option;
        try {
          option = cipherOptionConstructor.newInstance(aesCipherSuite);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
        List<Object> cipherOptions = Lists.newArrayListWithCapacity(1);
        cipherOptions.add(option);
        return cipherOptions;
      }

      private Object unwrap(Object option, SaslClient saslClient) throws IOException {
        byte[] inKey = getInKey(option);
        if (inKey != null) {
          inKey = saslClient.unwrap(inKey, 0, inKey.length);
        }
        byte[] outKey = getOutKey(option);
        if (outKey != null) {
          outKey = saslClient.unwrap(outKey, 0, outKey.length);
        }
        try {
          return cipherOptionWithKeyAndIvConstructor.newInstance(getCipherSuite(option), inKey,
            getInIv(option), outKey, getOutIv(option));
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }

      @SuppressWarnings("unchecked")
      @Override
      public Object getCipherOption(DataTransferEncryptorMessageProto proto,
          boolean isNegotiatedQopPrivacy, SaslClient saslClient) throws IOException {
        List<Object> cipherOptions;
        try {
          cipherOptions =
              (List<Object>) convertCipherOptionProtosMethod.invoke(null,
                getCipherOptionListMethod.invoke(proto));
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
        if (cipherOptions == null || cipherOptions.isEmpty()) {
          return null;
        }
        Object cipherOption = cipherOptions.get(0);
        return isNegotiatedQopPrivacy ? unwrap(cipherOption, saslClient) : cipherOption;
      }

      @Override
      public void addCipherOptions(Builder builder, List<Object> cipherOptions) {
        try {
          addAllCipherOptionMethod.invoke(builder,
            convertCipherOptionsMethod.invoke(null, cipherOptions));
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private static CipherHelper createCipherHelper() {
    Class<?> cipherOptionClass;
    try {
      cipherOptionClass = Class.forName("org.apache.hadoop.crypto.CipherOption");
    } catch (ClassNotFoundException e) {
      LOG.warn("No CipherOption class found, should be hadoop 2.5-");
      return createCipherHelper25();
    }
    try {
      return createCipherHelper27(cipherOptionClass);
    } catch (NoSuchMethodException | ClassNotFoundException e) {
      throw new Error(e);
    }
  }

  static {
    SASL_ADAPTOR = createSaslAdaptor();
    CIPHER_HELPER = createCipherHelper();
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
     * @Param password SASL password
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

    private int step = 0;

    public SaslNegotiateHandler(Configuration conf, String username, char[] password,
        Map<String, String> saslProps, int timeoutMs, Promise<Void> promise) throws SaslException {
      this.conf = conf;
      this.saslProps = saslProps;
      this.saslClient =
          Sasl.createSaslClient(new String[] { MECHANISM }, username, PROTOCOL, SERVER_NAME,
            saslProps, new SaslClientCallbackHandler(username, password));
      this.timeoutMs = timeoutMs;
      this.promise = promise;
    }

    private void sendSaslMessage(ChannelHandlerContext ctx, byte[] payload) throws IOException {
      sendSaslMessage(ctx, payload, null);
    }

    private void sendSaslMessage(ChannelHandlerContext ctx, byte[] payload, List<Object> options)
        throws IOException {
      DataTransferEncryptorMessageProto.Builder builder =
          DataTransferEncryptorMessageProto.newBuilder();
      builder.setStatus(DataTransferEncryptorStatus.SUCCESS);
      if (payload != null) {
        builder.setPayload(ByteString.copyFrom(payload));
      }
      if (options != null) {
        CIPHER_HELPER.addCipherOptions(builder, options);
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
      LOG.debug("Verifying QOP, requested QOP = " + requestedQop + ", negotiated QOP = "
          + negotiatedQop);
      if (!requestedQop.contains(negotiatedQop)) {
        throw new IOException(String.format("SASL handshake completed, but "
            + "channel does not have acceptable quality of protection, "
            + "requested = %s, negotiated = %s", requestedQop, negotiatedQop));
      }
    }

    private boolean useWrap() {
      String qop = (String) saslClient.getNegotiatedProperty(Sasl.QOP);
      return qop != null && !"auth".equalsIgnoreCase(qop);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException {
      if (msg instanceof DataTransferEncryptorMessageProto) {
        DataTransferEncryptorMessageProto proto = (DataTransferEncryptorMessageProto) msg;
        check(proto);
        byte[] challenge = proto.getPayload().toByteArray();
        byte[] response = saslClient.evaluateChallenge(challenge);
        switch (step) {
          case 1: {
            List<Object> cipherOptions = null;
            if (requestedQopContainsPrivacy()) {
              cipherOptions = CIPHER_HELPER.getCipherOptions(conf);
            }
            sendSaslMessage(ctx, response, cipherOptions);
            ctx.flush();
            step++;
            break;
          }
          case 2: {
            assert response == null;
            checkSaslComplete();
            Object cipherOption =
                CIPHER_HELPER.getCipherOption(proto, isNegotiatedQopPrivacy(), saslClient);
            ChannelPipeline p = ctx.pipeline();
            while (p.first() != null) {
              p.removeFirst();
            }
            if (cipherOption != null) {
              CryptoCodec codec = new CryptoCodec(conf, cipherOption);
              p.addLast(new EncryptHandler(codec), new DecryptHandler(codec));
            } else {
              if (useWrap()) {
                p.addLast(new SaslWrapHandler(saslClient), new LengthFieldBasedFrameDecoder(
                    Integer.MAX_VALUE, 0, 4), new SaslUnwrapHandler(saslClient));
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

    private final CryptoCodec codec;

    public DecryptHandler(CryptoCodec codec) {
      this.codec = codec;
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
      ByteBuffer outBuffer = outBuf.nioBuffer();
      codec.decrypt(inBuffer, outBuffer);
      outBuf.writerIndex(inBuf.readableBytes());
      if (release) {
        inBuf.release();
      }
      ctx.fireChannelRead(outBuf);
    }
  }

  private static final class EncryptHandler extends MessageToByteEncoder<ByteBuf> {

    private final CryptoCodec codec;

    public EncryptHandler(CryptoCodec codec) {
      super(false);
      this.codec = codec;
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
      ByteBuffer outBuffer = out.nioBuffer();
      codec.encrypt(inBuffer, outBuffer);
      out.writerIndex(inBuf.readableBytes());
      if (release) {
        inBuf.release();
      }
    }
  }

  private static String getUserNameFromEncryptionKey(DataEncryptionKey encryptionKey) {
    return encryptionKey.keyId + NAME_DELIMITER + encryptionKey.blockPoolId + NAME_DELIMITER
        + new String(Base64.encodeBase64(encryptionKey.nonce, false), Charsets.UTF_8);
  }

  private static char[] encryptionKeyToPassword(byte[] encryptionKey) {
    return new String(Base64.encodeBase64(encryptionKey, false), Charsets.UTF_8).toCharArray();
  }

  private static String buildUsername(Token<BlockTokenIdentifier> blockToken) {
    return new String(Base64.encodeBase64(blockToken.getIdentifier(), false), Charsets.UTF_8);
  }

  private static char[] buildClientPassword(Token<BlockTokenIdentifier> blockToken) {
    return new String(Base64.encodeBase64(blockToken.getPassword(), false), Charsets.UTF_8)
        .toCharArray();
  }

  private static Map<String, String> createSaslPropertiesForEncryption(String encryptionAlgorithm) {
    Map<String, String> saslProps = Maps.newHashMapWithExpectedSize(3);
    saslProps.put(Sasl.QOP, QualityOfProtection.PRIVACY.getSaslQop());
    saslProps.put(Sasl.SERVER_AUTH, "true");
    saslProps.put("com.sun.security.sasl.digest.cipher", encryptionAlgorithm);
    return saslProps;
  }

  private static void doSaslNegotiation(Configuration conf, Channel channel, int timeoutMs,
      String username, char[] password, Map<String, String> saslProps, Promise<Void> saslPromise) {
    try {
      channel.pipeline().addLast(new IdleStateHandler(timeoutMs, 0, 0, TimeUnit.MILLISECONDS),
        new ProtobufVarint32FrameDecoder(),
        new ProtobufDecoder(DataTransferEncryptorMessageProto.getDefaultInstance()),
        new SaslNegotiateHandler(conf, username, password, saslProps, timeoutMs, saslPromise));
    } catch (SaslException e) {
      saslPromise.tryFailure(e);
    }
  }

  static void trySaslNegotiate(Configuration conf, Channel channel, DatanodeInfo dnInfo,
      int timeoutMs, DFSClient client, Token<BlockTokenIdentifier> accessToken,
      Promise<Void> saslPromise) {
    SaslPropertiesResolver saslPropsResolver = SASL_ADAPTOR.getSaslPropsResolver(client);
    TrustedChannelResolver trustedChannelResolver = SASL_ADAPTOR.getTrustedChannelResolver(client);
    AtomicBoolean fallbackToSimpleAuth = SASL_ADAPTOR.getFallbackToSimpleAuth(client);
    InetAddress addr = ((InetSocketAddress) channel.remoteAddress()).getAddress();
    if (trustedChannelResolver.isTrusted() || trustedChannelResolver.isTrusted(addr)) {
      saslPromise.trySuccess(null);
      return;
    }
    DataEncryptionKey encryptionKey;
    try {
      encryptionKey = SASL_ADAPTOR.createDataEncryptionKey(client);
    } catch (Exception e) {
      saslPromise.tryFailure(e);
      return;
    }
    if (encryptionKey != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("SASL client doing encrypted handshake for addr = " + addr + ", datanodeId = "
            + dnInfo);
      }
      doSaslNegotiation(conf, channel, timeoutMs, getUserNameFromEncryptionKey(encryptionKey),
        encryptionKeyToPassword(encryptionKey.encryptionKey),
        createSaslPropertiesForEncryption(encryptionKey.encryptionAlgorithm), saslPromise);
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
        LOG.debug("SASL client doing general handshake for addr = " + addr + ", datanodeId = "
            + dnInfo);
      }
      doSaslNegotiation(conf, channel, timeoutMs, buildUsername(accessToken),
        buildClientPassword(accessToken), saslPropsResolver.getClientProperties(addr), saslPromise);
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

}
