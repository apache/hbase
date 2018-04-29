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

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;
import static org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputSaslHelper.createEncryptor;
import static org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputSaslHelper.trySaslNegotiate;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT;
import static org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage.PIPELINE_SETUP_CREATE;
import static org.apache.hbase.thirdparty.io.netty.channel.ChannelOption.CONNECT_TIMEOUT_MILLIS;
import static org.apache.hbase.thirdparty.io.netty.handler.timeout.IdleState.READER_IDLE;

import com.google.protobuf.CodedOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.crypto.Encryptor;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemLinkResolver;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BaseHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.CachingStrategyProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientOperationHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.PipelineAckProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ExtendedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageTypeProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Throwables;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.io.netty.bootstrap.Bootstrap;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBufAllocator;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBufOutputStream;
import org.apache.hbase.thirdparty.io.netty.buffer.PooledByteBufAllocator;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelFuture;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelFutureListener;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandler;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelInitializer;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPipeline;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoop;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.hbase.thirdparty.io.netty.handler.codec.protobuf.ProtobufDecoder;
import org.apache.hbase.thirdparty.io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.apache.hbase.thirdparty.io.netty.handler.timeout.IdleStateEvent;
import org.apache.hbase.thirdparty.io.netty.handler.timeout.IdleStateHandler;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.Future;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.FutureListener;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.Promise;

/**
 * Helper class for implementing {@link FanOutOneBlockAsyncDFSOutput}.
 */
@InterfaceAudience.Private
public final class FanOutOneBlockAsyncDFSOutputHelper {
  private static final Logger LOG =
      LoggerFactory.getLogger(FanOutOneBlockAsyncDFSOutputHelper.class);

  private FanOutOneBlockAsyncDFSOutputHelper() {
  }

  public static final String ASYNC_DFS_OUTPUT_CREATE_MAX_RETRIES = "hbase.fs.async.create.retries";

  public static final int DEFAULT_ASYNC_DFS_OUTPUT_CREATE_MAX_RETRIES = 10;
  // use pooled allocator for performance.
  private static final ByteBufAllocator ALLOC = PooledByteBufAllocator.DEFAULT;

  // copied from DFSPacket since it is package private.
  public static final long HEART_BEAT_SEQNO = -1L;

  // Timeouts for communicating with DataNode for streaming writes/reads
  public static final int READ_TIMEOUT = 60 * 1000;

  private static final DatanodeInfo[] EMPTY_DN_ARRAY = new DatanodeInfo[0];

  // helper class for getting Status from PipelineAckProto. In hadoop 2.6 or before, there is a
  // getStatus method, and for hadoop 2.7 or after, the status is retrieved from flag. The flag may
  // get from proto directly, or combined by the reply field of the proto and a ECN object. See
  // createPipelineAckStatusGetter for more details.
  private interface PipelineAckStatusGetter {
    Status get(PipelineAckProto ack);
  }

  private static final PipelineAckStatusGetter PIPELINE_ACK_STATUS_GETTER;

  // StorageType enum is placed under o.a.h.hdfs in hadoop 2.6 and o.a.h.fs in hadoop 2.7. So here
  // we need to use reflection to set it.See createStorageTypeSetter for more details.
  private interface StorageTypeSetter {
    OpWriteBlockProto.Builder set(OpWriteBlockProto.Builder builder, Enum<?> storageType);
  }

  private static final StorageTypeSetter STORAGE_TYPE_SETTER;

  // helper class for calling add block method on namenode. There is a addBlockFlags parameter for
  // hadoop 2.8 or later. See createBlockAdder for more details.
  private interface BlockAdder {

    LocatedBlock addBlock(ClientProtocol namenode, String src, String clientName,
        ExtendedBlock previous, DatanodeInfo[] excludeNodes, long fileId, String[] favoredNodes)
        throws IOException;
  }

  private static final BlockAdder BLOCK_ADDER;

  private interface LeaseManager {

    void begin(DFSClient client, long inodeId);

    void end(DFSClient client, long inodeId);
  }

  private static final LeaseManager LEASE_MANAGER;

  // This is used to terminate a recoverFileLease call when FileSystem is already closed.
  // isClientRunning is not public so we need to use reflection.
  private interface DFSClientAdaptor {

    boolean isClientRunning(DFSClient client);
  }

  private static final DFSClientAdaptor DFS_CLIENT_ADAPTOR;

  // helper class for convert protos.
  private interface PBHelper {

    ExtendedBlockProto convert(ExtendedBlock b);

    TokenProto convert(Token<?> tok);
  }

  private static final PBHelper PB_HELPER;

  // helper class for creating data checksum.
  private interface ChecksumCreater {
    DataChecksum createChecksum(DFSClient client);
  }

  private static final ChecksumCreater CHECKSUM_CREATER;

  // helper class for creating files.
  private interface FileCreator {
    default HdfsFileStatus create(ClientProtocol instance, String src, FsPermission masked,
        String clientName, EnumSetWritable<CreateFlag> flag, boolean createParent,
        short replication, long blockSize, CryptoProtocolVersion[] supportedVersions)
        throws Exception {
      try {
        return (HdfsFileStatus) createObject(instance, src, masked, clientName, flag, createParent,
          replication, blockSize, supportedVersions);
      } catch (InvocationTargetException e) {
        if (e.getCause() instanceof Exception) {
          throw (Exception) e.getCause();
        } else {
          throw new RuntimeException(e.getCause());
        }
      }
    }

    Object createObject(ClientProtocol instance, String src, FsPermission masked, String clientName,
        EnumSetWritable<CreateFlag> flag, boolean createParent, short replication, long blockSize,
        CryptoProtocolVersion[] supportedVersions) throws Exception;
  }

  private static final FileCreator FILE_CREATOR;

  private static DFSClientAdaptor createDFSClientAdaptor() throws NoSuchMethodException {
    Method isClientRunningMethod = DFSClient.class.getDeclaredMethod("isClientRunning");
    isClientRunningMethod.setAccessible(true);
    return new DFSClientAdaptor() {

      @Override
      public boolean isClientRunning(DFSClient client) {
        try {
          return (Boolean) isClientRunningMethod.invoke(client);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private static LeaseManager createLeaseManager() throws NoSuchMethodException {
    Method beginFileLeaseMethod =
        DFSClient.class.getDeclaredMethod("beginFileLease", long.class, DFSOutputStream.class);
    beginFileLeaseMethod.setAccessible(true);
    Method endFileLeaseMethod = DFSClient.class.getDeclaredMethod("endFileLease", long.class);
    endFileLeaseMethod.setAccessible(true);
    return new LeaseManager() {

      @Override
      public void begin(DFSClient client, long inodeId) {
        try {
          beginFileLeaseMethod.invoke(client, inodeId, null);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void end(DFSClient client, long inodeId) {
        try {
          endFileLeaseMethod.invoke(client, inodeId);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private static PipelineAckStatusGetter createPipelineAckStatusGetter27()
      throws NoSuchMethodException {
    Method getFlagListMethod = PipelineAckProto.class.getMethod("getFlagList");
    @SuppressWarnings("rawtypes")
    Class<? extends Enum> ecnClass;
    try {
      ecnClass = Class.forName("org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck$ECN")
          .asSubclass(Enum.class);
    } catch (ClassNotFoundException e) {
      String msg = "Couldn't properly initialize the PipelineAck.ECN class. Please " +
          "update your WAL Provider to not make use of the 'asyncfs' provider. See " +
          "HBASE-16110 for more information.";
      LOG.error(msg, e);
      throw new Error(msg, e);
    }
    @SuppressWarnings("unchecked")
    Enum<?> disabledECN = Enum.valueOf(ecnClass, "DISABLED");
    Method getReplyMethod = PipelineAckProto.class.getMethod("getReply", int.class);
    Method combineHeaderMethod =
        PipelineAck.class.getMethod("combineHeader", ecnClass, Status.class);
    Method getStatusFromHeaderMethod =
        PipelineAck.class.getMethod("getStatusFromHeader", int.class);
    return new PipelineAckStatusGetter() {

      @Override
      public Status get(PipelineAckProto ack) {
        try {
          @SuppressWarnings("unchecked")
          List<Integer> flagList = (List<Integer>) getFlagListMethod.invoke(ack);
          Integer headerFlag;
          if (flagList.isEmpty()) {
            Status reply = (Status) getReplyMethod.invoke(ack, 0);
            headerFlag = (Integer) combineHeaderMethod.invoke(null, disabledECN, reply);
          } else {
            headerFlag = flagList.get(0);
          }
          return (Status) getStatusFromHeaderMethod.invoke(null, headerFlag);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private static PipelineAckStatusGetter createPipelineAckStatusGetter26()
      throws NoSuchMethodException {
    Method getStatusMethod = PipelineAckProto.class.getMethod("getStatus", int.class);
    return new PipelineAckStatusGetter() {

      @Override
      public Status get(PipelineAckProto ack) {
        try {
          return (Status) getStatusMethod.invoke(ack, 0);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private static PipelineAckStatusGetter createPipelineAckStatusGetter()
      throws NoSuchMethodException {
    try {
      return createPipelineAckStatusGetter27();
    } catch (NoSuchMethodException e) {
      LOG.debug("Can not get expected method " + e.getMessage() +
          ", this usually because your Hadoop is pre 2.7.0, " +
          "try the methods in Hadoop 2.6.x instead.");
    }
    return createPipelineAckStatusGetter26();
  }

  private static StorageTypeSetter createStorageTypeSetter() throws NoSuchMethodException {
    Method setStorageTypeMethod =
        OpWriteBlockProto.Builder.class.getMethod("setStorageType", StorageTypeProto.class);
    ImmutableMap.Builder<String, StorageTypeProto> builder = ImmutableMap.builder();
    for (StorageTypeProto storageTypeProto : StorageTypeProto.values()) {
      builder.put(storageTypeProto.name(), storageTypeProto);
    }
    ImmutableMap<String, StorageTypeProto> name2ProtoEnum = builder.build();
    return new StorageTypeSetter() {

      @Override
      public OpWriteBlockProto.Builder set(OpWriteBlockProto.Builder builder, Enum<?> storageType) {
        Object protoEnum = name2ProtoEnum.get(storageType.name());
        try {
          setStorageTypeMethod.invoke(builder, protoEnum);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
        return builder;
      }
    };
  }

  private static BlockAdder createBlockAdder() throws NoSuchMethodException {
    for (Method method : ClientProtocol.class.getMethods()) {
      if (method.getName().equals("addBlock")) {
        Method addBlockMethod = method;
        Class<?>[] paramTypes = addBlockMethod.getParameterTypes();
        if (paramTypes[paramTypes.length - 1] == String[].class) {
          return new BlockAdder() {

            @Override
            public LocatedBlock addBlock(ClientProtocol namenode, String src, String clientName,
                ExtendedBlock previous, DatanodeInfo[] excludeNodes, long fileId,
                String[] favoredNodes) throws IOException {
              try {
                return (LocatedBlock) addBlockMethod.invoke(namenode, src, clientName, previous,
                  excludeNodes, fileId, favoredNodes);
              } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
              } catch (InvocationTargetException e) {
                Throwables.propagateIfPossible(e.getTargetException(), IOException.class);
                throw new RuntimeException(e);
              }
            }
          };
        } else {
          return new BlockAdder() {

            @Override
            public LocatedBlock addBlock(ClientProtocol namenode, String src, String clientName,
                ExtendedBlock previous, DatanodeInfo[] excludeNodes, long fileId,
                String[] favoredNodes) throws IOException {
              try {
                return (LocatedBlock) addBlockMethod.invoke(namenode, src, clientName, previous,
                  excludeNodes, fileId, favoredNodes, null);
              } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
              } catch (InvocationTargetException e) {
                Throwables.propagateIfPossible(e.getTargetException(), IOException.class);
                throw new RuntimeException(e);
              }
            }
          };
        }
      }
    }
    throw new NoSuchMethodException("Can not find addBlock method in ClientProtocol");
  }

  private static PBHelper createPBHelper() throws NoSuchMethodException {
    Class<?> helperClass;
    String clazzName = "org.apache.hadoop.hdfs.protocolPB.PBHelperClient";
    try {
      helperClass = Class.forName(clazzName);
    } catch (ClassNotFoundException e) {
      helperClass = org.apache.hadoop.hdfs.protocolPB.PBHelper.class;
      LOG.debug("" + clazzName + " not found (Hadoop is pre-2.8.0?); using " +
          helperClass.toString() + " instead.");
    }
    Method convertEBMethod = helperClass.getMethod("convert", ExtendedBlock.class);
    Method convertTokenMethod = helperClass.getMethod("convert", Token.class);
    return new PBHelper() {

      @Override
      public ExtendedBlockProto convert(ExtendedBlock b) {
        try {
          return (ExtendedBlockProto) convertEBMethod.invoke(null, b);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public TokenProto convert(Token<?> tok) {
        try {
          return (TokenProto) convertTokenMethod.invoke(null, tok);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private static ChecksumCreater createChecksumCreater28(Method getConfMethod, Class<?> confClass)
      throws NoSuchMethodException {
    for (Method method : confClass.getMethods()) {
      if (method.getName().equals("createChecksum")) {
        Method createChecksumMethod = method;
        return new ChecksumCreater() {

          @Override
          public DataChecksum createChecksum(DFSClient client) {
            try {
              return (DataChecksum) createChecksumMethod.invoke(getConfMethod.invoke(client),
                (Object) null);
            } catch (IllegalAccessException | InvocationTargetException e) {
              throw new RuntimeException(e);
            }
          }
        };
      }
    }
    throw new NoSuchMethodException("Can not find createChecksum method in DfsClientConf");
  }

  private static ChecksumCreater createChecksumCreater27(Method getConfMethod, Class<?> confClass)
      throws NoSuchMethodException {
    Method createChecksumMethod = confClass.getDeclaredMethod("createChecksum");
    createChecksumMethod.setAccessible(true);
    return new ChecksumCreater() {

      @Override
      public DataChecksum createChecksum(DFSClient client) {
        try {
          return (DataChecksum) createChecksumMethod.invoke(getConfMethod.invoke(client));
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private static ChecksumCreater createChecksumCreater()
      throws NoSuchMethodException, ClassNotFoundException {
    Method getConfMethod = DFSClient.class.getMethod("getConf");
    try {
      return createChecksumCreater28(getConfMethod,
        Class.forName("org.apache.hadoop.hdfs.client.impl.DfsClientConf"));
    } catch (ClassNotFoundException e) {
      LOG.debug("No DfsClientConf class found, should be hadoop 2.7-", e);
    }
    return createChecksumCreater27(getConfMethod,
      Class.forName("org.apache.hadoop.hdfs.DFSClient$Conf"));
  }

  private static FileCreator createFileCreator3() throws NoSuchMethodException {
    Method createMethod = ClientProtocol.class.getMethod("create", String.class, FsPermission.class,
      String.class, EnumSetWritable.class, boolean.class, short.class, long.class,
      CryptoProtocolVersion[].class, String.class);

    return (instance, src, masked, clientName, flag, createParent, replication, blockSize,
        supportedVersions) -> {
      return (HdfsFileStatus) createMethod.invoke(instance, src, masked, clientName, flag,
        createParent, replication, blockSize, supportedVersions, null);
    };
  }

  private static FileCreator createFileCreator2() throws NoSuchMethodException {
    Method createMethod = ClientProtocol.class.getMethod("create", String.class, FsPermission.class,
      String.class, EnumSetWritable.class, boolean.class, short.class, long.class,
      CryptoProtocolVersion[].class);

    return (instance, src, masked, clientName, flag, createParent, replication, blockSize,
        supportedVersions) -> {
      return (HdfsFileStatus) createMethod.invoke(instance, src, masked, clientName, flag,
        createParent, replication, blockSize, supportedVersions);
    };
  }

  private static FileCreator createFileCreator() throws NoSuchMethodException {
    try {
      return createFileCreator3();
    } catch (NoSuchMethodException e) {
      LOG.debug("ClientProtocol::create wrong number of arguments, should be hadoop 2.x");
    }
    return createFileCreator2();
  }

  // cancel the processing if DFSClient is already closed.
  static final class CancelOnClose implements CancelableProgressable {

    private final DFSClient client;

    public CancelOnClose(DFSClient client) {
      this.client = client;
    }

    @Override
    public boolean progress() {
      return DFS_CLIENT_ADAPTOR.isClientRunning(client);
    }
  }

  static {
    try {
      PIPELINE_ACK_STATUS_GETTER = createPipelineAckStatusGetter();
      STORAGE_TYPE_SETTER = createStorageTypeSetter();
      BLOCK_ADDER = createBlockAdder();
      LEASE_MANAGER = createLeaseManager();
      DFS_CLIENT_ADAPTOR = createDFSClientAdaptor();
      PB_HELPER = createPBHelper();
      CHECKSUM_CREATER = createChecksumCreater();
      FILE_CREATOR = createFileCreator();
    } catch (Exception e) {
      String msg = "Couldn't properly initialize access to HDFS internals. Please " +
          "update your WAL Provider to not make use of the 'asyncfs' provider. See " +
          "HBASE-16110 for more information.";
      LOG.error(msg, e);
      throw new Error(msg, e);
    }
  }

  static void beginFileLease(DFSClient client, long inodeId) {
    LEASE_MANAGER.begin(client, inodeId);
  }

  static void endFileLease(DFSClient client, long inodeId) {
    LEASE_MANAGER.end(client, inodeId);
  }

  static DataChecksum createChecksum(DFSClient client) {
    return CHECKSUM_CREATER.createChecksum(client);
  }

  static Status getStatus(PipelineAckProto ack) {
    return PIPELINE_ACK_STATUS_GETTER.get(ack);
  }

  private static void processWriteBlockResponse(Channel channel, DatanodeInfo dnInfo,
      Promise<Channel> promise, int timeoutMs) {
    channel.pipeline().addLast(new IdleStateHandler(timeoutMs, 0, 0, TimeUnit.MILLISECONDS),
      new ProtobufVarint32FrameDecoder(),
      new ProtobufDecoder(BlockOpResponseProto.getDefaultInstance()),
      new SimpleChannelInboundHandler<BlockOpResponseProto>() {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, BlockOpResponseProto resp)
            throws Exception {
          Status pipelineStatus = resp.getStatus();
          if (PipelineAck.isRestartOOBStatus(pipelineStatus)) {
            throw new IOException("datanode " + dnInfo + " is restarting");
          }
          String logInfo = "ack with firstBadLink as " + resp.getFirstBadLink();
          if (resp.getStatus() != Status.SUCCESS) {
            if (resp.getStatus() == Status.ERROR_ACCESS_TOKEN) {
              throw new InvalidBlockTokenException("Got access token error" + ", status message " +
                  resp.getMessage() + ", " + logInfo);
            } else {
              throw new IOException("Got error" + ", status=" + resp.getStatus().name() +
                  ", status message " + resp.getMessage() + ", " + logInfo);
            }
          }
          // success
          ChannelPipeline p = ctx.pipeline();
          for (ChannelHandler handler; (handler = p.removeLast()) != null;) {
            // do not remove all handlers because we may have wrap or unwrap handlers at the header
            // of pipeline.
            if (handler instanceof IdleStateHandler) {
              break;
            }
          }
          // Disable auto read here. Enable it after we setup the streaming pipeline in
          // FanOutOneBLockAsyncDFSOutput.
          ctx.channel().config().setAutoRead(false);
          promise.trySuccess(ctx.channel());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
          promise.tryFailure(new IOException("connection to " + dnInfo + " is closed"));
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
          if (evt instanceof IdleStateEvent && ((IdleStateEvent) evt).state() == READER_IDLE) {
            promise
                .tryFailure(new IOException("Timeout(" + timeoutMs + "ms) waiting for response"));
          } else {
            super.userEventTriggered(ctx, evt);
          }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
          promise.tryFailure(cause);
        }
      });
  }

  private static void requestWriteBlock(Channel channel, Enum<?> storageType,
      OpWriteBlockProto.Builder writeBlockProtoBuilder) throws IOException {
    OpWriteBlockProto proto = STORAGE_TYPE_SETTER.set(writeBlockProtoBuilder, storageType).build();
    int protoLen = proto.getSerializedSize();
    ByteBuf buffer =
        channel.alloc().buffer(3 + CodedOutputStream.computeRawVarint32Size(protoLen) + protoLen);
    buffer.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION);
    buffer.writeByte(Op.WRITE_BLOCK.code);
    proto.writeDelimitedTo(new ByteBufOutputStream(buffer));
    channel.writeAndFlush(buffer);
  }

  private static void initialize(Configuration conf, Channel channel, DatanodeInfo dnInfo,
      Enum<?> storageType, OpWriteBlockProto.Builder writeBlockProtoBuilder, int timeoutMs,
      DFSClient client, Token<BlockTokenIdentifier> accessToken, Promise<Channel> promise)
      throws IOException {
    Promise<Void> saslPromise = channel.eventLoop().newPromise();
    trySaslNegotiate(conf, channel, dnInfo, timeoutMs, client, accessToken, saslPromise);
    saslPromise.addListener(new FutureListener<Void>() {

      @Override
      public void operationComplete(Future<Void> future) throws Exception {
        if (future.isSuccess()) {
          // setup response processing pipeline first, then send request.
          processWriteBlockResponse(channel, dnInfo, promise, timeoutMs);
          requestWriteBlock(channel, storageType, writeBlockProtoBuilder);
        } else {
          promise.tryFailure(future.cause());
        }
      }
    });
  }

  private static List<Future<Channel>> connectToDataNodes(Configuration conf, DFSClient client,
      String clientName, LocatedBlock locatedBlock, long maxBytesRcvd, long latestGS,
      BlockConstructionStage stage, DataChecksum summer, EventLoopGroup eventLoopGroup,
      Class<? extends Channel> channelClass) {
    Enum<?>[] storageTypes = locatedBlock.getStorageTypes();
    DatanodeInfo[] datanodeInfos = locatedBlock.getLocations();
    boolean connectToDnViaHostname =
        conf.getBoolean(DFS_CLIENT_USE_DN_HOSTNAME, DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT);
    int timeoutMs = conf.getInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY, READ_TIMEOUT);
    ExtendedBlock blockCopy = new ExtendedBlock(locatedBlock.getBlock());
    blockCopy.setNumBytes(locatedBlock.getBlockSize());
    ClientOperationHeaderProto header = ClientOperationHeaderProto.newBuilder()
        .setBaseHeader(BaseHeaderProto.newBuilder().setBlock(PB_HELPER.convert(blockCopy))
            .setToken(PB_HELPER.convert(locatedBlock.getBlockToken())))
        .setClientName(clientName).build();
    ChecksumProto checksumProto = DataTransferProtoUtil.toProto(summer);
    OpWriteBlockProto.Builder writeBlockProtoBuilder = OpWriteBlockProto.newBuilder()
        .setHeader(header).setStage(OpWriteBlockProto.BlockConstructionStage.valueOf(stage.name()))
        .setPipelineSize(1).setMinBytesRcvd(locatedBlock.getBlock().getNumBytes())
        .setMaxBytesRcvd(maxBytesRcvd).setLatestGenerationStamp(latestGS)
        .setRequestedChecksum(checksumProto)
        .setCachingStrategy(CachingStrategyProto.newBuilder().setDropBehind(true).build());
    List<Future<Channel>> futureList = new ArrayList<>(datanodeInfos.length);
    for (int i = 0; i < datanodeInfos.length; i++) {
      DatanodeInfo dnInfo = datanodeInfos[i];
      Enum<?> storageType = storageTypes[i];
      Promise<Channel> promise = eventLoopGroup.next().newPromise();
      futureList.add(promise);
      String dnAddr = dnInfo.getXferAddr(connectToDnViaHostname);
      new Bootstrap().group(eventLoopGroup).channel(channelClass)
          .option(CONNECT_TIMEOUT_MILLIS, timeoutMs).handler(new ChannelInitializer<Channel>() {

            @Override
            protected void initChannel(Channel ch) throws Exception {
              // we need to get the remote address of the channel so we can only move on after
              // channel connected. Leave an empty implementation here because netty does not allow
              // a null handler.
            }
          }).connect(NetUtils.createSocketAddr(dnAddr)).addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
              if (future.isSuccess()) {
                initialize(conf, future.channel(), dnInfo, storageType, writeBlockProtoBuilder,
                  timeoutMs, client, locatedBlock.getBlockToken(), promise);
              } else {
                promise.tryFailure(future.cause());
              }
            }
          });
    }
    return futureList;
  }

  /**
   * Exception other than RemoteException thrown when calling create on namenode
   */
  public static class NameNodeException extends IOException {

    private static final long serialVersionUID = 3143237406477095390L;

    public NameNodeException(Throwable cause) {
      super(cause);
    }
  }

  private static FanOutOneBlockAsyncDFSOutput createOutput(DistributedFileSystem dfs, String src,
      boolean overwrite, boolean createParent, short replication, long blockSize,
      EventLoopGroup eventLoopGroup, Class<? extends Channel> channelClass) throws IOException {
    Configuration conf = dfs.getConf();
    FSUtils fsUtils = FSUtils.getInstance(dfs, conf);
    DFSClient client = dfs.getClient();
    String clientName = client.getClientName();
    ClientProtocol namenode = client.getNamenode();
    int createMaxRetries = conf.getInt(ASYNC_DFS_OUTPUT_CREATE_MAX_RETRIES,
      DEFAULT_ASYNC_DFS_OUTPUT_CREATE_MAX_RETRIES);
    DatanodeInfo[] excludesNodes = EMPTY_DN_ARRAY;
    for (int retry = 0;; retry++) {
      HdfsFileStatus stat;
      try {
        stat = FILE_CREATOR.create(namenode, src,
          FsPermission.getFileDefault().applyUMask(FsPermission.getUMask(conf)), clientName,
          new EnumSetWritable<>(overwrite ? EnumSet.of(CREATE, OVERWRITE) : EnumSet.of(CREATE)),
          createParent, replication, blockSize, CryptoProtocolVersion.supported());
      } catch (Exception e) {
        if (e instanceof RemoteException) {
          throw (RemoteException) e;
        } else {
          throw new NameNodeException(e);
        }
      }
      beginFileLease(client, stat.getFileId());
      boolean succ = false;
      LocatedBlock locatedBlock = null;
      List<Future<Channel>> futureList = null;
      try {
        DataChecksum summer = createChecksum(client);
        locatedBlock = BLOCK_ADDER.addBlock(namenode, src, client.getClientName(), null,
          excludesNodes, stat.getFileId(), null);
        List<Channel> datanodeList = new ArrayList<>();
        futureList = connectToDataNodes(conf, client, clientName, locatedBlock, 0L, 0L,
          PIPELINE_SETUP_CREATE, summer, eventLoopGroup, channelClass);
        for (int i = 0, n = futureList.size(); i < n; i++) {
          try {
            datanodeList.add(futureList.get(i).syncUninterruptibly().getNow());
          } catch (Exception e) {
            // exclude the broken DN next time
            excludesNodes = ArrayUtils.add(excludesNodes, locatedBlock.getLocations()[i]);
            throw e;
          }
        }
        Encryptor encryptor = createEncryptor(conf, stat, client);
        FanOutOneBlockAsyncDFSOutput output =
          new FanOutOneBlockAsyncDFSOutput(conf, fsUtils, dfs, client, namenode, clientName, src,
              stat.getFileId(), locatedBlock, encryptor, datanodeList, summer, ALLOC);
        succ = true;
        return output;
      } catch (RemoteException e) {
        LOG.warn("create fan-out dfs output {} failed, retry = {}", src, retry, e);
        if (shouldRetryCreate(e)) {
          if (retry >= createMaxRetries) {
            throw e.unwrapRemoteException();
          }
        } else {
          throw e.unwrapRemoteException();
        }
      } catch (IOException e) {
        LOG.warn("create fan-out dfs output {} failed, retry = {}", src, retry, e);
        if (retry >= createMaxRetries) {
          throw e;
        }
        // overwrite the old broken file.
        overwrite = true;
        try {
          Thread.sleep(ConnectionUtils.getPauseTime(100, retry));
        } catch (InterruptedException ie) {
          throw new InterruptedIOException();
        }
      } finally {
        if (!succ) {
          if (futureList != null) {
            for (Future<Channel> f : futureList) {
              f.addListener(new FutureListener<Channel>() {

                @Override
                public void operationComplete(Future<Channel> future) throws Exception {
                  if (future.isSuccess()) {
                    future.getNow().close();
                  }
                }
              });
            }
          }
          endFileLease(client, stat.getFileId());
        }
      }
    }
  }

  /**
   * Create a {@link FanOutOneBlockAsyncDFSOutput}. The method maybe blocked so do not call it
   * inside an {@link EventLoop}.
   */
  public static FanOutOneBlockAsyncDFSOutput createOutput(DistributedFileSystem dfs, Path f,
      boolean overwrite, boolean createParent, short replication, long blockSize,
      EventLoopGroup eventLoopGroup, Class<? extends Channel> channelClass) throws IOException {
    return new FileSystemLinkResolver<FanOutOneBlockAsyncDFSOutput>() {

      @Override
      public FanOutOneBlockAsyncDFSOutput doCall(Path p)
          throws IOException, UnresolvedLinkException {
        return createOutput(dfs, p.toUri().getPath(), overwrite, createParent, replication,
          blockSize, eventLoopGroup, channelClass);
      }

      @Override
      public FanOutOneBlockAsyncDFSOutput next(FileSystem fs, Path p) throws IOException {
        throw new UnsupportedOperationException();
      }
    }.resolve(dfs, f);
  }

  public static boolean shouldRetryCreate(RemoteException e) {
    // RetryStartFileException is introduced in HDFS 2.6+, so here we can only use the class name.
    // For exceptions other than this, we just throw it out. This is same with
    // DFSOutputStream.newStreamForCreate.
    return e.getClassName().endsWith("RetryStartFileException");
  }

  static void completeFile(DFSClient client, ClientProtocol namenode, String src, String clientName,
      ExtendedBlock block, long fileId) {
    for (int retry = 0;; retry++) {
      try {
        if (namenode.complete(src, clientName, block, fileId)) {
          endFileLease(client, fileId);
          return;
        } else {
          LOG.warn("complete file " + src + " not finished, retry = " + retry);
        }
      } catch (RemoteException e) {
        IOException ioe = e.unwrapRemoteException();
        if (ioe instanceof LeaseExpiredException) {
          LOG.warn("lease for file " + src + " is expired, give up", e);
          return;
        } else {
          LOG.warn("complete file " + src + " failed, retry = " + retry, e);
        }
      } catch (Exception e) {
        LOG.warn("complete file " + src + " failed, retry = " + retry, e);
      }
      sleepIgnoreInterrupt(retry);
    }
  }

  static void sleepIgnoreInterrupt(int retry) {
    try {
      Thread.sleep(ConnectionUtils.getPauseTime(100, retry));
    } catch (InterruptedException e) {
    }
  }
}
