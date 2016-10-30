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

import static io.netty.channel.ChannelOption.CONNECT_TIMEOUT_MILLIS;
import static io.netty.handler.timeout.IdleState.READER_IDLE;
import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;
import static org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputSaslHelper.createEncryptor;
import static org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputSaslHelper.trySaslNegotiate;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT;
import static org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage.PIPELINE_SETUP_CREATE;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.CodedOutputStream;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.crypto.Encryptor;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemLinkResolver;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
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

/**
 * Helper class for implementing {@link FanOutOneBlockAsyncDFSOutput}.
 */
@InterfaceAudience.Private
public final class FanOutOneBlockAsyncDFSOutputHelper {

  private static final Log LOG = LogFactory.getLog(FanOutOneBlockAsyncDFSOutputHelper.class);

  private FanOutOneBlockAsyncDFSOutputHelper() {
  }

  // use pooled allocator for performance.
  private static final ByteBufAllocator ALLOC = PooledByteBufAllocator.DEFAULT;

  // copied from DFSPacket since it is package private.
  public static final long HEART_BEAT_SEQNO = -1L;

  // Timeouts for communicating with DataNode for streaming writes/reads
  public static final int READ_TIMEOUT = 60 * 1000;
  public static final int READ_TIMEOUT_EXTENSION = 5 * 1000;
  public static final int WRITE_TIMEOUT = 8 * 60 * 1000;

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
    DataChecksum createChecksum(Object conf);
  }

  private static final ChecksumCreater CHECKSUM_CREATER;

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
      String msg = "Couldn't properly initialize the PipelineAck.ECN class. Please "
          + "update your WAL Provider to not make use of the 'asyncfs' provider. See "
          + "HBASE-16110 for more information.";
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
      LOG.debug("Can not get expected methods, should be hadoop 2.6-", e);
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
    try {
      helperClass = Class.forName("org.apache.hadoop.hdfs.protocolPB.PBHelperClient");
    } catch (ClassNotFoundException e) {
      LOG.debug("No PBHelperClient class found, should be hadoop 2.7-", e);
      helperClass = org.apache.hadoop.hdfs.protocolPB.PBHelper.class;
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

  private static ChecksumCreater createChecksumCreater28(Class<?> confClass)
      throws NoSuchMethodException {
    for (Method method : confClass.getMethods()) {
      if (method.getName().equals("createChecksum")) {
        Method createChecksumMethod = method;
        return new ChecksumCreater() {

          @Override
          public DataChecksum createChecksum(Object conf) {
            try {
              return (DataChecksum) createChecksumMethod.invoke(conf, (Object) null);
            } catch (IllegalAccessException | InvocationTargetException e) {
              throw new RuntimeException(e);
            }
          }
        };
      }
    }
    throw new NoSuchMethodException("Can not find createChecksum method in DfsClientConf");
  }

  private static ChecksumCreater createChecksumCreater27(Class<?> confClass)
      throws NoSuchMethodException {
    Method createChecksumMethod = confClass.getDeclaredMethod("createChecksum");
    createChecksumMethod.setAccessible(true);
    return new ChecksumCreater() {

      @Override
      public DataChecksum createChecksum(Object conf) {
        try {
          return (DataChecksum) createChecksumMethod.invoke(conf);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private static ChecksumCreater createChecksumCreater()
      throws NoSuchMethodException, ClassNotFoundException {
    try {
      return createChecksumCreater28(
        Class.forName("org.apache.hadoop.hdfs.client.impl.DfsClientConf"));
    } catch (ClassNotFoundException e) {
      LOG.debug("No DfsClientConf class found, should be hadoop 2.7-", e);
    }
    return createChecksumCreater27(Class.forName("org.apache.hadoop.hdfs.DFSClient$Conf"));
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
    } catch (Exception e) {
      String msg = "Couldn't properly initialize access to HDFS internals. Please "
          + "update your WAL Provider to not make use of the 'asyncfs' provider. See "
          + "HBASE-16110 for more information.";
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
    return CHECKSUM_CREATER.createChecksum(client.getConf());
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
              throw new InvalidBlockTokenException("Got access token error" + ", status message "
                  + resp.getMessage() + ", " + logInfo);
            } else {
              throw new IOException("Got error" + ", status=" + resp.getStatus().name()
                  + ", status message " + resp.getMessage() + ", " + logInfo);
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
      BlockConstructionStage stage, DataChecksum summer, EventLoop eventLoop) {
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
      Promise<Channel> promise = eventLoop.newPromise();
      futureList.add(promise);
      String dnAddr = dnInfo.getXferAddr(connectToDnViaHostname);
      new Bootstrap().group(eventLoop).channel(NioSocketChannel.class)
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
      EventLoop eventLoop) throws IOException {
    Configuration conf = dfs.getConf();
    FSUtils fsUtils = FSUtils.getInstance(dfs, conf);
    DFSClient client = dfs.getClient();
    String clientName = client.getClientName();
    ClientProtocol namenode = client.getNamenode();
    HdfsFileStatus stat;
    try {
      stat = namenode.create(src,
        FsPermission.getFileDefault().applyUMask(FsPermission.getUMask(conf)), clientName,
        new EnumSetWritable<CreateFlag>(
            overwrite ? EnumSet.of(CREATE, OVERWRITE) : EnumSet.of(CREATE)),
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
      locatedBlock = BLOCK_ADDER.addBlock(namenode, src, client.getClientName(), null, null,
        stat.getFileId(), null);
      List<Channel> datanodeList = new ArrayList<>();
      futureList = connectToDataNodes(conf, client, clientName, locatedBlock, 0L, 0L,
        PIPELINE_SETUP_CREATE, summer, eventLoop);
      for (Future<Channel> future : futureList) {
        // fail the creation if there are connection failures since we are fail-fast. The upper
        // layer should retry itself if needed.
        datanodeList.add(future.syncUninterruptibly().getNow());
      }
      Encryptor encryptor = createEncryptor(conf, stat, client);
      FanOutOneBlockAsyncDFSOutput output =
          new FanOutOneBlockAsyncDFSOutput(conf, fsUtils, dfs, client, namenode, clientName, src,
              stat.getFileId(), locatedBlock, encryptor, eventLoop, datanodeList, summer, ALLOC);
      succ = true;
      return output;
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
        fsUtils.recoverFileLease(dfs, new Path(src), conf, new CancelOnClose(client));
      }
    }
  }

  /**
   * Create a {@link FanOutOneBlockAsyncDFSOutput}. The method maybe blocked so do not call it
   * inside {@link EventLoop}.
   * @param eventLoop all connections to datanode will use the same event loop.
   */
  public static FanOutOneBlockAsyncDFSOutput createOutput(DistributedFileSystem dfs, Path f,
      boolean overwrite, boolean createParent, short replication, long blockSize,
      EventLoop eventLoop) throws IOException {
    return new FileSystemLinkResolver<FanOutOneBlockAsyncDFSOutput>() {

      @Override
      public FanOutOneBlockAsyncDFSOutput doCall(Path p)
          throws IOException, UnresolvedLinkException {
        return createOutput(dfs, p.toUri().getPath(), overwrite, createParent, replication,
          blockSize, eventLoop);
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
