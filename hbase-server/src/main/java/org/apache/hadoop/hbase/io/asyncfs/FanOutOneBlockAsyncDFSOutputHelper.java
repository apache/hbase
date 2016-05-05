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
import static org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputSaslHelper.createCryptoCodec;
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
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemLinkResolver;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputSaslHelper.CryptoCodec;
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
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto.Builder;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.PipelineAckProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageTypeProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
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

  // helper class for creating DataChecksum object.
  private static final Method CREATE_CHECKSUM;

  // helper class for getting Status from PipelineAckProto. In hadoop 2.6 or before, there is a
  // getStatus method, and for hadoop 2.7 or after, the status is retrieved from flag. The flag may
  // get from proto directly, or combined by the reply field of the proto and a ECN object. See
  // createPipelineAckStatusGetter for more details.
  private interface PipelineAckStatusGetter {
    Status get(PipelineAckProto ack);
  }

  private static final PipelineAckStatusGetter PIPELINE_ACK_STATUS_GETTER;

  // StorageType enum is added in hadoop 2.4, but it is moved to another package in hadoop 2.6 and
  // the setter method in OpWriteBlockProto is also added in hadoop 2.6. So we need to skip the
  // setStorageType call if it is hadoop 2.5 or before. See createStorageTypeSetter for more
  // details.
  private interface StorageTypeSetter {
    OpWriteBlockProto.Builder set(OpWriteBlockProto.Builder builder, Enum<?> storageType);
  }

  private static final StorageTypeSetter STORAGE_TYPE_SETTER;

  // helper class for calling create method on namenode. There is a supportedVersions parameter for
  // hadoop 2.6 or after. See createFileCreater for more details.
  private interface FileCreater {
    HdfsFileStatus create(ClientProtocol namenode, String src, FsPermission masked,
        String clientName, EnumSetWritable<CreateFlag> flag, boolean createParent,
        short replication, long blockSize) throws IOException;
  }

  private static final FileCreater FILE_CREATER;

  // helper class for add or remove lease from DFSClient. Hadoop 2.4 use src as the Map's key, and
  // hadoop 2.5 or after use inodeId. See createLeaseManager for more details.
  private interface LeaseManager {

    void begin(DFSClient client, String src, long inodeId);

    void end(DFSClient client, String src, long inodeId);
  }

  private static final LeaseManager LEASE_MANAGER;

  // This is used to terminate a recoverFileLease call when FileSystem is already closed.
  // isClientRunning is not public so we need to use reflection.
  private interface DFSClientAdaptor {

    boolean isClientRunning(DFSClient client);
  }

  private static final DFSClientAdaptor DFS_CLIENT_ADAPTOR;

  private static DFSClientAdaptor createDFSClientAdaptor() {
    try {
      final Method isClientRunningMethod = DFSClient.class.getDeclaredMethod("isClientRunning");
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
    } catch (NoSuchMethodException e) {
      throw new Error(e);
    }
  }

  private static LeaseManager createLeaseManager() {
    try {
      final Method beginFileLeaseMethod =
          DFSClient.class.getDeclaredMethod("beginFileLease", long.class, DFSOutputStream.class);
      beginFileLeaseMethod.setAccessible(true);
      final Method endFileLeaseMethod =
          DFSClient.class.getDeclaredMethod("endFileLease", long.class);
      endFileLeaseMethod.setAccessible(true);
      return new LeaseManager() {

        @Override
        public void begin(DFSClient client, String src, long inodeId) {
          try {
            beginFileLeaseMethod.invoke(client, inodeId, null);
          } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public void end(DFSClient client, String src, long inodeId) {
          try {
            endFileLeaseMethod.invoke(client, inodeId);
          } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
          }
        }
      };
    } catch (NoSuchMethodException e) {
      LOG.warn("No inodeId related lease methods found, should be hadoop 2.4-", e);
    }
    try {
      final Method beginFileLeaseMethod =
          DFSClient.class.getDeclaredMethod("beginFileLease", String.class, DFSOutputStream.class);
      beginFileLeaseMethod.setAccessible(true);
      final Method endFileLeaseMethod =
          DFSClient.class.getDeclaredMethod("endFileLease", String.class);
      endFileLeaseMethod.setAccessible(true);
      return new LeaseManager() {

        @Override
        public void begin(DFSClient client, String src, long inodeId) {
          try {
            beginFileLeaseMethod.invoke(client, src, null);
          } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public void end(DFSClient client, String src, long inodeId) {
          try {
            endFileLeaseMethod.invoke(client, src);
          } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
          }
        }
      };
    } catch (NoSuchMethodException e) {
      throw new Error(e);
    }
  }

  private static PipelineAckStatusGetter createPipelineAckStatusGetter() {
    try {
      final Method getFlagListMethod = PipelineAckProto.class.getMethod("getFlagList");
      @SuppressWarnings("rawtypes")
      Class<? extends Enum> ecnClass;
      try {
        ecnClass =
            Class.forName("org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck$ECN")
                .asSubclass(Enum.class);
      } catch (ClassNotFoundException e) {
        throw new Error(e);
      }
      @SuppressWarnings("unchecked")
      final Enum<?> disabledECN = Enum.valueOf(ecnClass, "DISABLED");
      final Method getReplyMethod = PipelineAckProto.class.getMethod("getReply", int.class);
      final Method combineHeaderMethod =
          PipelineAck.class.getMethod("combineHeader", ecnClass, Status.class);
      final Method getStatusFromHeaderMethod =
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
    } catch (NoSuchMethodException e) {
      LOG.warn("Can not get expected methods, should be hadoop 2.6-", e);
    }
    try {
      final Method getStatusMethod = PipelineAckProto.class.getMethod("getStatus", int.class);
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
    } catch (NoSuchMethodException e) {
      throw new Error(e);
    }
  }

  private static StorageTypeSetter createStorageTypeSetter() {
    final Method setStorageTypeMethod;
    try {
      setStorageTypeMethod =
          OpWriteBlockProto.Builder.class.getMethod("setStorageType", StorageTypeProto.class);
    } catch (NoSuchMethodException e) {
      LOG.warn("noSetStorageType method found, should be hadoop 2.5-", e);
      return new StorageTypeSetter() {

        @Override
        public Builder set(Builder builder, Enum<?> storageType) {
          return builder;
        }
      };
    }
    ImmutableMap.Builder<String, StorageTypeProto> builder = ImmutableMap.builder();
    for (StorageTypeProto storageTypeProto : StorageTypeProto.values()) {
      builder.put(storageTypeProto.name(), storageTypeProto);
    }
    final ImmutableMap<String, StorageTypeProto> name2ProtoEnum = builder.build();
    return new StorageTypeSetter() {

      @Override
      public Builder set(Builder builder, Enum<?> storageType) {
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

  private static FileCreater createFileCreater() {
    for (Method method : ClientProtocol.class.getMethods()) {
      if (method.getName().equals("create")) {
        final Method createMethod = method;
        Class<?>[] paramTypes = createMethod.getParameterTypes();
        if (paramTypes[paramTypes.length - 1] == long.class) {
          return new FileCreater() {

            @Override
            public HdfsFileStatus create(ClientProtocol namenode, String src, FsPermission masked,
                String clientName, EnumSetWritable<CreateFlag> flag, boolean createParent,
                short replication, long blockSize) throws IOException {
              try {
                return (HdfsFileStatus) createMethod.invoke(namenode, src, masked, clientName,
                  flag, createParent, replication, blockSize);
              } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
              } catch (InvocationTargetException e) {
                Throwables.propagateIfPossible(e.getTargetException(), IOException.class);
                throw new RuntimeException(e);
              }
            }
          };
        } else {
          try {
            Class<?> cryptoProtocolVersionClass =
                Class.forName("org.apache.hadoop.crypto.CryptoProtocolVersion");
            Method supportedMethod = cryptoProtocolVersionClass.getMethod("supported");
            final Object supported = supportedMethod.invoke(null);
            return new FileCreater() {

              @Override
              public HdfsFileStatus create(ClientProtocol namenode, String src,
                  FsPermission masked, String clientName, EnumSetWritable<CreateFlag> flag,
                  boolean createParent, short replication, long blockSize) throws IOException {
                try {
                  return (HdfsFileStatus) createMethod.invoke(namenode, src, masked, clientName,
                    flag, createParent, replication, blockSize, supported);
                } catch (IllegalAccessException e) {
                  throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                  Throwables.propagateIfPossible(e.getTargetException(), IOException.class);
                  throw new RuntimeException(e);
                }
              }
            };
          } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException
              | InvocationTargetException e) {
            throw new Error(e);
          }
        }
      }
    }
    throw new Error("No create method found for " + ClientProtocol.class.getName());
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
      CREATE_CHECKSUM = DFSClient.Conf.class.getDeclaredMethod("createChecksum");
      CREATE_CHECKSUM.setAccessible(true);
    } catch (NoSuchMethodException e) {
      throw new Error(e);
    }

    PIPELINE_ACK_STATUS_GETTER = createPipelineAckStatusGetter();
    STORAGE_TYPE_SETTER = createStorageTypeSetter();
    FILE_CREATER = createFileCreater();
    LEASE_MANAGER = createLeaseManager();
    DFS_CLIENT_ADAPTOR = createDFSClientAdaptor();
  }

  static void beginFileLease(DFSClient client, String src, long inodeId) {
    LEASE_MANAGER.begin(client, src, inodeId);
  }

  static void endFileLease(DFSClient client, String src, long inodeId) {
    LEASE_MANAGER.end(client, src, inodeId);
  }

  static DataChecksum createChecksum(DFSClient client) {
    try {
      return (DataChecksum) CREATE_CHECKSUM.invoke(client.getConf());
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  static Status getStatus(PipelineAckProto ack) {
    return PIPELINE_ACK_STATUS_GETTER.get(ack);
  }

  private static void processWriteBlockResponse(Channel channel, final DatanodeInfo dnInfo,
      final Promise<Channel> promise, final int timeoutMs) {
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

  private static void initialize(Configuration conf, final Channel channel,
      final DatanodeInfo dnInfo, final Enum<?> storageType,
      final OpWriteBlockProto.Builder writeBlockProtoBuilder, final int timeoutMs,
      DFSClient client, Token<BlockTokenIdentifier> accessToken, final Promise<Channel> promise) {
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

  private static List<Future<Channel>> connectToDataNodes(final Configuration conf,
      final DFSClient client, String clientName, final LocatedBlock locatedBlock,
      long maxBytesRcvd, long latestGS, BlockConstructionStage stage, DataChecksum summer,
      EventLoop eventLoop) {
    Enum<?>[] storageTypes = locatedBlock.getStorageTypes();
    DatanodeInfo[] datanodeInfos = locatedBlock.getLocations();
    boolean connectToDnViaHostname =
        conf.getBoolean(DFS_CLIENT_USE_DN_HOSTNAME, DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT);
    final int timeoutMs =
        conf.getInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY, HdfsServerConstants.READ_TIMEOUT);
    ExtendedBlock blockCopy = new ExtendedBlock(locatedBlock.getBlock());
    blockCopy.setNumBytes(locatedBlock.getBlockSize());
    ClientOperationHeaderProto header =
        ClientOperationHeaderProto
            .newBuilder()
            .setBaseHeader(
              BaseHeaderProto.newBuilder().setBlock(PBHelper.convert(blockCopy))
                  .setToken(PBHelper.convert(locatedBlock.getBlockToken())))
            .setClientName(clientName).build();
    ChecksumProto checksumProto = DataTransferProtoUtil.toProto(summer);
    final OpWriteBlockProto.Builder writeBlockProtoBuilder =
        OpWriteBlockProto.newBuilder().setHeader(header)
            .setStage(OpWriteBlockProto.BlockConstructionStage.valueOf(stage.name()))
            .setPipelineSize(1).setMinBytesRcvd(locatedBlock.getBlock().getNumBytes())
            .setMaxBytesRcvd(maxBytesRcvd).setLatestGenerationStamp(latestGS)
            .setRequestedChecksum(checksumProto)
            .setCachingStrategy(CachingStrategyProto.newBuilder().setDropBehind(true).build());
    List<Future<Channel>> futureList = new ArrayList<>(datanodeInfos.length);
    for (int i = 0; i < datanodeInfos.length; i++) {
      final DatanodeInfo dnInfo = datanodeInfos[i];
      // Use Enum here because StoregType is moved to another package in hadoop 2.6. Use StorageType
      // will cause compilation error for hadoop 2.5 or before.
      final Enum<?> storageType = storageTypes[i];
      final Promise<Channel> promise = eventLoop.newPromise();
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
      stat =
          FILE_CREATER.create(
            namenode,
            src,
            FsPermission.getFileDefault().applyUMask(FsPermission.getUMask(conf)),
            clientName,
            new EnumSetWritable<CreateFlag>(overwrite ? EnumSet.of(CREATE, OVERWRITE) : EnumSet
                .of(CREATE)), createParent, replication, blockSize);
    } catch (Exception e) {
      if (e instanceof RemoteException) {
        throw (RemoteException) e;
      } else {
        throw new NameNodeException(e);
      }
    }
    beginFileLease(client, src, stat.getFileId());
    boolean succ = false;
    LocatedBlock locatedBlock = null;
    List<Future<Channel>> futureList = null;
    try {
      DataChecksum summer = createChecksum(client);
      locatedBlock =
          namenode.addBlock(src, client.getClientName(), null, null, stat.getFileId(), null);
      List<Channel> datanodeList = new ArrayList<>();
      futureList =
          connectToDataNodes(conf, client, clientName, locatedBlock, 0L, 0L, PIPELINE_SETUP_CREATE,
            summer, eventLoop);
      for (Future<Channel> future : futureList) {
        // fail the creation if there are connection failures since we are fail-fast. The upper
        // layer should retry itself if needed.
        datanodeList.add(future.syncUninterruptibly().getNow());
      }
      CryptoCodec cryptocodec = createCryptoCodec(conf, stat, client);
      FanOutOneBlockAsyncDFSOutput output = new FanOutOneBlockAsyncDFSOutput(conf, fsUtils, dfs,
          client, namenode, clientName, src, stat.getFileId(), locatedBlock, cryptocodec, eventLoop,
          datanodeList, summer, ALLOC);
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
        endFileLease(client, src, stat.getFileId());
        fsUtils.recoverFileLease(dfs, new Path(src), conf, new CancelOnClose(client));
      }
    }
  }

  /**
   * Create a {@link FanOutOneBlockAsyncDFSOutput}. The method maybe blocked so do not call it
   * inside {@link EventLoop}.
   * @param eventLoop all connections to datanode will use the same event loop.
   */
  public static FanOutOneBlockAsyncDFSOutput createOutput(final DistributedFileSystem dfs, Path f,
      final boolean overwrite, final boolean createParent, final short replication,
      final long blockSize, final EventLoop eventLoop) throws IOException {
    return new FileSystemLinkResolver<FanOutOneBlockAsyncDFSOutput>() {

      @Override
      public FanOutOneBlockAsyncDFSOutput doCall(Path p) throws IOException,
          UnresolvedLinkException {
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

  static void completeFile(DFSClient client, ClientProtocol namenode, String src,
      String clientName, ExtendedBlock block, long fileId) {
    for (int retry = 0;; retry++) {
      try {
        if (namenode.complete(src, clientName, block, fileId)) {
          endFileLease(client, src, fileId);
          return;
        } else {
          LOG.warn("complete file " + src + " not finished, retry = " + retry);
        }
      } catch (LeaseExpiredException e) {
        LOG.warn("lease for file " + src + " is expired, give up", e);
        return;
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
