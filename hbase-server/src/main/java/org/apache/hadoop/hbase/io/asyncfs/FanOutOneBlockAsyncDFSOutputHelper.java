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

import static org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputSaslHelper.createEncryptor;
import static org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputSaslHelper.trySaslNegotiate;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT;
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
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.util.CancelableProgressable;
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
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck.ECN;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BaseHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.CachingStrategyProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientOperationHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.PipelineAckProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    };

    Object createObject(ClientProtocol instance, String src, FsPermission masked, String clientName,
        EnumSetWritable<CreateFlag> flag, boolean createParent, short replication, long blockSize,
        CryptoProtocolVersion[] supportedVersions) throws Exception;
  }

  private static final FileCreator FILE_CREATOR;

  // CreateFlag.SHOULD_REPLICATE is to make OutputStream on a EC directory support hflush/hsync, but
  // EC is introduced in hadoop 3.x so we do not have this enum on 2.x, that's why we need to
  // indirectly reference it through reflection.
  private static final CreateFlag SHOULD_REPLICATE_FLAG;

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

  private static FileCreator createFileCreator3_3() throws NoSuchMethodException {
    Method createMethod = ClientProtocol.class.getMethod("create", String.class, FsPermission.class,
        String.class, EnumSetWritable.class, boolean.class, short.class, long.class,
        CryptoProtocolVersion[].class, String.class, String.class);

    return (instance, src, masked, clientName, flag, createParent, replication, blockSize,
        supportedVersions) -> {
      return (HdfsFileStatus) createMethod.invoke(instance, src, masked, clientName, flag,
          createParent, replication, blockSize, supportedVersions, null, null);
    };
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
      return createFileCreator3_3();
    } catch (NoSuchMethodException e) {
      LOG.debug("ClientProtocol::create wrong number of arguments, should be hadoop 3.2 or below");
    }

    try {
      return createFileCreator3();
    } catch (NoSuchMethodException e) {
      LOG.debug("ClientProtocol::create wrong number of arguments, should be hadoop 2.x");
    }
    return createFileCreator2();
  }

  private static CreateFlag loadShouldReplicateFlag() {
    try {
      return CreateFlag.valueOf("SHOULD_REPLICATE");
    } catch (IllegalArgumentException e) {
      LOG.debug("can not find SHOULD_REPLICATE flag, should be hadoop 2.x", e);
      return null;
    }
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
      LEASE_MANAGER = createLeaseManager();
      DFS_CLIENT_ADAPTOR = createDFSClientAdaptor();
      FILE_CREATOR = createFileCreator();
      SHOULD_REPLICATE_FLAG = loadShouldReplicateFlag();
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
    return client.getConf().createChecksum(null);
  }

  static Status getStatus(PipelineAckProto ack) {
    List<Integer> flagList = ack.getFlagList();
    Integer headerFlag;
    if (flagList.isEmpty()) {
      Status reply = ack.getReply(0);
      headerFlag = PipelineAck.combineHeader(ECN.DISABLED, reply);
    } else {
      headerFlag = flagList.get(0);
    }
    return PipelineAck.getStatusFromHeader(headerFlag);
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

  private static void requestWriteBlock(Channel channel, StorageType storageType,
      OpWriteBlockProto.Builder writeBlockProtoBuilder) throws IOException {
    OpWriteBlockProto proto =
      writeBlockProtoBuilder.setStorageType(PBHelperClient.convertStorageType(storageType)).build();
    int protoLen = proto.getSerializedSize();
    ByteBuf buffer =
      channel.alloc().buffer(3 + CodedOutputStream.computeRawVarint32Size(protoLen) + protoLen);
    buffer.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION);
    buffer.writeByte(Op.WRITE_BLOCK.code);
    proto.writeDelimitedTo(new ByteBufOutputStream(buffer));
    channel.writeAndFlush(buffer);
  }

  private static void initialize(Configuration conf, Channel channel, DatanodeInfo dnInfo,
      StorageType storageType, OpWriteBlockProto.Builder writeBlockProtoBuilder, int timeoutMs,
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
    StorageType[] storageTypes = locatedBlock.getStorageTypes();
    DatanodeInfo[] datanodeInfos = locatedBlock.getLocations();
    boolean connectToDnViaHostname =
        conf.getBoolean(DFS_CLIENT_USE_DN_HOSTNAME, DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT);
    int timeoutMs = conf.getInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY, READ_TIMEOUT);
    ExtendedBlock blockCopy = new ExtendedBlock(locatedBlock.getBlock());
    blockCopy.setNumBytes(locatedBlock.getBlockSize());
    ClientOperationHeaderProto header = ClientOperationHeaderProto.newBuilder()
      .setBaseHeader(BaseHeaderProto.newBuilder().setBlock(PBHelperClient.convert(blockCopy))
        .setToken(PBHelperClient.convert(locatedBlock.getBlockToken())))
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
      StorageType storageType = storageTypes[i];
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

  private static EnumSetWritable<CreateFlag> getCreateFlags(boolean overwrite) {
    List<CreateFlag> flags = new ArrayList<>();
    flags.add(CreateFlag.CREATE);
    if (overwrite) {
      flags.add(CreateFlag.OVERWRITE);
    }
    if (SHOULD_REPLICATE_FLAG != null) {
      flags.add(SHOULD_REPLICATE_FLAG);
    }
    return new EnumSetWritable<>(EnumSet.copyOf(flags));
  }

  private static FanOutOneBlockAsyncDFSOutput createOutput(DistributedFileSystem dfs, String src,
      boolean overwrite, boolean createParent, short replication, long blockSize,
      EventLoopGroup eventLoopGroup, Class<? extends Channel> channelClass) throws IOException {
    Configuration conf = dfs.getConf();
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
          getCreateFlags(overwrite), createParent, replication, blockSize,
          CryptoProtocolVersion.supported());
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
        locatedBlock = namenode.addBlock(src, client.getClientName(), null, excludesNodes,
          stat.getFileId(), null, null);
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
          new FanOutOneBlockAsyncDFSOutput(conf, dfs, client, namenode, clientName, src,
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
