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

import static org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper.HEART_BEAT_SEQNO;
import static org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper.READ_TIMEOUT;
import static org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper.completeFile;
import static org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper.endFileLease;
import static org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper.getStatus;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.apache.hbase.thirdparty.io.netty.handler.timeout.IdleState.READER_IDLE;
import static org.apache.hbase.thirdparty.io.netty.handler.timeout.IdleState.WRITER_IDLE;

import com.google.errorprone.annotations.RestrictedApi;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.Encryptor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper.CancelOnClose;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.RecoverLeaseFSUtils;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.PipelineAckProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.util.DataChecksum;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.base.Throwables;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBufAllocator;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandler.Sharable;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelId;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelOutboundInvoker;
import org.apache.hbase.thirdparty.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.hbase.thirdparty.io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.apache.hbase.thirdparty.io.netty.handler.timeout.IdleStateEvent;
import org.apache.hbase.thirdparty.io.netty.handler.timeout.IdleStateHandler;

/**
 * An asynchronous HDFS output stream implementation which fans out data to datanode and only
 * supports writing file with only one block.
 * <p>
 * Use the createOutput method in {@link FanOutOneBlockAsyncDFSOutputHelper} to create. The mainly
 * usage of this class is implementing WAL, so we only expose a little HDFS configurations in the
 * method. And we place it here under io package because we want to make it independent of WAL
 * implementation thus easier to move it to HDFS project finally.
 * <p>
 * Note that, although we support pipelined flush, i.e, write new data and then flush before the
 * previous flush succeeds, the implementation is not thread safe, so you should not call its
 * methods concurrently.
 * <p>
 * Advantages compare to DFSOutputStream:
 * <ol>
 * <li>The fan out mechanism. This will reduce the latency.</li>
 * <li>Fail-fast when connection to datanode error. The WAL implementation could open new writer
 * ASAP.</li>
 * <li>We could benefit from netty's ByteBuf management mechanism.</li>
 * </ol>
 */
@InterfaceAudience.Private
public class FanOutOneBlockAsyncDFSOutput implements AsyncFSOutput {

  // The MAX_PACKET_SIZE is 16MB but it include the header size and checksum size. So here we set a
  // smaller limit for data size.
  private static final int MAX_DATA_LEN = 12 * 1024 * 1024;

  private final Configuration conf;

  private final DistributedFileSystem dfs;

  private final DFSClient client;

  private final ClientProtocol namenode;

  private final String clientName;

  private final String src;

  private final long fileId;

  private final ExtendedBlock block;

  private final DatanodeInfo[] locations;

  private final Encryptor encryptor;

  private final Map<Channel, DatanodeInfo> datanodeInfoMap;

  private final DataChecksum summer;

  private final int maxDataLen;

  private final ByteBufAllocator alloc;

  private static final class Callback {

    private final CompletableFuture<Long> future;

    private final long ackedLength;

    // should be backed by a thread safe collection
    private final Set<ChannelId> unfinishedReplicas;
    private final long packetDataLen;
    private final long flushTimestamp;
    private long lastAckTimestamp = -1;

    public Callback(CompletableFuture<Long> future, long ackedLength,
      final Collection<Channel> replicas, long packetDataLen) {
      this.future = future;
      this.ackedLength = ackedLength;
      this.packetDataLen = packetDataLen;
      this.flushTimestamp = EnvironmentEdgeManager.currentTime();
      if (replicas.isEmpty()) {
        this.unfinishedReplicas = Collections.emptySet();
      } else {
        this.unfinishedReplicas =
          Collections.newSetFromMap(new ConcurrentHashMap<ChannelId, Boolean>(replicas.size()));
        replicas.stream().map(Channel::id).forEachOrdered(unfinishedReplicas::add);
      }
    }
  }

  private final ConcurrentLinkedDeque<Callback> waitingAckQueue = new ConcurrentLinkedDeque<>();

  private volatile long ackedBlockLength = 0L;

  // this could be different from acked block length because a packet can not start at the middle of
  // a chunk.
  private long nextPacketOffsetInBlock = 0L;

  // the length of the trailing partial chunk, this is because the packet start offset must be
  // aligned with the length of checksum chunk so we need to resend the same data.
  private int trailingPartialChunkLength = 0;

  private long nextPacketSeqno = 0L;

  private ByteBuf buf;

  private final SendBufSizePredictor sendBufSizePRedictor = new SendBufSizePredictor();

  // State for connections to DN
  private enum State {
    STREAMING, CLOSING, BROKEN, CLOSED
  }

  private volatile State state;

  private final StreamSlowMonitor streamSlowMonitor;

  // all lock-free to make it run faster
  private void completed(Channel channel) {
    for (Iterator<Callback> iter = waitingAckQueue.iterator(); iter.hasNext();) {
      Callback c = iter.next();
      // if the current unfinished replicas does not contain us then it means that we have already
      // acked this one, let's iterate to find the one we have not acked yet.
      if (c.unfinishedReplicas.remove(channel.id())) {
        long current = EnvironmentEdgeManager.currentTime();
        streamSlowMonitor.checkProcessTimeAndSpeed(datanodeInfoMap.get(channel), c.packetDataLen,
            current - c.flushTimestamp, c.lastAckTimestamp, c.unfinishedReplicas.size());
        c.lastAckTimestamp = current;
        if (c.unfinishedReplicas.isEmpty()) {
          // we need to remove first before complete the future. It is possible that after we
          // complete the future the upper layer will call close immediately before we remove the
          // entry from waitingAckQueue and lead to an IllegalStateException. And also set the
          // ackedBlockLength first otherwise we may use a wrong length to commit the block. This
          // may lead to multiple remove and assign but is OK. The semantic of iter.remove is
          // removing the entry returned by calling previous next, so if the entry has already been
          // removed then it is a no-op, and for the assign, the values are the same so no problem.
          iter.remove();
          ackedBlockLength = c.ackedLength;
          // the future.complete check is to confirm that we are the only one who grabbed the work,
          // otherwise just give up and return.
          if (c.future.complete(c.ackedLength)) {
            // also wake up flush requests which have the same length.
            while (iter.hasNext()) {
              Callback maybeDummyCb = iter.next();
              if (maybeDummyCb.ackedLength == c.ackedLength) {
                iter.remove();
                maybeDummyCb.future.complete(c.ackedLength);
              } else {
                break;
              }
            }
          }
        }
        return;
      }
    }
  }

  // this usually does not happen which means it is not on the critical path so make it synchronized
  // so that the implementation will not burn up our brain as there are multiple state changes and
  // checks.
  private synchronized void failed(Channel channel, Supplier<Throwable> errorSupplier) {
    if (state == State.CLOSED) {
      return;
    }
    if (state == State.BROKEN) {
      failWaitingAckQueue(channel, errorSupplier);
      return;
    }
    if (state == State.CLOSING) {
      Callback c = waitingAckQueue.peekFirst();
      if (c == null || !c.unfinishedReplicas.contains(channel.id())) {
        // nothing, the endBlock request has already finished.
        return;
      }
    }
    // disable further write, and fail all pending ack.
    state = State.BROKEN;
    failWaitingAckQueue(channel, errorSupplier);
    datanodeInfoMap.keySet().forEach(ChannelOutboundInvoker::close);
  }

  private void failWaitingAckQueue(Channel channel, Supplier<Throwable> errorSupplier) {
    Throwable error = errorSupplier.get();
    for (Iterator<Callback> iter = waitingAckQueue.iterator(); iter.hasNext();) {
      Callback c = iter.next();
      // find the first sync request which we have not acked yet and fail all the request after it.
      if (!c.unfinishedReplicas.contains(channel.id())) {
        continue;
      }
      for (;;) {
        c.future.completeExceptionally(error);
        if (!iter.hasNext()) {
          break;
        }
        c = iter.next();
      }
      break;
    }
  }

  @Sharable
  private final class AckHandler extends SimpleChannelInboundHandler<PipelineAckProto> {

    private final int timeoutMs;

    public AckHandler(int timeoutMs) {
      this.timeoutMs = timeoutMs;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, PipelineAckProto ack) throws Exception {
      Status reply = getStatus(ack);
      if (reply != Status.SUCCESS) {
        failed(ctx.channel(), () -> new IOException("Bad response " + reply + " for block " +
          block + " from datanode " + ctx.channel().remoteAddress()));
        return;
      }
      if (PipelineAck.isRestartOOBStatus(reply)) {
        failed(ctx.channel(), () -> new IOException("Restart response " + reply + " for block " +
          block + " from datanode " + ctx.channel().remoteAddress()));
        return;
      }
      if (ack.getSeqno() == HEART_BEAT_SEQNO) {
        return;
      }
      completed(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      if (state == State.CLOSED) {
        return;
      }
      failed(ctx.channel(),
        () -> new IOException("Connection to " + ctx.channel().remoteAddress() + " closed"));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      failed(ctx.channel(), () -> cause);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      if (evt instanceof IdleStateEvent) {
        IdleStateEvent e = (IdleStateEvent) evt;
        if (e.state() == READER_IDLE) {
          failed(ctx.channel(),
            () -> new IOException("Timeout(" + timeoutMs + "ms) waiting for response"));
        } else if (e.state() == WRITER_IDLE) {
          PacketHeader heartbeat = new PacketHeader(4, 0, HEART_BEAT_SEQNO, false, 0, false);
          int len = heartbeat.getSerializedSize();
          ByteBuf buf = alloc.buffer(len);
          heartbeat.putInBuffer(buf.nioBuffer(0, len));
          buf.writerIndex(len);
          ctx.channel().writeAndFlush(buf);
        }
        return;
      }
      super.userEventTriggered(ctx, evt);
    }
  }

  private void setupReceiver(int timeoutMs) {
    AckHandler ackHandler = new AckHandler(timeoutMs);
    for (Channel ch : datanodeInfoMap.keySet()) {
      ch.pipeline().addLast(
        new IdleStateHandler(timeoutMs, timeoutMs / 2, 0, TimeUnit.MILLISECONDS),
        new ProtobufVarint32FrameDecoder(),
        new ProtobufDecoder(PipelineAckProto.getDefaultInstance()), ackHandler);
      ch.config().setAutoRead(true);
    }
  }

  FanOutOneBlockAsyncDFSOutput(Configuration conf,DistributedFileSystem dfs,
      DFSClient client, ClientProtocol namenode, String clientName, String src, long fileId,
      LocatedBlock locatedBlock, Encryptor encryptor, Map<Channel, DatanodeInfo> datanodeInfoMap,
      DataChecksum summer, ByteBufAllocator alloc, StreamSlowMonitor streamSlowMonitor) {
    this.conf = conf;
    this.dfs = dfs;
    this.client = client;
    this.namenode = namenode;
    this.fileId = fileId;
    this.clientName = clientName;
    this.src = src;
    this.block = locatedBlock.getBlock();
    this.locations = locatedBlock.getLocations();
    this.encryptor = encryptor;
    this.datanodeInfoMap = datanodeInfoMap;
    this.summer = summer;
    this.maxDataLen = MAX_DATA_LEN - (MAX_DATA_LEN % summer.getBytesPerChecksum());
    this.alloc = alloc;
    this.buf = alloc.directBuffer(sendBufSizePRedictor.initialSize());
    this.state = State.STREAMING;
    setupReceiver(conf.getInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY, READ_TIMEOUT));
    this.streamSlowMonitor = streamSlowMonitor;
  }

  @Override
  public void writeInt(int i) {
    buf.ensureWritable(4);
    buf.writeInt(i);
  }

  @Override
  public void write(ByteBuffer bb) {
    buf.ensureWritable(bb.remaining());
    buf.writeBytes(bb);
  }

  @Override
  public void write(byte[] b) {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) {
    buf.ensureWritable(len);
    buf.writeBytes(b, off, len);
  }

  @Override
  public int buffered() {
    return buf.readableBytes();
  }

  @Override
  public DatanodeInfo[] getPipeline() {
    return locations;
  }

  private void flushBuffer(CompletableFuture<Long> future, ByteBuf dataBuf,
      long nextPacketOffsetInBlock, boolean syncBlock) {
    int dataLen = dataBuf.readableBytes();
    int chunkLen = summer.getBytesPerChecksum();
    int trailingPartialChunkLen = dataLen % chunkLen;
    int numChecks = dataLen / chunkLen + (trailingPartialChunkLen != 0 ? 1 : 0);
    int checksumLen = numChecks * summer.getChecksumSize();
    ByteBuf checksumBuf = alloc.directBuffer(checksumLen);
    summer.calculateChunkedSums(dataBuf.nioBuffer(), checksumBuf.nioBuffer(0, checksumLen));
    checksumBuf.writerIndex(checksumLen);
    PacketHeader header = new PacketHeader(4 + checksumLen + dataLen, nextPacketOffsetInBlock,
        nextPacketSeqno, false, dataLen, syncBlock);
    int headerLen = header.getSerializedSize();
    ByteBuf headerBuf = alloc.buffer(headerLen);
    header.putInBuffer(headerBuf.nioBuffer(0, headerLen));
    headerBuf.writerIndex(headerLen);
    Callback c = new Callback(future, nextPacketOffsetInBlock + dataLen,
        datanodeInfoMap.keySet(), dataLen);
    waitingAckQueue.addLast(c);
    // recheck again after we pushed the callback to queue
    if (state != State.STREAMING && waitingAckQueue.peekFirst() == c) {
      future.completeExceptionally(new IOException("stream already broken"));
      // it's the one we have just pushed or just a no-op
      waitingAckQueue.removeFirst();
      return;
    }
    // TODO: we should perhaps measure time taken per DN here;
    //       we could collect statistics per DN, and/or exclude bad nodes in createOutput.
    datanodeInfoMap.keySet().forEach(ch -> {
      ch.write(headerBuf.retainedDuplicate());
      ch.write(checksumBuf.retainedDuplicate());
      ch.writeAndFlush(dataBuf.retainedDuplicate());
    });
    checksumBuf.release();
    headerBuf.release();
    dataBuf.release();
    nextPacketSeqno++;
  }

  private void flush0(CompletableFuture<Long> future, boolean syncBlock) {
    if (state != State.STREAMING) {
      future.completeExceptionally(new IOException("stream already broken"));
      return;
    }
    int dataLen = buf.readableBytes();
    if (dataLen == trailingPartialChunkLength) {
      // no new data
      long lengthAfterFlush = nextPacketOffsetInBlock + dataLen;
      Callback lastFlush = waitingAckQueue.peekLast();
      if (lastFlush != null) {
        Callback c = new Callback(future, lengthAfterFlush, Collections.emptySet(), dataLen);
        waitingAckQueue.addLast(c);
        // recheck here if we have already removed the previous callback from the queue
        if (waitingAckQueue.peekFirst() == c) {
          // all previous callbacks have been removed
          // notice that this does mean we will always win here because the background thread may
          // have already started to mark the future here as completed in the completed or failed
          // methods but haven't removed it from the queue yet. That's also why the removeFirst
          // call below may be a no-op.
          if (state != State.STREAMING) {
            future.completeExceptionally(new IOException("stream already broken"));
          } else {
            future.complete(lengthAfterFlush);
          }
          // it's the one we have just pushed or just a no-op
          waitingAckQueue.removeFirst();
        }
      } else {
        // we must have acked all the data so the ackedBlockLength must be same with
        // lengthAfterFlush
        future.complete(lengthAfterFlush);
      }
      return;
    }

    if (encryptor != null) {
      ByteBuf encryptBuf = alloc.directBuffer(dataLen);
      buf.readBytes(encryptBuf, trailingPartialChunkLength);
      int toEncryptLength = dataLen - trailingPartialChunkLength;
      try {
        encryptor.encrypt(buf.nioBuffer(trailingPartialChunkLength, toEncryptLength),
          encryptBuf.nioBuffer(trailingPartialChunkLength, toEncryptLength));
      } catch (IOException e) {
        encryptBuf.release();
        future.completeExceptionally(e);
        return;
      }
      encryptBuf.writerIndex(dataLen);
      buf.release();
      buf = encryptBuf;
    }

    if (dataLen > maxDataLen) {
      // We need to write out the data by multiple packets as the max packet allowed is 16M.
      long nextSubPacketOffsetInBlock = nextPacketOffsetInBlock;
      for (int remaining = dataLen;;) {
        if (remaining < maxDataLen) {
          flushBuffer(future, buf.readRetainedSlice(remaining), nextSubPacketOffsetInBlock,
            syncBlock);
          break;
        } else {
          flushBuffer(new CompletableFuture<>(), buf.readRetainedSlice(maxDataLen),
            nextSubPacketOffsetInBlock, syncBlock);
          remaining -= maxDataLen;
          nextSubPacketOffsetInBlock += maxDataLen;
        }
      }
    } else {
      flushBuffer(future, buf.retain(), nextPacketOffsetInBlock, syncBlock);
    }
    trailingPartialChunkLength = dataLen % summer.getBytesPerChecksum();
    ByteBuf newBuf = alloc.directBuffer(sendBufSizePRedictor.guess(dataLen))
        .ensureWritable(trailingPartialChunkLength);
    if (trailingPartialChunkLength != 0) {
      buf.readerIndex(dataLen - trailingPartialChunkLength).readBytes(newBuf,
        trailingPartialChunkLength);
    }
    buf.release();
    this.buf = newBuf;
    nextPacketOffsetInBlock += dataLen - trailingPartialChunkLength;
  }

  /**
   * Flush the buffer out to datanodes.
   * @param syncBlock will call hsync if true, otherwise hflush.
   * @return A CompletableFuture that hold the acked length after flushing.
   */
  @Override
  public CompletableFuture<Long> flush(boolean syncBlock) {
    CompletableFuture<Long> future = new CompletableFuture<>();
    flush0(future, syncBlock);
    return future;
  }

  private void endBlock() throws IOException {
    Preconditions.checkState(waitingAckQueue.isEmpty(),
      "should call flush first before calling close");
    if (state != State.STREAMING) {
      throw new IOException("stream already broken");
    }
    state = State.CLOSING;
    long finalizedLength = ackedBlockLength;
    PacketHeader header = new PacketHeader(4, finalizedLength, nextPacketSeqno, true, 0, false);
    buf.release();
    buf = null;
    int headerLen = header.getSerializedSize();
    ByteBuf headerBuf = alloc.directBuffer(headerLen);
    header.putInBuffer(headerBuf.nioBuffer(0, headerLen));
    headerBuf.writerIndex(headerLen);
    CompletableFuture<Long> future = new CompletableFuture<>();
    waitingAckQueue.add(new Callback(future, finalizedLength, datanodeInfoMap.keySet(), 0));
    datanodeInfoMap.keySet().forEach(ch -> ch.writeAndFlush(headerBuf.retainedDuplicate()));
    headerBuf.release();
    try {
      future.get();
    } catch (InterruptedException e) {
      throw (IOException) new InterruptedIOException().initCause(e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      Throwables.propagateIfPossible(cause, IOException.class);
      throw new IOException(cause);
    }
  }

  /**
   * The close method when error occurred. Now we just call recoverFileLease.
   */
  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public void recoverAndClose(CancelableProgressable reporter) throws IOException {
    if (buf != null) {
      buf.release();
      buf = null;
    }
    datanodeInfoMap.keySet().forEach(ChannelOutboundInvoker::close);
    datanodeInfoMap.keySet().forEach(ch -> ch.closeFuture().awaitUninterruptibly());
    endFileLease(client, fileId);
    RecoverLeaseFSUtils.recoverFileLease(dfs, new Path(src), conf,
      reporter == null ? new CancelOnClose(client) : reporter);
  }

  /**
   * End the current block and complete file at namenode. You should call
   * {@link #recoverAndClose(CancelableProgressable)} if this method throws an exception.
   */
  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public void close() throws IOException {
    endBlock();
    state = State.CLOSED;
    datanodeInfoMap.keySet().forEach(ChannelOutboundInvoker::close);
    datanodeInfoMap.keySet().forEach(ch -> ch.closeFuture().awaitUninterruptibly());
    block.setNumBytes(ackedBlockLength);
    completeFile(client, namenode, src, clientName, block, fileId);
  }

  @Override
  public boolean isBroken() {
    return state == State.BROKEN;
  }

  @Override
  public long getSyncedLength() {
    return this.ackedBlockLength;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  Map<Channel, DatanodeInfo> getDatanodeInfoMap() {
    return this.datanodeInfoMap;
  }
}
