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

import static org.apache.hadoop.hbase.shaded.io.netty.handler.timeout.IdleState.READER_IDLE;
import static org.apache.hadoop.hbase.shaded.io.netty.handler.timeout.IdleState.WRITER_IDLE;
import static org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper.HEART_BEAT_SEQNO;
import static org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper.READ_TIMEOUT;
import static org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper.completeFile;
import static org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper.endFileLease;
import static org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper.getStatus;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;


import org.apache.hadoop.hbase.shaded.io.netty.buffer.ByteBuf;
import org.apache.hadoop.hbase.shaded.io.netty.buffer.ByteBufAllocator;
import org.apache.hadoop.hbase.shaded.io.netty.channel.Channel;
import org.apache.hadoop.hbase.shaded.io.netty.channel.ChannelHandler.Sharable;
import org.apache.hadoop.hbase.shaded.io.netty.channel.ChannelHandlerContext;
import org.apache.hadoop.hbase.shaded.io.netty.channel.EventLoop;
import org.apache.hadoop.hbase.shaded.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.hadoop.hbase.shaded.io.netty.handler.codec.protobuf.ProtobufDecoder;
import org.apache.hadoop.hbase.shaded.io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.apache.hadoop.hbase.shaded.io.netty.handler.timeout.IdleStateEvent;
import org.apache.hadoop.hbase.shaded.io.netty.handler.timeout.IdleStateHandler;
import org.apache.hadoop.hbase.shaded.io.netty.util.concurrent.Future;
import org.apache.hadoop.hbase.shaded.io.netty.util.concurrent.Promise;
import org.apache.hadoop.hbase.shaded.io.netty.util.concurrent.PromiseCombiner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.Encryptor;
import org.apache.hadoop.fs.Path;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper.CancelOnClose;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.PipelineAckProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.util.DataChecksum;

import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;

/**
 * An asynchronous HDFS output stream implementation which fans out data to datanode and only
 * supports writing file with only one block.
 * <p>
 * Use the createOutput method in {@link FanOutOneBlockAsyncDFSOutputHelper} to create. The mainly
 * usage of this class is implementing WAL, so we only expose a little HDFS configurations in the
 * method. And we place it here under util package because we want to make it independent of WAL
 * implementation thus easier to move it to HDFS project finally.
 * <p>
 * Note that, all connections to datanode will run in the same {@link EventLoop} which means we only
 * need one thread here. But be careful, we do some blocking operations in {@link #close()} and
 * {@link #recoverAndClose(CancelableProgressable)} methods, so do not call them inside
 * {@link EventLoop}. And for {@link #write(byte[])} {@link #write(byte[], int, int)},
 * {@link #buffered()} and {@link #flush(boolean)}, if you call them outside {@link EventLoop},
 * there will be an extra context-switch.
 * <p>
 * Advantages compare to DFSOutputStream:
 * <ol>
 * <li>The fan out mechanism. This will reduce the latency.</li>
 * <li>The asynchronous WAL could also run in the same EventLoop, we could just call write and flush
 * inside the EventLoop thread, so generally we only have one thread to do all the things.</li>
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

  private final FSUtils fsUtils;

  private final DistributedFileSystem dfs;

  private final DFSClient client;

  private final ClientProtocol namenode;

  private final String clientName;

  private final String src;

  private final long fileId;

  private final LocatedBlock locatedBlock;

  private final Encryptor encryptor;

  private final EventLoop eventLoop;

  private final List<Channel> datanodeList;

  private final DataChecksum summer;

  private final int maxDataLen;

  private final ByteBufAllocator alloc;

  private static final class Callback {

    private final Promise<Void> promise;

    private final long ackedLength;

    private final Set<Channel> unfinishedReplicas;

    public Callback(Promise<Void> promise, long ackedLength, Collection<Channel> replicas) {
      this.promise = promise;
      this.ackedLength = ackedLength;
      if (replicas.isEmpty()) {
        this.unfinishedReplicas = Collections.emptySet();
      } else {
        this.unfinishedReplicas =
            Collections.newSetFromMap(new IdentityHashMap<Channel, Boolean>(replicas.size()));
        this.unfinishedReplicas.addAll(replicas);
      }
    }
  }

  private final Deque<Callback> waitingAckQueue = new ArrayDeque<>();

  // this could be different from acked block length because a packet can not start at the middle of
  // a chunk.
  private long nextPacketOffsetInBlock = 0L;

  private long nextPacketSeqno = 0L;

  private ByteBuf buf;
  // buf's initial capacity - 4KB
  private int capacity = 4 * 1024;

  // LIMIT is 128MB
  private static final int LIMIT = 128 * 1024 * 1024;

  private enum State {
    STREAMING, CLOSING, BROKEN, CLOSED
  }

  private State state;

  private void completed(Channel channel) {
    if (waitingAckQueue.isEmpty()) {
      return;
    }
    for (Callback c : waitingAckQueue) {
      if (c.unfinishedReplicas.remove(channel)) {
        if (c.unfinishedReplicas.isEmpty()) {
          c.promise.trySuccess(null);
          // since we will remove the Callback entry from waitingAckQueue if its unfinishedReplicas
          // is empty, so this could only happen at the head of waitingAckQueue, so we just call
          // removeFirst here.
          waitingAckQueue.removeFirst();
          // also wake up flush requests which have the same length.
          for (Callback cb; (cb = waitingAckQueue.peekFirst()) != null;) {
            if (cb.ackedLength == c.ackedLength) {
              cb.promise.trySuccess(null);
              waitingAckQueue.removeFirst();
            } else {
              break;
            }
          }
        }
        return;
      }
    }
  }

  private void failed(Channel channel, Supplier<Throwable> errorSupplier) {
    if (state == State.BROKEN || state == State.CLOSED) {
      return;
    }
    if (state == State.CLOSING) {
      Callback c = waitingAckQueue.peekFirst();
      if (c == null || !c.unfinishedReplicas.contains(channel)) {
        // nothing, the endBlock request has already finished.
        return;
      }
    }
    // disable further write, and fail all pending ack.
    state = State.BROKEN;
    Throwable error = errorSupplier.get();
    waitingAckQueue.stream().forEach(c -> c.promise.tryFailure(error));
    waitingAckQueue.clear();
    datanodeList.forEach(ch -> ch.close());
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
        failed(ctx.channel(), () -> new IOException("Bad response " + reply + " for block "
            + locatedBlock.getBlock() + " from datanode " + ctx.channel().remoteAddress()));
        return;
      }
      if (PipelineAck.isRestartOOBStatus(reply)) {
        failed(ctx.channel(), () -> new IOException("Restart response " + reply + " for block "
            + locatedBlock.getBlock() + " from datanode " + ctx.channel().remoteAddress()));
        return;
      }
      if (ack.getSeqno() == HEART_BEAT_SEQNO) {
        return;
      }
      completed(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
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
    for (Channel ch : datanodeList) {
      ch.pipeline().addLast(
        new IdleStateHandler(timeoutMs, timeoutMs / 2, 0, TimeUnit.MILLISECONDS),
        new ProtobufVarint32FrameDecoder(),
        new ProtobufDecoder(PipelineAckProto.getDefaultInstance()), ackHandler);
      ch.config().setAutoRead(true);
    }
  }

  FanOutOneBlockAsyncDFSOutput(Configuration conf, FSUtils fsUtils, DistributedFileSystem dfs,
      DFSClient client, ClientProtocol namenode, String clientName, String src, long fileId,
      LocatedBlock locatedBlock, Encryptor encryptor, EventLoop eventLoop,
      List<Channel> datanodeList, DataChecksum summer, ByteBufAllocator alloc) {
    this.conf = conf;
    this.fsUtils = fsUtils;
    this.dfs = dfs;
    this.client = client;
    this.namenode = namenode;
    this.fileId = fileId;
    this.clientName = clientName;
    this.src = src;
    this.locatedBlock = locatedBlock;
    this.encryptor = encryptor;
    this.eventLoop = eventLoop;
    this.datanodeList = datanodeList;
    this.summer = summer;
    this.maxDataLen = MAX_DATA_LEN - (MAX_DATA_LEN % summer.getBytesPerChecksum());
    this.alloc = alloc;
    this.buf = alloc.directBuffer(capacity);
    this.state = State.STREAMING;
    setupReceiver(conf.getInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY, READ_TIMEOUT));
  }

  private void writeInt0(int i) {
    buf.ensureWritable(4);
    buf.writeInt(i);
  }

  @Override
  public void writeInt(int i) {
    if (eventLoop.inEventLoop()) {
      writeInt0(i);
    } else {
      eventLoop.submit(() -> writeInt0(i));
    }
  }

  private void write0(ByteBuffer bb) {
    buf.ensureWritable(bb.remaining());
    buf.writeBytes(bb);
  }

  @Override
  public void write(ByteBuffer bb) {
    if (eventLoop.inEventLoop()) {
      write0(bb);
    } else {
      eventLoop.submit(() -> write0(bb));
    }
  }

  @Override
  public void write(byte[] b) {
    write(b, 0, b.length);
  }

  private void write0(byte[] b, int off, int len) {
    buf.ensureWritable(len);
    buf.writeBytes(b, off, len);
  }

  @Override
  public void write(byte[] b, int off, int len) {
    if (eventLoop.inEventLoop()) {
      write0(b, off, len);
    } else {
      eventLoop.submit(() -> write0(b, off, len)).syncUninterruptibly();
    }
  }

  @Override
  public int buffered() {
    if (eventLoop.inEventLoop()) {
      return buf.readableBytes();
    } else {
      return eventLoop.submit(() -> buf.readableBytes()).syncUninterruptibly().getNow().intValue();
    }
  }

  @Override
  public DatanodeInfo[] getPipeline() {
    return locatedBlock.getLocations();
  }

  private Promise<Void> flushBuffer(ByteBuf dataBuf, long nextPacketOffsetInBlock,
      boolean syncBlock) {
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

    long ackedLength = nextPacketOffsetInBlock + dataLen;
    Promise<Void> promise = eventLoop.<Void> newPromise().addListener(future -> {
      if (future.isSuccess()) {
        locatedBlock.getBlock().setNumBytes(ackedLength);
      }
    });
    waitingAckQueue.addLast(new Callback(promise, ackedLength, datanodeList));
    for (Channel ch : datanodeList) {
      ch.write(headerBuf.duplicate().retain());
      ch.write(checksumBuf.duplicate().retain());
      ch.writeAndFlush(dataBuf.duplicate().retain());
    }
    checksumBuf.release();
    headerBuf.release();
    dataBuf.release();
    nextPacketSeqno++;
    return promise;
  }

  private void flush0(CompletableFuture<Long> future, boolean syncBlock) {
    if (state != State.STREAMING) {
      future.completeExceptionally(new IOException("stream already broken"));
      return;
    }
    int dataLen = buf.readableBytes();
    if (encryptor != null) {
      ByteBuf encryptBuf = alloc.directBuffer(dataLen);
      try {
        encryptor.encrypt(buf.nioBuffer(buf.readerIndex(), dataLen),
          encryptBuf.nioBuffer(0, dataLen));
      } catch (IOException e) {
        encryptBuf.release();
        future.completeExceptionally(e);
        return;
      }
      encryptBuf.writerIndex(dataLen);
      buf.release();
      buf = encryptBuf;
    }
    long lengthAfterFlush = nextPacketOffsetInBlock + dataLen;
    if (lengthAfterFlush == locatedBlock.getBlock().getNumBytes()) {
      // no new data, just return
      future.complete(locatedBlock.getBlock().getNumBytes());
      return;
    }
    Callback c = waitingAckQueue.peekLast();
    if (c != null && lengthAfterFlush == c.ackedLength) {
      // just append it to the tail of waiting ack queue,, do not issue new hflush request.
      waitingAckQueue.addLast(new Callback(eventLoop.<Void> newPromise().addListener(f -> {
        if (f.isSuccess()) {
          future.complete(lengthAfterFlush);
        } else {
          future.completeExceptionally(f.cause());
        }
      }), lengthAfterFlush, Collections.<Channel> emptyList()));
      return;
    }
    Promise<Void> promise;
    if (dataLen > maxDataLen) {
      // We need to write out the data by multiple packets as the max packet allowed is 16M.
      PromiseCombiner combiner = new PromiseCombiner();
      long nextSubPacketOffsetInBlock = nextPacketOffsetInBlock;
      for (int remaining = dataLen; remaining > 0;) {
        int toWriteDataLen = Math.min(remaining, maxDataLen);
        combiner.add((Future<Void>) flushBuffer(buf.readRetainedSlice(toWriteDataLen),
          nextSubPacketOffsetInBlock, syncBlock));
        nextSubPacketOffsetInBlock += toWriteDataLen;
        remaining -= toWriteDataLen;
      }
      promise = eventLoop.newPromise();
      combiner.finish(promise);
    } else {
      promise = flushBuffer(buf.retain(), nextPacketOffsetInBlock, syncBlock);
    }
    promise.addListener(f -> {
      if (f.isSuccess()) {
        future.complete(lengthAfterFlush);
      } else {
        future.completeExceptionally(f.cause());
      }
    });
    int trailingPartialChunkLen = dataLen % summer.getBytesPerChecksum();
    ByteBuf newBuf = alloc.directBuffer(guess(dataLen)).ensureWritable(trailingPartialChunkLen);
    if (trailingPartialChunkLen != 0) {
      buf.readerIndex(dataLen - trailingPartialChunkLen).readBytes(newBuf, trailingPartialChunkLen);
    }
    buf.release();
    this.buf = newBuf;
    nextPacketOffsetInBlock += dataLen - trailingPartialChunkLen;
  }

  /**
   * Flush the buffer out to datanodes.
   * @param syncBlock will call hsync if true, otherwise hflush.
   * @return A CompletableFuture that hold the acked length after flushing.
   */
  public CompletableFuture<Long> flush(boolean syncBlock) {
    CompletableFuture<Long> future = new CompletableFuture<>();
    if (eventLoop.inEventLoop()) {
      flush0(future, syncBlock);
    } else {
      eventLoop.execute(() -> flush0(future, syncBlock));
    }
    return future;
  }

  private void endBlock(Promise<Void> promise, long size) {
    if (state != State.STREAMING) {
      promise.tryFailure(new IOException("stream already broken"));
      return;
    }
    if (!waitingAckQueue.isEmpty()) {
      promise.tryFailure(new IllegalStateException("should call flush first before calling close"));
      return;
    }
    state = State.CLOSING;
    PacketHeader header = new PacketHeader(4, size, nextPacketSeqno, true, 0, false);
    buf.release();
    buf = null;
    int headerLen = header.getSerializedSize();
    ByteBuf headerBuf = alloc.directBuffer(headerLen);
    header.putInBuffer(headerBuf.nioBuffer(0, headerLen));
    headerBuf.writerIndex(headerLen);
    waitingAckQueue.add(new Callback(promise, size, datanodeList));
    datanodeList.forEach(ch -> ch.writeAndFlush(headerBuf.duplicate().retain()));
    headerBuf.release();
  }

  /**
   * The close method when error occurred. Now we just call recoverFileLease.
   */
  @Override
  public void recoverAndClose(CancelableProgressable reporter) throws IOException {
    assert !eventLoop.inEventLoop();
    datanodeList.forEach(ch -> ch.closeFuture().awaitUninterruptibly());
    endFileLease(client, fileId);
    fsUtils.recoverFileLease(dfs, new Path(src), conf,
      reporter == null ? new CancelOnClose(client) : reporter);
  }

  /**
   * End the current block and complete file at namenode. You should call
   * {@link #recoverAndClose(CancelableProgressable)} if this method throws an exception.
   */
  @Override
  public void close() throws IOException {
    assert !eventLoop.inEventLoop();
    Promise<Void> promise = eventLoop.newPromise();
    eventLoop.execute(() -> endBlock(promise, nextPacketOffsetInBlock + buf.readableBytes()));
    promise.addListener(f -> datanodeList.forEach(ch -> ch.close())).syncUninterruptibly();
    datanodeList.forEach(ch -> ch.closeFuture().awaitUninterruptibly());
    completeFile(client, namenode, src, clientName, locatedBlock.getBlock(), fileId);
  }

  @VisibleForTesting
  int guess(int bytesWritten) {
    // if the bytesWritten is greater than the current capacity
    // always increase the capacity in powers of 2.
    if (bytesWritten > this.capacity) {
      // Ensure we don't cross the LIMIT
      if ((this.capacity << 1) <= LIMIT) {
        // increase the capacity in the range of power of 2
        this.capacity = this.capacity << 1;
      }
    } else {
      // if we see that the bytesWritten is lesser we could again decrease
      // the capacity by dividing it by 2 if the bytesWritten is satisfied by
      // that reduction
      if ((this.capacity >> 1) >= bytesWritten) {
        this.capacity = this.capacity >> 1;
      }
    }
    return this.capacity;
  }
}
