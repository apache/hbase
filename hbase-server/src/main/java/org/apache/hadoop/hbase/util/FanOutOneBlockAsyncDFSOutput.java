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

import static org.apache.hadoop.hbase.util.FanOutOneBlockAsyncDFSOutputHelper.HEART_BEAT_SEQNO;
import static org.apache.hadoop.hbase.util.FanOutOneBlockAsyncDFSOutputHelper.completeFile;
import static org.apache.hadoop.hbase.util.FanOutOneBlockAsyncDFSOutputHelper.endFileLease;
import static org.apache.hadoop.hbase.util.FanOutOneBlockAsyncDFSOutputHelper.getStatus;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.CompletionHandler;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.FanOutOneBlockAsyncDFSOutputHelper.CancelOnClose;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.PipelineAckProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.util.DataChecksum;

import com.google.common.base.Supplier;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

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
 * {@link #buffered()} and {@link #flush(Object, CompletionHandler, boolean)}, if you call them
 * outside {@link EventLoop}, there will be an extra context-switch.
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
public class FanOutOneBlockAsyncDFSOutput implements Closeable {

  private final Configuration conf;

  private final FSUtils fsUtils;

  private final DistributedFileSystem dfs;

  private final DFSClient client;

  private final ClientProtocol namenode;

  private final String clientName;

  private final String src;

  private final long fileId;

  private final LocatedBlock locatedBlock;

  private final EventLoop eventLoop;

  private final List<Channel> datanodeList;

  private final DataChecksum summer;

  private final ByteBufAllocator alloc;

  private static final class Callback {

    public final Promise<Void> promise;

    public final long ackedLength;

    public final Set<Channel> unfinishedReplicas;

    public Callback(Promise<Void> promise, long ackedLength, Collection<Channel> replicas) {
      this.promise = promise;
      this.ackedLength = ackedLength;
      if (replicas.isEmpty()) {
        this.unfinishedReplicas = Collections.emptySet();
      } else {
        this.unfinishedReplicas = Collections
            .newSetFromMap(new IdentityHashMap<Channel, Boolean>(replicas.size()));
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
    for (Callback c : waitingAckQueue) {
      c.promise.tryFailure(error);
    }
    waitingAckQueue.clear();
    for (Channel ch : datanodeList) {
      ch.close();
    }
  }

  private void setupReceiver(final int timeoutMs) {
    SimpleChannelInboundHandler<PipelineAckProto> ackHandler = new SimpleChannelInboundHandler<PipelineAckProto>() {

      @Override
      public boolean isSharable() {
        return true;
      }

      @Override
      protected void channelRead0(final ChannelHandlerContext ctx, PipelineAckProto ack)
          throws Exception {
        final Status reply = getStatus(ack);
        if (reply != Status.SUCCESS) {
          failed(ctx.channel(), new Supplier<Throwable>() {

            @Override
            public Throwable get() {
              return new IOException("Bad response " + reply + " for block "
                  + locatedBlock.getBlock() + " from datanode " + ctx.channel().remoteAddress());
            }
          });
          return;
        }
        if (PipelineAck.isRestartOOBStatus(reply)) {
          failed(ctx.channel(), new Supplier<Throwable>() {

            @Override
            public Throwable get() {
              return new IOException("Restart response " + reply + " for block "
                  + locatedBlock.getBlock() + " from datanode " + ctx.channel().remoteAddress());
            }
          });
          return;
        }
        if (ack.getSeqno() == HEART_BEAT_SEQNO) {
          return;
        }
        completed(ctx.channel());
      }

      @Override
      public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        failed(ctx.channel(), new Supplier<Throwable>() {

          @Override
          public Throwable get() {
            return new IOException("Connection to " + ctx.channel().remoteAddress() + " closed");
          }
        });
      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, final Throwable cause)
          throws Exception {
        failed(ctx.channel(), new Supplier<Throwable>() {

          @Override
          public Throwable get() {
            return cause;
          }
        });
      }

      @Override
      public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
          IdleStateEvent e = (IdleStateEvent) evt;
          if (e.state() == IdleState.READER_IDLE) {
            failed(ctx.channel(), new Supplier<Throwable>() {

              @Override
              public Throwable get() {
                return new IOException("Timeout(" + timeoutMs + "ms) waiting for response");
              }
            });
          } else if (e.state() == IdleState.WRITER_IDLE) {
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

    };
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
      LocatedBlock locatedBlock, EventLoop eventLoop, List<Channel> datanodeList,
      DataChecksum summer, ByteBufAllocator alloc) {
    this.conf = conf;
    this.fsUtils = fsUtils;
    this.dfs = dfs;
    this.client = client;
    this.namenode = namenode;
    this.fileId = fileId;
    this.clientName = clientName;
    this.src = src;
    this.locatedBlock = locatedBlock;
    this.eventLoop = eventLoop;
    this.datanodeList = datanodeList;
    this.summer = summer;
    this.alloc = alloc;
    this.buf = alloc.directBuffer();
    this.state = State.STREAMING;
    setupReceiver(conf.getInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY, HdfsServerConstants.READ_TIMEOUT));
  }

  /**
   * Just call write(b, 0, b.length).
   * @see #write(byte[], int, int)
   */
  public void write(byte[] b) {
    write(b, 0, b.length);
  }

  /**
   * Copy the data into the buffer. Note that you need to call
   * {@link #flush(Object, CompletionHandler, boolean)} to flush the buffer manually.
   */
  public void write(final byte[] b, final int off, final int len) {
    if (eventLoop.inEventLoop()) {
      buf.ensureWritable(len).writeBytes(b, off, len);
    } else {
      eventLoop.submit(new Runnable() {

        @Override
        public void run() {
          buf.ensureWritable(len).writeBytes(b, off, len);
        }
      }).syncUninterruptibly();
    }
  }

  /**
   * Return the current size of buffered data.
   */
  public int buffered() {
    if (eventLoop.inEventLoop()) {
      return buf.readableBytes();
    } else {
      return eventLoop.submit(new Callable<Integer>() {

        @Override
        public Integer call() throws Exception {
          return buf.readableBytes();
        }
      }).syncUninterruptibly().getNow().intValue();
    }
  }

  public DatanodeInfo[] getPipeline() {
    return locatedBlock.getLocations();
  }

  private <A> void flush0(final A attachment, final CompletionHandler<Long, ? super A> handler,
      boolean syncBlock) {
    if (state != State.STREAMING) {
      handler.failed(new IOException("stream already broken"), attachment);
      return;
    }
    int dataLen = buf.readableBytes();
    final long ackedLength = nextPacketOffsetInBlock + dataLen;
    if (ackedLength == locatedBlock.getBlock().getNumBytes()) {
      // no new data, just return
      handler.completed(locatedBlock.getBlock().getNumBytes(), attachment);
      return;
    }
    Promise<Void> promise = eventLoop.newPromise();
    promise.addListener(new FutureListener<Void>() {

      @Override
      public void operationComplete(Future<Void> future) throws Exception {
        if (future.isSuccess()) {
          locatedBlock.getBlock().setNumBytes(ackedLength);
          handler.completed(ackedLength, attachment);
        } else {
          handler.failed(future.cause(), attachment);
        }
      }
    });
    Callback c = waitingAckQueue.peekLast();
    if (c != null && ackedLength == c.ackedLength) {
      // just append it to the tail of waiting ack queue,, do not issue new hflush request.
      waitingAckQueue
          .addLast(new Callback(promise, ackedLength, Collections.<Channel> emptyList()));
      return;
    }
    int chunkLen = summer.getBytesPerChecksum();
    int trailingPartialChunkLen = dataLen % chunkLen;
    int numChecks = dataLen / chunkLen + (trailingPartialChunkLen != 0 ? 1 : 0);
    int checksumLen = numChecks * summer.getChecksumSize();
    ByteBuf checksumBuf = alloc.directBuffer(checksumLen);
    summer.calculateChunkedSums(buf.nioBuffer(), checksumBuf.nioBuffer(0, checksumLen));
    checksumBuf.writerIndex(checksumLen);
    PacketHeader header = new PacketHeader(4 + checksumLen + dataLen, nextPacketOffsetInBlock,
        nextPacketSeqno, false, dataLen, syncBlock);
    int headerLen = header.getSerializedSize();
    ByteBuf headerBuf = alloc.buffer(headerLen);
    header.putInBuffer(headerBuf.nioBuffer(0, headerLen));
    headerBuf.writerIndex(headerLen);

    waitingAckQueue.addLast(new Callback(promise, ackedLength, datanodeList));
    for (Channel ch : datanodeList) {
      ch.write(headerBuf.duplicate().retain());
      ch.write(checksumBuf.duplicate().retain());
      ch.writeAndFlush(buf.duplicate().retain());
    }
    checksumBuf.release();
    headerBuf.release();
    ByteBuf newBuf = alloc.directBuffer().ensureWritable(trailingPartialChunkLen);
    if (trailingPartialChunkLen != 0) {
      buf.readerIndex(dataLen - trailingPartialChunkLen).readBytes(newBuf, trailingPartialChunkLen);
    }
    buf.release();
    this.buf = newBuf;
    nextPacketOffsetInBlock += dataLen - trailingPartialChunkLen;
    nextPacketSeqno++;
  }

  /**
   * Flush the buffer out to datanodes.
   * @param attachment will be passed to handler when completed.
   * @param handler will set the acked length as result when completed.
   * @param syncBlock will call hsync if true, otherwise hflush.
   */
  public <A> void flush(final A attachment, final CompletionHandler<Long, ? super A> handler,
      final boolean syncBlock) {
    if (eventLoop.inEventLoop()) {
      flush0(attachment, handler, syncBlock);
    } else {
      eventLoop.execute(new Runnable() {

        @Override
        public void run() {
          flush0(attachment, handler, syncBlock);
        }
      });
    }
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
    ByteBuf headerBuf = alloc.buffer(headerLen);
    header.putInBuffer(headerBuf.nioBuffer(0, headerLen));
    headerBuf.writerIndex(headerLen);
    waitingAckQueue.add(new Callback(promise, size, datanodeList));
    for (Channel ch : datanodeList) {
      ch.writeAndFlush(headerBuf.duplicate().retain());
    }
    headerBuf.release();
  }

  /**
   * The close method when error occurred. Now we just call recoverFileLease.
   */
  public void recoverAndClose(CancelableProgressable reporter) throws IOException {
    assert !eventLoop.inEventLoop();
    for (Channel ch : datanodeList) {
      ch.closeFuture().awaitUninterruptibly();
    }
    endFileLease(client, src, fileId);
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
    final Promise<Void> promise = eventLoop.newPromise();
    eventLoop.execute(new Runnable() {

      @Override
      public void run() {
        endBlock(promise, nextPacketOffsetInBlock + buf.readableBytes());
      }
    });
    promise.addListener(new FutureListener<Void>() {

      @Override
      public void operationComplete(Future<Void> future) throws Exception {
        for (Channel ch : datanodeList) {
          ch.close();
        }
      }
    }).syncUninterruptibly();
    for (Channel ch : datanodeList) {
      ch.closeFuture().awaitUninterruptibly();
    }
    completeFile(client, namenode, src, clientName, locatedBlock.getBlock(), fileId);
  }
}
