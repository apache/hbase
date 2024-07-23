/*
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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.io.asyncfs.AsyncFSOutput;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.wal.AsyncFSWALProvider;
import org.apache.hadoop.hbase.wal.WALProvider.AsyncWriter;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.SingleThreadEventExecutor;

/**
 * An asynchronous implementation of FSWAL.
 * <p>
 * Here 'waitingConsumePayloads' acts as the RingBuffer in FSHLog.
 * <p>
 * For append, we process it as follow:
 * <ol>
 * <li>In the caller thread(typically, in the rpc handler thread):
 * <ol>
 * <li>Insert the entry into 'waitingConsumePayloads'. Use ringbuffer sequence as txid.</li>
 * <li>Schedule the consumer task if needed. See {@link #shouldScheduleConsumer()} for more details.
 * </li>
 * </ol>
 * </li>
 * <li>In the consumer task(executed in a single threaded thread pool)
 * <ol>
 * <li>Poll the entry from {@link #waitingConsumePayloads} and insert it into
 * {@link #toWriteAppends}</li>
 * <li>Poll the entry from {@link #toWriteAppends}, append it to the AsyncWriter, and insert it into
 * {@link #unackedAppends}</li>
 * <li>If the buffered size reaches {@link #batchSize}, or there is a sync request, then we call
 * sync on the AsyncWriter.</li>
 * <li>In the callback methods:
 * <ul>
 * <li>If succeeded, poll the entry from {@link #unackedAppends} and drop it.</li>
 * <li>If failed, add all the entries in {@link #unackedAppends} back to {@link #toWriteAppends} and
 * wait for writing them again.</li>
 * </ul>
 * </li>
 * </ol>
 * </li>
 * </ol>
 * For sync, the processing stages are almost same. And different from FSHLog, we will open a new
 * writer and rewrite unacked entries to the new writer and sync again if we hit a sync error.
 * <p>
 * Here we only describe the logic of doReplaceWriter. The main logic of rollWriter is same with
 * FSHLog.<br>
 * For a normal roll request(for example, we have reached the log roll size):
 * <ol>
 * <li>In the log roller thread, we will set {@link #waitingRoll} to true and
 * {@link #readyForRolling} to false, and then wait on {@link #readyForRolling}(see
 * {@link #waitForSafePoint()}).</li>
 * <li>In the consumer thread, we will stop polling entries from {@link #waitingConsumePayloads} if
 * {@link #waitingRoll} is true, and also stop writing the entries in {@link #toWriteAppends} out.
 * </li>
 * <li>If there are unflush data in the writer, sync them.</li>
 * <li>When all out-going sync request is finished, i.e, the {@link #unackedAppends} is empty,
 * signal the {@link #readyForRollingCond}.</li>
 * <li>Back to the log roller thread, now we can confirm that there are no out-going entries, i.e.,
 * we reach a safe point. So it is safe to replace old writer with new writer now.</li>
 * <li>Set {@link #writerBroken} and {@link #waitingRoll} to false.</li>
 * <li>Schedule the consumer task.</li>
 * <li>Schedule a background task to close the old writer.</li>
 * </ol>
 * For a broken writer roll request, the only difference is that we can bypass the wait for safe
 * point stage.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class AsyncFSWAL extends AbstractFSWAL<AsyncWriter> {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncFSWAL.class);

  public static final String ASYNC_WAL_USE_SHARED_EVENT_LOOP =
    "hbase.wal.async.use-shared-event-loop";
  public static final boolean DEFAULT_ASYNC_WAL_USE_SHARED_EVENT_LOOP = false;

  public static final String ASYNC_WAL_WAIT_ON_SHUTDOWN_IN_SECONDS =
    "hbase.wal.async.wait.on.shutdown.seconds";
  public static final int DEFAULT_ASYNC_WAL_WAIT_ON_SHUTDOWN_IN_SECONDS = 5;

  private final EventLoopGroup eventLoopGroup;

  private final Class<? extends Channel> channelClass;

  private volatile AsyncFSOutput fsOut;

  private final StreamSlowMonitor streamSlowMonitor;

  public AsyncFSWAL(FileSystem fs, Abortable abortable, Path rootDir, String logDir,
    String archiveDir, Configuration conf, List<WALActionsListener> listeners,
    boolean failIfWALExists, String prefix, String suffix, FileSystem remoteFs, Path remoteWALDir,
    EventLoopGroup eventLoopGroup, Class<? extends Channel> channelClass, StreamSlowMonitor monitor)
    throws FailedLogCloseException, IOException {
    super(fs, abortable, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix,
      suffix, remoteFs, remoteWALDir);
    this.eventLoopGroup = eventLoopGroup;
    this.channelClass = channelClass;
    this.streamSlowMonitor = monitor;
    if (conf.getBoolean(ASYNC_WAL_USE_SHARED_EVENT_LOOP, DEFAULT_ASYNC_WAL_USE_SHARED_EVENT_LOOP)) {
      this.consumeExecutor = eventLoopGroup.next();
      this.shouldShutDownConsumeExecutorWhenClose = false;
      if (consumeExecutor instanceof SingleThreadEventExecutor) {
        try {
          Field field = SingleThreadEventExecutor.class.getDeclaredField("taskQueue");
          field.setAccessible(true);
          Queue<?> queue = (Queue<?>) field.get(consumeExecutor);
          this.hasConsumerTask = () -> queue.peek() == consumer;
        } catch (Exception e) {
          LOG.warn("Can not get task queue of " + consumeExecutor
            + ", this is not necessary, just give up", e);
          this.hasConsumerTask = () -> false;
        }
      } else {
        this.hasConsumerTask = () -> false;
      }
    } else {
      this.createSingleThreadPoolConsumeExecutor("AsyncFSWAL", rootDir, prefix);
    }

    this.setWaitOnShutdownInSeconds(conf.getInt(ASYNC_WAL_WAIT_ON_SHUTDOWN_IN_SECONDS,
      DEFAULT_ASYNC_WAL_WAIT_ON_SHUTDOWN_IN_SECONDS), ASYNC_WAL_WAIT_ON_SHUTDOWN_IN_SECONDS);
  }

  @Override
  protected CompletableFuture<Long> doWriterSync(AsyncWriter writer, boolean shouldUseHsync,
    long txidWhenSyn) {
    return writer.sync(shouldUseHsync);
  }

  protected final AsyncWriter createAsyncWriter(FileSystem fs, Path path) throws IOException {
    return AsyncFSWALProvider.createAsyncWriter(conf, fs, path, false, this.blocksize,
      eventLoopGroup, channelClass, streamSlowMonitor);
  }

  @Override
  protected AsyncWriter createWriterInstance(FileSystem fs, Path path) throws IOException {
    return createAsyncWriter(fs, path);
  }

  @Override
  protected void onWriterReplaced(AsyncWriter nextWriter) {
    if (nextWriter instanceof AsyncProtobufLogWriter) {
      this.fsOut = ((AsyncProtobufLogWriter) nextWriter).getOutput();
    }
  }

  @Override
  protected void doAppend(AsyncWriter writer, FSWALEntry entry) {
    writer.append(entry);
  }

  @Override
  DatanodeInfo[] getPipeline() {
    AsyncFSOutput output = this.fsOut;
    return output != null ? output.getPipeline() : new DatanodeInfo[0];
  }

  @Override
  int getLogReplication() {
    return getPipeline().length;
  }

  @Override
  protected boolean doCheckLogLowReplication() {
    // not like FSHLog, AsyncFSOutput will fail immediately if there are errors writing to DNs, so
    // typically there is no 'low replication' state, only a 'broken' state.
    AsyncFSOutput output = this.fsOut;
    return output != null && output.isBroken();
  }

  @Override
  protected AsyncWriter createCombinedWriter(AsyncWriter localWriter, AsyncWriter remoteWriter) {
    // put remote writer first as usually it will cost more time to finish, so we write to it first
    return CombinedAsyncWriter.create(remoteWriter, localWriter);
  }
}
