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
package org.apache.hadoop.hbase.coprocessor.example.row.stats.recorder;

import static org.apache.hadoop.hbase.coprocessor.example.row.stats.utils.RowStatisticsConfigurationUtil.getInt;
import static org.apache.hadoop.hbase.coprocessor.example.row.stats.utils.RowStatisticsConfigurationUtil.getLong;
import static org.apache.hadoop.hbase.coprocessor.example.row.stats.utils.RowStatisticsTableUtil.NAMESPACED_TABLE_NAME;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionConfiguration;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.RowStatisticsImpl;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.ringbuffer.RowStatisticsDisruptorExceptionHandler;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.ringbuffer.RowStatisticsEventHandler;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.ringbuffer.RowStatisticsRingBufferEnvelope;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.ringbuffer.RowStatisticsRingBufferPayload;
import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

@InterfaceAudience.Private
public final class RowStatisticsTableRecorder implements RowStatisticsRecorder {

  private static final Logger LOG = LoggerFactory.getLogger(RowStatisticsTableRecorder.class);
  // Must be multiple of 2. Should be greater than num regions/RS
  private static final int DEFAULT_EVENT_COUNT = 1024;
  private static final long DISRUPTOR_SHUTDOWN_TIMEOUT_MS = 60_0000L;
  private final BufferedMutator bufferedMutator;
  private final Counter rowStatisticsDropped;
  private final Disruptor<RowStatisticsRingBufferEnvelope> disruptor;
  private final RingBuffer<RowStatisticsRingBufferEnvelope> ringBuffer;
  private final AtomicBoolean closed;

  /*
   * This constructor is ONLY for testing. Use TableRecorder#forClusterConnection if you want to
   * instantiate a TableRecorder object.
   */
  private RowStatisticsTableRecorder(BufferedMutator bufferedMutator,
    Disruptor<RowStatisticsRingBufferEnvelope> disruptor, Counter rowStatisticsDropped) {
    this.bufferedMutator = bufferedMutator;
    this.disruptor = disruptor;
    this.ringBuffer = disruptor.getRingBuffer();
    this.rowStatisticsDropped = rowStatisticsDropped;
    this.closed = new AtomicBoolean(false);
  }

  public static RowStatisticsTableRecorder forClusterConnection(Connection clusterConnection,
    Counter rowStatisticsDropped, Counter rowStatisticsPutFailed) {
    BufferedMutator bufferedMutator =
      initializeBufferedMutator(clusterConnection, rowStatisticsPutFailed);
    if (bufferedMutator == null) {
      return null;
    }

    Disruptor<RowStatisticsRingBufferEnvelope> disruptor =
      initializeDisruptor(bufferedMutator, rowStatisticsPutFailed);
    disruptor.start();

    return new RowStatisticsTableRecorder(bufferedMutator, disruptor, rowStatisticsDropped);
  }

  @Override
  public void record(RowStatisticsImpl rowStatistics, Optional<byte[]> fullRegionName) {
    if (!closed.get()) {
      if (
        !ringBuffer.tryPublishEvent((envelope, seqId) -> envelope
          .load(new RowStatisticsRingBufferPayload(rowStatistics, fullRegionName.get())))
      ) {
        rowStatisticsDropped.increment();
        LOG.error("Failed to load row statistics for region={} into the ring buffer",
          rowStatistics.getRegion());
      }
    } else {
      rowStatisticsDropped.increment();
      LOG.error("TableRecorder is closed. Will not record row statistics for region={}",
        rowStatistics.getRegion());
    }
  }

  public void close() throws IOException {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    try {
      disruptor.shutdown(DISRUPTOR_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      LOG.warn(
        "Disruptor shutdown timed out after {} ms. Forcing halt. Some row statistics may be lost",
        DISRUPTOR_SHUTDOWN_TIMEOUT_MS);
      disruptor.halt();
      disruptor.shutdown();
    }
    bufferedMutator.close();
  }

  private static BufferedMutator initializeBufferedMutator(Connection conn,
    Counter rowStatisticsPutFailed) {
    Configuration conf = conn.getConfiguration();
    TableRecorderExceptionListener exceptionListener =
      new TableRecorderExceptionListener(rowStatisticsPutFailed);
    BufferedMutatorParams params = new BufferedMutatorParams(NAMESPACED_TABLE_NAME)
      .rpcTimeout(getInt(conf, HConstants.HBASE_RPC_TIMEOUT_KEY, 15_000))
      .operationTimeout(getInt(conf, HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30_000))
      .setWriteBufferPeriodicFlushTimeoutMs(
        getLong(conf, ConnectionConfiguration.WRITE_BUFFER_PERIODIC_FLUSH_TIMEOUT_MS, 60_000L))
      .writeBufferSize(getLong(conf, ConnectionConfiguration.WRITE_BUFFER_SIZE_KEY,
        ConnectionConfiguration.WRITE_BUFFER_SIZE_DEFAULT))
      .listener(exceptionListener);
    BufferedMutator bufferedMutator = null;
    try {
      bufferedMutator = conn.getBufferedMutator(params);
    } catch (IOException e) {
      LOG.error("This should NEVER print!", e);
    }
    return bufferedMutator;
  }

  private static Disruptor<RowStatisticsRingBufferEnvelope>
    initializeDisruptor(BufferedMutator bufferedMutator, Counter rowStatisticsPutFailures) {
    Disruptor<RowStatisticsRingBufferEnvelope> disruptor =
      new Disruptor<>(RowStatisticsRingBufferEnvelope::new, DEFAULT_EVENT_COUNT,
        new ThreadFactoryBuilder().setNameFormat("rowstats.append-pool-%d").setDaemon(true)
          .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build(),
        ProducerType.MULTI, new BlockingWaitStrategy());
    disruptor.setDefaultExceptionHandler(new RowStatisticsDisruptorExceptionHandler());
    RowStatisticsEventHandler rowStatisticsEventHandler =
      new RowStatisticsEventHandler(bufferedMutator, rowStatisticsPutFailures);
    disruptor.handleEventsWith(new RowStatisticsEventHandler[] { rowStatisticsEventHandler });
    return disruptor;
  }

  protected static class TableRecorderExceptionListener
    implements BufferedMutator.ExceptionListener {

    private final Counter rowStatisticsPutFailures;

    TableRecorderExceptionListener(Counter counter) {
      this.rowStatisticsPutFailures = counter;
    }

    public void onException(RetriesExhaustedWithDetailsException exception,
      BufferedMutator mutator) {
      long failedPuts = mutator.getWriteBufferSize();
      rowStatisticsPutFailures.increment(failedPuts);
      LOG.error(
        "Periodic flush of buffered mutator failed. Cannot persist {} row stats stored in buffer",
        failedPuts, exception);
    }
  }
}
