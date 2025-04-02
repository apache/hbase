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
package org.apache.hadoop.hbase.coprocessor.example.row.stats.ringbuffer;

import static org.apache.hadoop.hbase.coprocessor.example.row.stats.utils.RowStatisticsTableUtil.buildPutForRegion;

import com.lmax.disruptor.EventHandler;
import java.io.IOException;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.RowStatistics;
import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class RowStatisticsEventHandler implements EventHandler<RowStatisticsRingBufferEnvelope> {

  private static final Logger LOG = LoggerFactory.getLogger(RowStatisticsEventHandler.class);
  private final BufferedMutator bufferedMutator;
  private final Counter rowStatisticsPutFailures;

  public RowStatisticsEventHandler(BufferedMutator bufferedMutator,
    Counter rowStatisticsPutFailures) {
    this.bufferedMutator = bufferedMutator;
    this.rowStatisticsPutFailures = rowStatisticsPutFailures;
  }

  @Override
  public void onEvent(RowStatisticsRingBufferEnvelope event, long sequence, boolean endOfBatch)
    throws Exception {
    final RowStatisticsRingBufferPayload payload = event.getPayload();
    if (payload != null) {
      final RowStatistics rowStatistics = payload.getRowStatistics();
      final byte[] fullRegionName = payload.getFullRegionName();
      Put put = buildPutForRegion(fullRegionName, rowStatistics, rowStatistics.isMajor());
      try {
        bufferedMutator.mutate(put);
      } catch (IOException e) {
        rowStatisticsPutFailures.increment();
        LOG.error("Mutate operation failed. Cannot persist row statistics for region {}",
          rowStatistics.getRegion(), e);
      }
    }
  }
}
