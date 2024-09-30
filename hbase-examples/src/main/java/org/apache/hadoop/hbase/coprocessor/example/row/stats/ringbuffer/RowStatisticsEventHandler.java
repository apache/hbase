package org.apache.hadoop.hbase.coprocessor.example.row.stats.ringbuffer;

import static org.apache.hadoop.hbase.coprocessor.example.row.stats.utils.TableUtil.buildPutForRegion;
import com.lmax.disruptor.EventHandler;
import java.io.IOException;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.RowStatistics;
import org.apache.hadoop.hbase.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class RowStatisticsEventHandler implements EventHandler<RingBufferEnvelope> {

  private static final Logger LOG = LoggerFactory.getLogger(
    RowStatisticsEventHandler.class
  );
  private final BufferedMutator bufferedMutator;
  private final Counter rowStatisticsPutFailures;

  public RowStatisticsEventHandler(
    BufferedMutator bufferedMutator,
    Counter rowStatisticsPutFailures
  ) {
    this.bufferedMutator = bufferedMutator;
    this.rowStatisticsPutFailures = rowStatisticsPutFailures;
  }

  @Override
  public void onEvent(RingBufferEnvelope event, long sequence, boolean endOfBatch)
    throws Exception {
    final RingBufferPayload payload = event.getPayload();
    if (payload != null) {
      final RowStatistics rowStatistics = payload.getRowStatistics();
      final boolean isMajor = payload.getIsMajor();
      final byte[] fullRegionName = payload.getFullRegionName();
      Put put = buildPutForRegion(fullRegionName, rowStatistics, isMajor);
      try {
        bufferedMutator.mutate(put);
      } catch (IOException e) {
        rowStatisticsPutFailures.increment();
        LOG.error(
          "Mutate operation failed. Cannot persist row statistics for region {}",
          rowStatistics.getRegion(),
          e
        );
      }
    }
  }
}
