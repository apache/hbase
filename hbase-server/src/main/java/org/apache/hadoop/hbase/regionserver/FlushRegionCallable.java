package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.procedure2.BaseRSProcedureCallable;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.FlushRegionParameter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Collections;

@InterfaceAudience.Private
public class FlushRegionCallable extends BaseRSProcedureCallable {

  private static final Logger LOG = LoggerFactory.getLogger(FlushRegionCallable.class);

  private RegionInfo regionInfo;

  private byte[] columnFamily;

  @Override
  protected void doCall() throws Exception {
    HRegion region = rs.getRegion(regionInfo.getEncodedName());
    if (region == null) {
      throw new NotServingRegionException("region=" + regionInfo.getRegionNameAsString());
    }
    LOG.debug("Starting region operation on {}", region);
    region.startRegionOperation();
    try {
        long readPt = region.getReadPoint(IsolationLevel.READ_COMMITTED);
        HRegion.FlushResult res;
        if (columnFamily == null) {
          res = region.flush(true);
        } else {
          res = region.flushcache(Collections.singletonList(columnFamily),
            false, FlushLifeCycleTracker.DUMMY);
        }
        if (res.getResult() == HRegion.FlushResult.Result.CANNOT_FLUSH) {
          region.waitForFlushes();
          if (region.getMaxFlushedSeqId() < readPt) {
            throw new IOException("Unable to complete flush " + regionInfo);
          }
        }
    } finally {
      LOG.debug("Closing region operation on {}", region);
      region.closeRegionOperation();
    }
  }

  @Override
  protected void initParameter(byte[] parameter) throws Exception {
    FlushRegionParameter param = FlushRegionParameter.parseFrom(parameter);
    this.regionInfo = ProtobufUtil.toRegionInfo(param.getRegion());
    if (param.hasColumnFamily()) {
      this.columnFamily = param.getColumnFamily().toByteArray();
    }
  }

  @Override
  public EventType getEventType() {
    return EventType.RS_FLUSH_REGIONS;
  }
}
