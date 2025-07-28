package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.procedure2.BaseRSProcedureCallable;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@InterfaceAudience.Private
public class RefreshHFilesCallable extends BaseRSProcedureCallable  {
  private static final Logger LOG = LoggerFactory.getLogger(RefreshHFilesCallable.class);

  private RegionInfo regionInfo;

  @Override
  protected void doCall() throws Exception {
    HRegion region = rs.getRegion(regionInfo.getEncodedName());
    LOG.debug("Starting refrehHfiles operation on region {}", region);

    try {
      for (Store store : region.getStores()) {
        store.refreshStoreFiles();
      }
    } catch(IOException ioe) {
      LOG.warn("Exception while trying to refresh store files: ", ioe);
    }
  }


  @Override
  protected void initParameter(byte[] parameter) throws Exception {
    MasterProcedureProtos.RefreshHFilesRegionParameter param = MasterProcedureProtos.RefreshHFilesRegionParameter.parseFrom(parameter);
    this.regionInfo = ProtobufUtil.toRegionInfo(param.getRegion());
  }

  @Override
  public EventType getEventType() {
    return EventType.RS_REFRESH_HFILES;
  }
}
