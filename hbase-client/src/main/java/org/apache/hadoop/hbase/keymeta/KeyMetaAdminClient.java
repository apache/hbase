package org.apache.hadoop.hbase.keymeta;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.io.crypto.PBEKeyStatus;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.PBEAdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.PBEAdminProtos.PBEAdminRequest;
import org.apache.hadoop.hbase.protobuf.generated.PBEAdminProtos.PBEAdminResponse;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

@InterfaceAudience.Public
public class KeyMetaAdminClient implements KeyMetaAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(KeyMetaAdminClient.class);
  private PBEAdminProtos.PBEAdminService.BlockingInterface stub;

  public KeyMetaAdminClient(Connection conn) throws IOException {
    this.stub = PBEAdminProtos.PBEAdminService.newBlockingStub(conn.getAdmin().coprocessorService());
  }

  @Override public PBEKeyStatus enablePBE(String pbePrefix) throws IOException {
    try {
      PBEAdminResponse pbeAdminResponse = stub.enablePBE(null,
        PBEAdminRequest.newBuilder().setPbePrefix(pbePrefix).build());
      LOG.info("Got response: " + pbeAdminResponse);
      return PBEKeyStatus.forValue((byte) pbeAdminResponse.getPbeStatus().getNumber());
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }
}
