package org.apache.hadoop.hbase.keymeta;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyStatus;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos.ManagedKeysRequest;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos.ManagedKeysResponse;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.security.KeyException;
import java.util.ArrayList;
import java.util.List;

@InterfaceAudience.Public
public class KeymetaAdminClient implements KeymetaAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(KeymetaAdminClient.class);
  private ManagedKeysProtos.ManagedKeysService.BlockingInterface stub;

  public KeymetaAdminClient(Connection conn) throws IOException {
    this.stub = ManagedKeysProtos.ManagedKeysService.newBlockingStub(conn.getAdmin().coprocessorService());
  }

  @Override
  public ManagedKeyStatus enableKeyManagement(String keyCust, String keyNamespace)
      throws IOException {
    try {
      ManagedKeysResponse response = stub.enableKeyManagement(null,
        ManagedKeysRequest.newBuilder().setKeyCust(keyCust).setKeyNamespace(keyNamespace).build());
      LOG.info("Got response: " + response);
      return ManagedKeyStatus.forValue((byte) response.getKeyStatus().getNumber());
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public List<ManagedKeyData> getManagedKeys(String keyCust, String keyNamespace)
    throws IOException, KeyException {
    List<ManagedKeyData> keyStatuses = new ArrayList<>();
    try {
      ManagedKeysProtos.GetManagedKeysResponse statusResponse = stub.getManagedKeys(null,
        ManagedKeysRequest.newBuilder().setKeyCust(keyCust).setKeyNamespace(keyNamespace).build());
      for (ManagedKeysResponse status: statusResponse.getStatusList()) {
        keyStatuses.add(new ManagedKeyData(
          status.getKeyCustBytes().toByteArray(),
          status.getKeyNamespace(), null,
          ManagedKeyStatus.forValue((byte) status.getKeyStatus().getNumber()),
          status.getKeyMetadata(),
          status.getRefreshTimestamp(), status.getReadOpCount(), status.getWriteOpCount()));
      }
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
    return keyStatuses;
  }
}
