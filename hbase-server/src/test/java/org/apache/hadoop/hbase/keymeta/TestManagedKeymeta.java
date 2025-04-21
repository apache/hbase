package org.apache.hadoop.hbase.keymeta;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyStatus;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.io.IOException;
import java.security.KeyException;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

@Category({ MasterTests.class, MediumTests.class })
public class TestManagedKeymeta extends ManagedKeyTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestManagedKeymeta.class);

  @Test
  public void testEnableLocal() throws Exception {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    KeymetaAdmin keymetaAdmin = master.getKeymetaAdmin();
    doTestEnable(keymetaAdmin);
  }

  @Test
  public void testEnableOverRPC() throws Exception {
    KeymetaAdmin adminClient = new KeymetaAdminClient(TEST_UTIL.getConnection());
    doTestEnable(adminClient);
  }

  // TODO: Need to add test cases for multiple key spaces.
  private void doTestEnable(KeymetaAdmin adminClient) throws IOException, KeyException {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    MockManagedKeyProvider managedKeyProvider = (MockManagedKeyProvider)
      Encryption.getKeyProvider(master.getConfiguration());
    ;
    String cust = "cust1";
    String encodedCust = ManagedKeyProvider.encodeToStr(cust.getBytes());
    List<ManagedKeyData> managedKeyStatuses =
      adminClient.enableKeyManagement(encodedCust, ManagedKeyData.KEY_SPACE_GLOBAL);
    assertKeyDataListSingleKey(managedKeyStatuses, ManagedKeyStatus.ACTIVE);

    List<ManagedKeyData> managedKeys =
      adminClient.getManagedKeys(encodedCust, ManagedKeyData.KEY_SPACE_GLOBAL);
    assertEquals(1, managedKeys.size());
    assertEquals(managedKeyProvider.getLastGeneratedKeyData(cust,
        ManagedKeyData.KEY_SPACE_GLOBAL).cloneWithoutKey(), managedKeys.get(0).cloneWithoutKey());

    String nonExistentCust = "nonExistentCust";
    managedKeyProvider.setMockedKeyStatus(nonExistentCust, ManagedKeyStatus.FAILED);
    List<ManagedKeyData> keyDataList1 =
      adminClient.enableKeyManagement(ManagedKeyProvider.encodeToStr(nonExistentCust.getBytes()),
        ManagedKeyData.KEY_SPACE_GLOBAL);
    assertKeyDataListSingleKey(keyDataList1, ManagedKeyStatus.FAILED);

    String disabledCust = "disabledCust";
    managedKeyProvider.setMockedKeyStatus(disabledCust, ManagedKeyStatus.DISABLED);
    List<ManagedKeyData> keyDataList2 =
      adminClient.enableKeyManagement(ManagedKeyProvider.encodeToStr(disabledCust.getBytes()),
        ManagedKeyData.KEY_SPACE_GLOBAL);
    assertKeyDataListSingleKey(keyDataList2, ManagedKeyStatus.DISABLED);
  }

  private static void assertKeyDataListSingleKey(List<ManagedKeyData> managedKeyStatuses,
      ManagedKeyStatus keyStatus) {
    assertNotNull(managedKeyStatuses);
    assertEquals(1, managedKeyStatuses.size());
    assertEquals(keyStatus, managedKeyStatuses.get(0).getKeyStatus());
  }
}
