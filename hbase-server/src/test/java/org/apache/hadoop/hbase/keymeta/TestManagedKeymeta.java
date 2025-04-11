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

  private void doTestEnable(KeymetaAdmin adminClient) throws IOException, KeyException {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    MockManagedKeyProvider managedKeyProvider = (MockManagedKeyProvider)
      Encryption.getKeyProvider(master.getConfiguration());
    ;
    String cust = "cust1";
    String encodedCust = ManagedKeyProvider.encodeToStr(cust.getBytes());
    ManagedKeyStatus managedKeyStatus =
      adminClient.enableKeyManagement(encodedCust, ManagedKeyData.KEY_SPACE_GLOBAL);
    assertNotNull(managedKeyStatus);
    assertEquals(ManagedKeyStatus.ACTIVE, managedKeyStatus);

    List<ManagedKeyData> managedKeys =
      adminClient.getManagedKeys(encodedCust, ManagedKeyData.KEY_SPACE_GLOBAL);
    assertEquals(1, managedKeys.size());
    assertEquals(managedKeyProvider.getKey(cust.getBytes()).cloneWithoutKey(),
      managedKeys.get(0).cloneWithoutKey());

    String nonExistentCust = "nonExistentCust";
    managedKeyProvider.setKeyStatus(nonExistentCust, ManagedKeyStatus.FAILED);
    assertEquals(ManagedKeyStatus.FAILED, adminClient.enableKeyManagement(
      ManagedKeyProvider.encodeToStr(nonExistentCust.getBytes()), ManagedKeyData.KEY_SPACE_GLOBAL));

    String disabledCust = "disabledCust";
    managedKeyProvider.setKeyStatus(disabledCust, ManagedKeyStatus.DISABLED);
    assertEquals(ManagedKeyStatus.DISABLED, adminClient.enableKeyManagement(
      ManagedKeyProvider.encodeToStr(disabledCust.getBytes()), ManagedKeyData.KEY_SPACE_GLOBAL));
  }
}
