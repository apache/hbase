package org.apache.hadoop.hbase.keymeta;

import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.MockPBEKeyProvider;
import org.junit.After;
import org.junit.Before;

public class PBETestBase {
  protected HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, MockPBEKeyProvider.class.getName());
    TEST_UTIL.getConfiguration().set(HConstants.CRYPTO_PBE_ENABLED_CONF_KEY, "true");

    // Start the minicluster
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.waitFor(60000, () -> TEST_UTIL.getMiniHBaseCluster().getMaster().isInitialized());
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
}
