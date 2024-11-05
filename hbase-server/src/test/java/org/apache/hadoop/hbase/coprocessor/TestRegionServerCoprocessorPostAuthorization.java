package org.apache.hadoop.hbase.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@Category({ CoprocessorTests.class, MediumTests.class })
public class TestRegionServerCoprocessorPostAuthorization {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionServerCoprocessorPostAuthorization.class);

  public static class AuthorizationRegionServerObserver implements RegionServerCoprocessor, RegionServerObserver {
    final AtomicInteger ctPostAuthorization = new AtomicInteger(0);
    String userName = null;

    @Override public Optional<RegionServerObserver> getRegionServerObserver() {
      return Optional.of(this);
    }

    @Override public void postAuthorizeRegionServerConnection(
      ObserverContext<RegionServerCoprocessorEnvironment> ctx, String userName,
      X509Certificate[] clientCertificateChain) throws IOException {
      ctPostAuthorization.incrementAndGet();
    }
  }

  private static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // set configure to indicate which cp should be loaded
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
      AuthorizationRegionServerObserver.class.getName());
    TEST_UTIL.getConfiguration().setBoolean(CoprocessorHost.ABORT_ON_ERROR_KEY, false);
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testPostAuthorizationObserverCalled() {
    HRegionServer regionServer = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    AuthorizationRegionServerObserver observer =
      regionServer.getRegionServerCoprocessorHost().findCoprocessor(AuthorizationRegionServerObserver.class);
    assertNotEquals(0, observer.ctPostAuthorization.get());
  }
}
