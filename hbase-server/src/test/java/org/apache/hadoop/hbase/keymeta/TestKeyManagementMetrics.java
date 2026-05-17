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
package org.apache.hadoop.hbase.keymeta;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.keymeta.MetricsKeyManagementSource;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration test that verifies key management metrics are emitted correctly when operations are
 * performed against a running MiniCluster with key management enabled.
 */
@Category({ MiscTests.class, MediumTests.class })
public class TestKeyManagementMetrics extends ManagedKeyTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestKeyManagementMetrics.class);

  private static final MetricsAssertHelper METRICS_HELPER =
    CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  private static final String[] WRITE_COUNTERS =
    { MetricsKeyManagementSource.WRITE_KEY_LOOKUP_REQUESTS,
      MetricsKeyManagementSource.WRITE_KEY_LOOKUP_CACHE_HIT,
      MetricsKeyManagementSource.WRITE_KEY_LOOKUP_CACHE_MISS,
      MetricsKeyManagementSource.WRITE_KEY_LOOKUP_RESULT_USABLE,
      MetricsKeyManagementSource.WRITE_KEY_LOOKUP_RESULT_DISABLED };

  private static final String[] READ_COUNTERS =
    { MetricsKeyManagementSource.READ_KEY_LOOKUP_REQUESTS,
      MetricsKeyManagementSource.READ_KEY_LOOKUP_CACHE_HIT,
      MetricsKeyManagementSource.READ_KEY_LOOKUP_CACHE_MISS,
      MetricsKeyManagementSource.READ_KEY_LOOKUP_RESULT_USABLE,
      MetricsKeyManagementSource.READ_KEY_LOOKUP_RESULT_DISABLED };

  private static final String[] PROVIDER_COUNTERS =
    { MetricsKeyManagementSource.PROVIDER_CALL_COUNT,
      MetricsKeyManagementSource.PROVIDER_CALL_FAILURES };

  @Override
  protected void configureKeyProvider() {
    TEST_UTIL.getConfiguration().setBoolean(HConstants.CRYPTO_MANAGED_KEYS_METRICS_ENABLED_CONF_KEY,
      true);
  }

  private MetricsKeyManagementSource getSource() {
    return CompatibilitySingletonFactory.getInstance(MetricsKeyManagementSource.class);
  }

  /** Captures current values of the specified counters in a single snapshot. */
  private Map<String, Long> snapshot(String[]... counterGroups) {
    MetricsKeyManagementSource source = getSource();
    Map<String, Long> values = new HashMap<>();
    for (String[] group : counterGroups) {
      for (String name : group) {
        values.put(name, METRICS_HELPER.getCounter(name, source));
      }
    }
    return values;
  }

  /** Asserts that the specified counters increased relative to the before snapshot. */
  private void assertIncreased(Map<String, Long> before, String... names) {
    MetricsKeyManagementSource source = getSource();
    for (String name : names) {
      long current = METRICS_HELPER.getCounter(name, source);
      assertTrue(
        name + " should have increased (before=" + before.get(name) + ", after=" + current + ")",
        current > before.get(name));
    }
  }

  /** Asserts that the specified counters did NOT change relative to the before snapshot. */
  private void assertUnchanged(Map<String, Long> before, String... names) {
    MetricsKeyManagementSource source = getSource();
    for (String name : names) {
      long current = METRICS_HELPER.getCounter(name, source);
      assertTrue(
        name + " should not have changed (before=" + before.get(name) + ", after=" + current + ")",
        current == before.get(name));
    }
  }

  @Test
  public void testWritePathCacheMissAndHit() throws Exception {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    MockManagedKeyProvider provider =
      (MockManagedKeyProvider) Encryption.getManagedKeyProvider(master.getConfiguration());
    provider.setMultikeyGenMode(true);

    byte[] custBytes = "metricsCust1".getBytes();
    KeymetaAdmin adminClient = master.getKeymetaAdmin();
    adminClient.enableKeyManagement(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL);

    HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    ManagedKeyDataCache cache = regionServer.getManagedKeyDataCache();

    // --- First call: cache miss ---
    Map<String, Long> before = snapshot(WRITE_COUNTERS, PROVIDER_COUNTERS);

    cache.getActiveEntry(new Bytes(custBytes), ManagedKeyData.KEY_SPACE_GLOBAL_BYTES);

    assertIncreased(before, MetricsKeyManagementSource.WRITE_KEY_LOOKUP_REQUESTS,
      MetricsKeyManagementSource.WRITE_KEY_LOOKUP_CACHE_MISS,
      MetricsKeyManagementSource.WRITE_KEY_LOOKUP_RESULT_USABLE);
    assertUnchanged(before, MetricsKeyManagementSource.WRITE_KEY_LOOKUP_CACHE_HIT,
      MetricsKeyManagementSource.WRITE_KEY_LOOKUP_RESULT_DISABLED,
      MetricsKeyManagementSource.PROVIDER_CALL_COUNT,
      MetricsKeyManagementSource.PROVIDER_CALL_FAILURES);

    // --- Second call: cache hit ---
    Map<String, Long> before2 = snapshot(WRITE_COUNTERS);

    cache.getActiveEntry(new Bytes(custBytes), ManagedKeyData.KEY_SPACE_GLOBAL_BYTES);

    assertIncreased(before2, MetricsKeyManagementSource.WRITE_KEY_LOOKUP_REQUESTS,
      MetricsKeyManagementSource.WRITE_KEY_LOOKUP_CACHE_HIT,
      MetricsKeyManagementSource.WRITE_KEY_LOOKUP_RESULT_USABLE);
    assertUnchanged(before2, MetricsKeyManagementSource.WRITE_KEY_LOOKUP_CACHE_MISS,
      MetricsKeyManagementSource.WRITE_KEY_LOOKUP_RESULT_DISABLED);
  }

  @Test
  public void testReadPathCacheMissAndHit() throws Exception {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    MockManagedKeyProvider provider =
      (MockManagedKeyProvider) Encryption.getManagedKeyProvider(master.getConfiguration());
    provider.setMultikeyGenMode(true);

    byte[] custBytes = "metricsCust4".getBytes();
    KeymetaAdmin adminClient = master.getKeymetaAdmin();
    ManagedKeyData managedKey =
      adminClient.enableKeyManagement(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL);

    HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    ManagedKeyDataCache cache = regionServer.getManagedKeyDataCache();

    String keyMetadata = managedKey.getKeyMetadata();
    if (keyMetadata == null) {
      keyMetadata = adminClient.getManagedKeys(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL).stream()
        .filter(k -> k.getKeyMetadata() != null).map(ManagedKeyData::getKeyMetadata).findFirst()
        .orElseThrow(() -> new Exception("No key with metadata found"));
    }

    ManagedKeyIdentity keyIdentity =
      new KeyIdentityPrefixBytesBacked(new Bytes(custBytes), ManagedKeyData.KEY_SPACE_GLOBAL_BYTES);

    // --- First call: cache miss ---
    Map<String, Long> before = snapshot(READ_COUNTERS);

    cache.getEntry(keyIdentity, keyMetadata, null);

    assertIncreased(before, MetricsKeyManagementSource.READ_KEY_LOOKUP_REQUESTS,
      MetricsKeyManagementSource.READ_KEY_LOOKUP_CACHE_MISS,
      MetricsKeyManagementSource.READ_KEY_LOOKUP_RESULT_USABLE);
    assertUnchanged(before, MetricsKeyManagementSource.READ_KEY_LOOKUP_CACHE_HIT,
      MetricsKeyManagementSource.READ_KEY_LOOKUP_RESULT_DISABLED);

    // --- Second call: cache hit ---
    Map<String, Long> before2 = snapshot(READ_COUNTERS);

    cache.getEntry(keyIdentity, keyMetadata, null);

    assertIncreased(before2, MetricsKeyManagementSource.READ_KEY_LOOKUP_REQUESTS,
      MetricsKeyManagementSource.READ_KEY_LOOKUP_CACHE_HIT,
      MetricsKeyManagementSource.READ_KEY_LOOKUP_RESULT_USABLE);
    assertUnchanged(before2, MetricsKeyManagementSource.READ_KEY_LOOKUP_CACHE_MISS,
      MetricsKeyManagementSource.READ_KEY_LOOKUP_RESULT_DISABLED);
  }

  @Test
  public void testWritePathResultDisabled() throws Exception {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    MockManagedKeyProvider provider =
      (MockManagedKeyProvider) Encryption.getManagedKeyProvider(master.getConfiguration());
    provider.setMultikeyGenMode(true);

    byte[] custBytes = "metricsCustDisW".getBytes();
    KeymetaAdmin adminClient = master.getKeymetaAdmin();
    adminClient.enableKeyManagement(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL);

    // Disable key management for this custodian — puts a DISABLED marker in L2
    adminClient.disableKeyManagement(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL);

    HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    ManagedKeyDataCache cache = regionServer.getManagedKeyDataCache();

    // Clear cache so next lookup loads the DISABLED marker from L2
    cache.clearCache();

    Map<String, Long> before = snapshot(WRITE_COUNTERS);

    // Should find DISABLED state → resultDisabled
    cache.getActiveEntry(new Bytes(custBytes), ManagedKeyData.KEY_SPACE_GLOBAL_BYTES);

    assertIncreased(before, MetricsKeyManagementSource.WRITE_KEY_LOOKUP_REQUESTS,
      MetricsKeyManagementSource.WRITE_KEY_LOOKUP_CACHE_MISS);
    assertUnchanged(before, MetricsKeyManagementSource.WRITE_KEY_LOOKUP_CACHE_HIT,
      MetricsKeyManagementSource.WRITE_KEY_LOOKUP_RESULT_USABLE);
    // The disable marker should trigger resultDisabled
    assertIncreased(before, MetricsKeyManagementSource.WRITE_KEY_LOOKUP_RESULT_DISABLED);
  }

  @Test
  public void testReadPathResultDisabled() throws Exception {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    MockManagedKeyProvider provider =
      (MockManagedKeyProvider) Encryption.getManagedKeyProvider(master.getConfiguration());
    provider.setMultikeyGenMode(true);

    byte[] custBytes = "metricsCustDisR".getBytes();
    KeymetaAdmin adminClient = master.getKeymetaAdmin();
    ManagedKeyData managedKey =
      adminClient.enableKeyManagement(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL);

    // Get the key metadata
    String keyMetadata = managedKey.getKeyMetadata();
    if (keyMetadata == null) {
      keyMetadata = adminClient.getManagedKeys(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL).stream()
        .filter(k -> k.getKeyMetadata() != null).map(ManagedKeyData::getKeyMetadata).findFirst()
        .orElseThrow(() -> new Exception("No key with metadata found"));
    }

    // Disable the specific key
    adminClient.disableManagedKey(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL, keyMetadata);

    HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    ManagedKeyDataCache cache = regionServer.getManagedKeyDataCache();

    // Clear cache so next lookup loads the disabled key from L2
    cache.clearCache();

    ManagedKeyIdentity keyIdentity =
      new KeyIdentityPrefixBytesBacked(new Bytes(custBytes), ManagedKeyData.KEY_SPACE_GLOBAL_BYTES);

    Map<String, Long> before = snapshot(READ_COUNTERS);

    // Should find DISABLED key → resultDisabled
    cache.getEntry(keyIdentity, keyMetadata, null);

    assertIncreased(before, MetricsKeyManagementSource.READ_KEY_LOOKUP_REQUESTS,
      MetricsKeyManagementSource.READ_KEY_LOOKUP_CACHE_MISS,
      MetricsKeyManagementSource.READ_KEY_LOOKUP_RESULT_DISABLED);
    assertUnchanged(before, MetricsKeyManagementSource.READ_KEY_LOOKUP_CACHE_HIT,
      MetricsKeyManagementSource.READ_KEY_LOOKUP_RESULT_USABLE);
  }

  @Test
  public void testGaugeMetrics() throws Exception {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    MockManagedKeyProvider provider =
      (MockManagedKeyProvider) Encryption.getManagedKeyProvider(master.getConfiguration());
    provider.setMultikeyGenMode(true);

    byte[] custBytes = "metricsCust5".getBytes();
    KeymetaAdmin adminClient = master.getKeymetaAdmin();
    adminClient.enableKeyManagement(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL);

    HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    ManagedKeyDataCache cache = regionServer.getManagedKeyDataCache();

    // Populate both caches
    cache.getActiveEntry(new Bytes(custBytes), ManagedKeyData.KEY_SPACE_GLOBAL_BYTES);

    // Also do a read-path call to populate cacheByIdentity (needed for usableCount gauge)
    String keyMetadata = adminClient.getManagedKeys(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL)
      .stream().filter(k -> k.getKeyMetadata() != null).map(ManagedKeyData::getKeyMetadata)
      .findFirst().orElse(null);
    if (keyMetadata != null) {
      ManagedKeyIdentity keyIdentity = new KeyIdentityPrefixBytesBacked(new Bytes(custBytes),
        ManagedKeyData.KEY_SPACE_GLOBAL_BYTES);
      cache.getEntry(keyIdentity, keyMetadata, null);
    }

    // Manually trigger gauge computation (normally runs on a 5-minute timer).
    // Each assertGauge call triggers getMetrics() which clears the dirty flag, so we
    // re-compute before each assertion.
    MetricsKeyManagementSource source = getSource();

    cache.getMetrics().computeGauges();
    METRICS_HELPER.assertGaugeGt(MetricsKeyManagementSource.KEY_CACHE_ACTIVE_SIZE, 0, source);

    cache.getMetrics().computeGauges();
    METRICS_HELPER.assertGaugeGt(MetricsKeyManagementSource.KEY_CACHE_USABLE_COUNT, 0, source);
  }

  @Test
  public void testAdminProviderCallMetrics() throws Exception {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    MockManagedKeyProvider provider =
      (MockManagedKeyProvider) Encryption.getManagedKeyProvider(master.getConfiguration());
    provider.setMultikeyGenMode(true);

    Map<String, Long> before = snapshot(PROVIDER_COUNTERS);

    // enableKeyManagement calls retrieveActiveKey on the master which invokes the provider
    byte[] custBytes = "metricsCustAdmin".getBytes();
    KeymetaAdmin adminClient = master.getKeymetaAdmin();
    adminClient.enableKeyManagement(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL);

    assertIncreased(before, MetricsKeyManagementSource.PROVIDER_CALL_COUNT);
    assertUnchanged(before, MetricsKeyManagementSource.PROVIDER_CALL_FAILURES);
  }
}
