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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider;
import org.apache.hadoop.hbase.regionserver.keymeta.MetricsKeyManagementSource;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentMatchers;

/**
 * Unit test for metrics emitted by {@link ManagedKeyDataCache}. Uses a cache with no L2 accessor
 * (keymetaAccessor = null) which enables dynamic lookup directly to the provider, allowing
 * verification of all cache-level metric instrumentation without a MiniCluster.
 */
@Category({ MiscTests.class, SmallTests.class })
public class TestManagedKeyDataCacheMetrics {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestManagedKeyDataCacheMetrics.class);

  private ManagedKeyDataCache cache;
  private MetricsKeyManagementSource source;
  private MetricsKeyManagement metrics;
  private Configuration conf;

  @Before
  public void setUp() {
    Encryption.clearKeyProviderCache();
    conf = HBaseConfiguration.create();
    conf.set(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, "true");
    conf.set(HConstants.CRYPTO_MANAGED_KEYPROVIDER_CONF_KEY,
      MockManagedKeyProvider.class.getName());

    // keymetaAccessor = null enables dynamic lookup (provider called directly)
    cache = new ManagedKeyDataCache(conf, null);

    source = mock(MetricsKeyManagementSource.class);
    metrics = new MetricsKeyManagement(source);
    cache.setMetrics(metrics);
  }

  @Test
  public void testProviderCallOnWritePath() throws Exception {
    MockManagedKeyProvider provider =
      (MockManagedKeyProvider) Encryption.getManagedKeyProvider(conf);
    provider.setMultikeyGenMode(true);

    Bytes custBytes = new Bytes("provCust1".getBytes());
    cache.getActiveEntry(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL_BYTES);

    verify(source, times(1)).incrementWriteKeyLookupRequests();
    verify(source, times(1)).incrementWriteKeyLookupCacheMiss();
    verify(source, times(1)).incrementProviderCallCount();
    verify(source, times(1)).updateProviderCallTime(ArgumentMatchers.anyLong());
    verify(source, times(0)).incrementProviderCallFailures();
    verify(source, times(1)).incrementWriteKeyLookupResultUsable();
  }

  @Test
  public void testProviderCallOnReadPath() throws Exception {
    MockManagedKeyProvider provider =
      (MockManagedKeyProvider) Encryption.getManagedKeyProvider(conf);
    provider.setMultikeyGenMode(true);

    ManagedKeyIdentity keyIdentity = new KeyIdentityPrefixBytesBacked(
      new Bytes("provCust2".getBytes()), ManagedKeyData.KEY_SPACE_GLOBAL_BYTES);

    // Get a valid key metadata from the provider
    ManagedKeyData keyData = provider.getManagedKey(keyIdentity);
    String keyMetadata = keyData.getKeyMetadata();

    cache.getEntry(keyIdentity, keyMetadata, null);

    verify(source, times(1)).incrementReadKeyLookupRequests();
    verify(source, times(1)).incrementReadKeyLookupCacheMiss();
    verify(source, times(1)).incrementProviderCallCount();
    verify(source, times(1)).updateProviderCallTime(ArgumentMatchers.anyLong());
    verify(source, times(0)).incrementProviderCallFailures();
    verify(source, times(1)).incrementReadKeyLookupResultUsable();
  }

  @Test
  public void testProviderCallWithFailedResult() throws Exception {
    MockManagedKeyProvider provider =
      (MockManagedKeyProvider) Encryption.getManagedKeyProvider(conf);
    // Provider IOException is caught inside retrieveActiveKey which reports it as a provider
    // failure, then returns FAILED state (result is not usable).
    provider.setShouldThrowExceptionOnGetManagedKey(true);

    Bytes custBytes = new Bytes("provCustFail1".getBytes());
    cache.getActiveEntry(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL_BYTES);

    verify(source, times(1)).incrementWriteKeyLookupRequests();
    verify(source, times(1)).incrementWriteKeyLookupCacheMiss();
    verify(source, times(1)).incrementProviderCallCount();
    verify(source, times(1)).updateProviderCallTime(ArgumentMatchers.anyLong());
    verify(source, times(1)).incrementProviderCallFailures();
    verify(source, times(0)).incrementWriteKeyLookupResultUsable();
  }

  @Test
  public void testReadPathCacheHitWithFailedEntry() throws Exception {
    MockManagedKeyProvider provider =
      (MockManagedKeyProvider) Encryption.getManagedKeyProvider(conf);
    provider.setMultikeyGenMode(true);

    ManagedKeyIdentity keyIdentity = new KeyIdentityPrefixBytesBacked(
      new Bytes("provCustNeg".getBytes()), ManagedKeyData.KEY_SPACE_GLOBAL_BYTES);

    // Use invalid metadata to get a FAILED entry cached (provider returns FAILED for unknown keys)
    cache.getEntry(keyIdentity, "unknown:provCustNeg:global", null);

    // Reset mock to verify second call behavior
    org.mockito.Mockito.clearInvocations(source);

    // Second call hits the negative cache entry
    cache.getEntry(keyIdentity, "unknown:provCustNeg:global", null);

    verify(source, times(1)).incrementReadKeyLookupRequests();
    verify(source, times(1)).incrementReadKeyLookupCacheHit();
    verify(source, times(0)).incrementReadKeyLookupCacheMiss();
    verify(source, times(0)).incrementReadKeyLookupResultUsable();
    verify(source, times(0)).incrementReadKeyLookupResultDisabled();
  }

  @Test
  public void testProviderCallSucceedsButValidationFails() throws Exception {
    // A provider that returns null from getManagedKey: the provider call itself "succeeds"
    // (no IOException), but the post-call validation throws KeyException. The provider metric
    // should show success (providerCallCompleted), not failure.
    conf.set(HConstants.CRYPTO_MANAGED_KEYPROVIDER_CONF_KEY, NullReturningProvider.class.getName());
    Encryption.clearKeyProviderCache();
    cache = new ManagedKeyDataCache(conf, null);
    cache.setMetrics(metrics);

    Bytes custBytes = new Bytes("provCustNull".getBytes());
    cache.getActiveEntry(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL_BYTES);

    verify(source, times(1)).incrementWriteKeyLookupRequests();
    verify(source, times(1)).incrementWriteKeyLookupCacheMiss();
    verify(source, times(1)).incrementProviderCallCount();
    verify(source, times(1)).updateProviderCallTime(ArgumentMatchers.anyLong());
    verify(source, times(0)).incrementProviderCallFailures();
    verify(source, times(0)).incrementWriteKeyLookupResultUsable();
  }

  @Test
  public void testProviderCallSucceedsWithDisabledState() throws Exception {
    MockManagedKeyProvider provider =
      (MockManagedKeyProvider) Encryption.getManagedKeyProvider(conf);
    provider.setMultikeyGenMode(true);

    // When unwrapKey returns DISABLED state, the provider call itself succeeds
    // (providerCallCompleted). The subsequent validation in retrieveKey throws KeyException,
    // but that's not a provider failure.
    ManagedKeyIdentity keyIdentity = new KeyIdentityPrefixBytesBacked(
      new Bytes("provCustDisR".getBytes()), ManagedKeyData.KEY_SPACE_GLOBAL_BYTES);
    ManagedKeyData keyData = provider.getManagedKey(keyIdentity);
    String keyMetadata = keyData.getKeyMetadata();

    provider.setMockedKeyState("provCustDisR", ManagedKeyState.DISABLED);

    cache.getEntry(keyIdentity, keyMetadata, null);

    verify(source, times(1)).incrementReadKeyLookupRequests();
    verify(source, times(1)).incrementReadKeyLookupCacheMiss();
    verify(source, times(1)).incrementProviderCallCount();
    verify(source, times(1)).updateProviderCallTime(ArgumentMatchers.anyLong());
    verify(source, times(0)).incrementProviderCallFailures();
    verify(source, times(0)).incrementReadKeyLookupResultUsable();
  }

  @Test
  public void testWritePathCacheHitWithDisabledEntry() throws Exception {
    MockManagedKeyProvider provider =
      (MockManagedKeyProvider) Encryption.getManagedKeyProvider(conf);
    provider.setMultikeyGenMode(true);

    // First fetch loads a DISABLED state into active cache
    provider.setMockedKeyState("provCustDisW", ManagedKeyState.ACTIVE_DISABLED);
    Bytes custBytes = new Bytes("provCustDisW".getBytes());
    cache.getActiveEntry(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL_BYTES);

    org.mockito.Mockito.clearInvocations(source);

    // Second fetch hits the cached DISABLED entry (getIfPresent returns non-null, not ACTIVE)
    cache.getActiveEntry(custBytes, ManagedKeyData.KEY_SPACE_GLOBAL_BYTES);

    verify(source, times(1)).incrementWriteKeyLookupRequests();
    verify(source, times(1)).incrementWriteKeyLookupCacheHit();
    verify(source, times(0)).incrementWriteKeyLookupCacheMiss();
    verify(source, times(0)).incrementWriteKeyLookupResultUsable();
    verify(source, times(1)).incrementWriteKeyLookupResultDisabled();
  }

  @Test
  public void testReadPathCacheHitWithDisabledEntry() throws Exception {
    MockManagedKeyProvider provider =
      (MockManagedKeyProvider) Encryption.getManagedKeyProvider(conf);
    provider.setMultikeyGenMode(true);

    ManagedKeyIdentity keyIdentity = new KeyIdentityPrefixBytesBacked(
      new Bytes("provCustDisRH".getBytes()), ManagedKeyData.KEY_SPACE_GLOBAL_BYTES);
    ManagedKeyData keyData = provider.getManagedKey(keyIdentity);
    String keyMetadata = keyData.getKeyMetadata();

    // Set state to ACTIVE_DISABLED so unwrapKey returns it (not generic DISABLED which throws)
    provider.setMockedKeyState("provCustDisRH", ManagedKeyState.ACTIVE_DISABLED);

    // First call loads the ACTIVE_DISABLED entry into cache
    cache.getEntry(keyIdentity, keyMetadata, null);

    org.mockito.Mockito.clearInvocations(source);

    // Second call should be a cache hit on the DISABLED entry
    cache.getEntry(keyIdentity, keyMetadata, null);

    verify(source, times(1)).incrementReadKeyLookupRequests();
    verify(source, times(1)).incrementReadKeyLookupCacheHit();
    verify(source, times(0)).incrementReadKeyLookupCacheMiss();
    verify(source, times(0)).incrementReadKeyLookupResultUsable();
    verify(source, times(1)).incrementReadKeyLookupResultDisabled();
  }

  /** Provider that returns null from getManagedKey to trigger KeyException in retrieveActiveKey. */
  public static class NullReturningProvider extends MockManagedKeyProvider {
    @Override
    public ManagedKeyData getManagedKey(ManagedKeyIdentity keyIdentity) throws IOException {
      return null;
    }
  }
}
