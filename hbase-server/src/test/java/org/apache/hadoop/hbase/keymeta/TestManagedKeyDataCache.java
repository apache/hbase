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

import static org.apache.hadoop.hbase.io.crypto.ManagedKeyData.KEY_SPACE_GLOBAL;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.DISABLED;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.FAILED;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.INACTIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.stream.Collectors;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Suite;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

@RunWith(Suite.class)
@Suite.SuiteClasses({ TestManagedKeyDataCache.TestGeneric.class,
  TestManagedKeyDataCache.TestWithoutL2Cache.class,
  TestManagedKeyDataCache.TestWithL2CacheAndNoDynamicLookup.class,
  TestManagedKeyDataCache.TestWithL2CacheAndDynamicLookup.class, })
@Category({ MasterTests.class, SmallTests.class })
public class TestManagedKeyDataCache {
  private static final String ALIAS = "cust1";
  private static final byte[] CUST_ID = ALIAS.getBytes();
  private static Class<? extends MockManagedKeyProvider> providerClass;

  @Mock
  private Server server;
  @Spy
  protected MockManagedKeyProvider testProvider;
  protected ManagedKeyDataCache cache;
  protected Configuration conf = HBaseConfiguration.create();

  public static class ForwardingInterceptor {
    static ThreadLocal<MockManagedKeyProvider> delegate = new ThreadLocal<>();

    static void setDelegate(MockManagedKeyProvider d) {
      delegate.set(d);
    }

    @RuntimeType
    public Object intercept(@Origin Method method, @AllArguments Object[] args) throws Throwable {
      // Translate the InvocationTargetException that results when the provider throws an exception.
      // This is actually not needed if the intercept is delegated directly to the spy.
      try {
        return method.invoke(delegate.get(), args); // calls the spy, triggering Mockito
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }

  @BeforeClass
  public static synchronized void setUpInterceptor() {
    if (providerClass != null) {
      return;
    }
    providerClass = new ByteBuddy().subclass(MockManagedKeyProvider.class)
      .name("org.apache.hadoop.hbase.io.crypto.MockManagedKeyProviderSpy")
      .method(ElementMatchers.any()) // Intercept all methods
      // Using a delegator instead of directly forwarding to testProvider to
      // facilitate switching the testProvider instance. Besides, it
      .intercept(MethodDelegation.to(new ForwardingInterceptor())).make()
      .load(MockManagedKeyProvider.class.getClassLoader(), ClassLoadingStrategy.Default.INJECTION)
      .getLoaded();
  }

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    ForwardingInterceptor.setDelegate(testProvider);

    Encryption.clearKeyProviderCache();

    conf.set(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, "true");
    conf.set(HConstants.CRYPTO_MANAGED_KEYPROVIDER_CONF_KEY, providerClass.getName());

    // Configure the server mock to return the configuration
    when(server.getConfiguration()).thenReturn(conf);

    testProvider.setMultikeyGenMode(true);
  }

  @Category({ MasterTests.class, SmallTests.class })
  public static class TestGeneric {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestGeneric.class);

    @Test
    public void testEmptyCache() throws Exception {
      ManagedKeyDataCache cache = new ManagedKeyDataCache(HBaseConfiguration.create(), null);
      assertEquals(0, cache.getGenericCacheEntryCount());
      assertEquals(0, cache.getActiveCacheEntryCount());
    }

    @Test
    public void testActiveKeysCacheKeyEqualsAndHashCode() {
      byte[] custodian1 = new byte[] { 1, 2, 3 };
      byte[] custodian2 = new byte[] { 1, 2, 3 };
      byte[] custodian3 = new byte[] { 4, 5, 6 };
      String namespace1 = "ns1";
      String namespace2 = "ns2";

      // Reflexive
      ManagedKeyDataCache.ActiveKeysCacheKey key1 =
        new ManagedKeyDataCache.ActiveKeysCacheKey(custodian1, namespace1);
      assertTrue(key1.equals(key1));

      // Symmetric and consistent for equal content
      ManagedKeyDataCache.ActiveKeysCacheKey key2 =
        new ManagedKeyDataCache.ActiveKeysCacheKey(custodian2, namespace1);
      assertTrue(key1.equals(key2));
      assertTrue(key2.equals(key1));
      assertEquals(key1.hashCode(), key2.hashCode());

      // Different custodian
      ManagedKeyDataCache.ActiveKeysCacheKey key3 =
        new ManagedKeyDataCache.ActiveKeysCacheKey(custodian3, namespace1);
      assertFalse(key1.equals(key3));
      assertFalse(key3.equals(key1));

      // Different namespace
      ManagedKeyDataCache.ActiveKeysCacheKey key4 =
        new ManagedKeyDataCache.ActiveKeysCacheKey(custodian1, namespace2);
      assertFalse(key1.equals(key4));
      assertFalse(key4.equals(key1));

      // Null and different class
      assertFalse(key1.equals(null));
      assertFalse(key1.equals("not a key"));

      // Both fields different
      ManagedKeyDataCache.ActiveKeysCacheKey key5 =
        new ManagedKeyDataCache.ActiveKeysCacheKey(custodian3, namespace2);
      assertFalse(key1.equals(key5));
      assertFalse(key5.equals(key1));
    }
  }

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestWithoutL2Cache extends TestManagedKeyDataCache {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestWithoutL2Cache.class);

    @Before
    public void setUp() {
      super.setUp();
      cache = new ManagedKeyDataCache(conf, null);
    }

    @Test
    public void testGenericCacheForNonExistentKey() throws Exception {
      assertNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, "test-metadata", null));
      verify(testProvider).unwrapKey(any(String.class), any());
    }

    @Test
    public void testWithInvalidProvider() throws Exception {
      ManagedKeyData globalKey1 = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      doThrow(new IOException("Test exception")).when(testProvider).unwrapKey(any(String.class),
        any());
      assertNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, globalKey1.getKeyMetadata(), null));
      verify(testProvider).unwrapKey(any(String.class), any());
      // A second call to getEntry should not result in a call to the provider due to -ve entry.
      clearInvocations(testProvider);
      verify(testProvider, never()).unwrapKey(any(String.class), any());
      assertNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, globalKey1.getKeyMetadata(), null));
      doThrow(new IOException("Test exception")).when(testProvider).getManagedKey(any(),
        any(String.class));
      assertNull(cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      verify(testProvider).getManagedKey(any(), any(String.class));
      // A second call to getRandomEntry should not result in a call to the provider due to -ve
      // entry.
      clearInvocations(testProvider);
      assertNull(cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      verify(testProvider, never()).getManagedKey(any(), any(String.class));
    }

    @Test
    public void testGenericCache() throws Exception {
      ManagedKeyData globalKey1 = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      assertEquals(globalKey1,
        cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, globalKey1.getKeyMetadata(), null));
      verify(testProvider).getManagedKey(any(), any(String.class));
      clearInvocations(testProvider);
      ManagedKeyData globalKey2 = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      assertEquals(globalKey2,
        cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, globalKey2.getKeyMetadata(), null));
      verify(testProvider).getManagedKey(any(), any(String.class));
      clearInvocations(testProvider);
      ManagedKeyData globalKey3 = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      assertEquals(globalKey3,
        cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, globalKey3.getKeyMetadata(), null));
      verify(testProvider).getManagedKey(any(), any(String.class));
    }

    @Test
    public void testActiveKeysCache() throws Exception {
      assertNotNull(cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      verify(testProvider).getManagedKey(any(), any(String.class));
      clearInvocations(testProvider);
      ManagedKeyData activeKey = cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL);
      assertNotNull(activeKey);
      assertEquals(activeKey, cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      verify(testProvider, never()).getManagedKey(any(), any(String.class));
    }

    @Test
    public void testGenericCacheOperations() throws Exception {
      ManagedKeyData globalKey1 = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      ManagedKeyData nsKey1 = testProvider.getManagedKey(CUST_ID, "namespace1");
      assertGenericCacheEntries(nsKey1, globalKey1);
      ManagedKeyData globalKey2 = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      assertGenericCacheEntries(globalKey2, nsKey1, globalKey1);
      ManagedKeyData nsKey2 = testProvider.getManagedKey(CUST_ID, "namespace1");
      assertGenericCacheEntries(nsKey2, globalKey2, nsKey1, globalKey1);
    }

    @Test
    public void testActiveKeyGetNoActive() throws Exception {
      testProvider.setMockedKeyState(ALIAS, FAILED);
      assertNull(cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      verify(testProvider).getManagedKey(any(), any(String.class));
      clearInvocations(testProvider);
      assertNull(cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      verify(testProvider, never()).getManagedKey(any(), any(String.class));
    }

    @Test
    public void testActiveKeysCacheOperations() throws Exception {
      assertNotNull(cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      assertNotNull(cache.getActiveEntry(CUST_ID, "namespace1"));
      assertEquals(2, cache.getActiveCacheEntryCount());

      cache.clearCache();
      assertEquals(0, cache.getActiveCacheEntryCount());
      assertNotNull(cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      assertEquals(1, cache.getActiveCacheEntryCount());
    }

    @Test
    public void testGenericCacheUsingActiveKeysCacheOverProvider() throws Exception {
      ManagedKeyData key = cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL);
      assertNotNull(key);
      assertEquals(key, cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadata(), null));
      verify(testProvider, never()).unwrapKey(any(String.class), any());
    }

    @Test
    public void testThatActiveKeysCache_SkipsProvider_WhenLoadedViaGenericCache() throws Exception {
      ManagedKeyData key1 = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      assertEquals(key1, cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key1.getKeyMetadata(), null));
      ManagedKeyData key2 = testProvider.getManagedKey(CUST_ID, "namespace1");
      assertEquals(key2, cache.getEntry(CUST_ID, "namespace1", key2.getKeyMetadata(), null));
      verify(testProvider, times(2)).getManagedKey(any(), any(String.class));
      assertEquals(2, cache.getActiveCacheEntryCount());
      clearInvocations(testProvider);
      assertEquals(key1, cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      assertEquals(key2, cache.getActiveEntry(CUST_ID, "namespace1"));
      // ACTIVE keys are automatically added to activeKeysCache when loaded
      // via getEntry, so getActiveEntry will find them there and won't call the provider
      verify(testProvider, never()).getManagedKey(any(), any(String.class));
      cache.clearCache();
      assertEquals(0, cache.getActiveCacheEntryCount());
    }

    @Test
    public void testThatNonActiveKey_IsIgnored_WhenLoadedViaGenericCache() throws Exception {
      testProvider.setMockedKeyState(ALIAS, FAILED);
      ManagedKeyData key = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      assertNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadata(), null));
      assertEquals(0, cache.getActiveCacheEntryCount());

      testProvider.setMockedKeyState(ALIAS, DISABLED);
      key = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      assertNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadata(), null));
      assertEquals(0, cache.getActiveCacheEntryCount());

      testProvider.setMockedKeyState(ALIAS, INACTIVE);
      key = testProvider.getManagedKey(CUST_ID, "namespace1");
      assertEquals(key, cache.getEntry(CUST_ID, "namespace1", key.getKeyMetadata(), null));
      assertEquals(0, cache.getActiveCacheEntryCount());
    }

    @Test
    public void testActiveKeysCacheWithMultipleCustodiansInGenericCache() throws Exception {
      ManagedKeyData key1 = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      assertNotNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key1.getKeyMetadata(), null));
      String alias2 = "cust2";
      byte[] cust_id2 = alias2.getBytes();
      ManagedKeyData key2 = testProvider.getManagedKey(cust_id2, KEY_SPACE_GLOBAL);
      assertNotNull(cache.getEntry(cust_id2, KEY_SPACE_GLOBAL, key2.getKeyMetadata(), null));
      assertNotNull(cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      // ACTIVE keys are automatically added to activeKeysCache when loaded.
      assertEquals(2, cache.getActiveCacheEntryCount());
    }

    @Test
    public void testActiveKeysCacheWithMultipleNamespaces() throws Exception {
      ManagedKeyData key1 = cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL);
      assertNotNull(key1);
      assertEquals(key1, cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      ManagedKeyData key2 = cache.getActiveEntry(CUST_ID, "namespace1");
      assertNotNull(key2);
      assertEquals(key2, cache.getActiveEntry(CUST_ID, "namespace1"));
      ManagedKeyData key3 = cache.getActiveEntry(CUST_ID, "namespace2");
      assertNotNull(key3);
      assertEquals(key3, cache.getActiveEntry(CUST_ID, "namespace2"));
      verify(testProvider, times(3)).getManagedKey(any(), any(String.class));
      assertEquals(3, cache.getActiveCacheEntryCount());
    }

    @Test
    public void testEjectKey_ActiveKeysCacheOnly() throws Exception {
      // Load a key into the active keys cache
      ManagedKeyData key = cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL);
      assertNotNull(key);
      assertEquals(1, cache.getActiveCacheEntryCount());

      // Eject the key - should remove from active keys cache
      boolean ejected = cache.ejectKey(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadataHash());
      assertTrue("Key should be ejected when metadata matches", ejected);
      assertEquals(0, cache.getActiveCacheEntryCount());

      // Try to eject again - should return false since it's already gone from active keys cache
      boolean ejectedAgain = cache.ejectKey(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadataHash());
      assertFalse("Should return false when key is already ejected", ejectedAgain);
      assertEquals(0, cache.getActiveCacheEntryCount());
    }

    @Test
    public void testEjectKey_GenericCacheOnly() throws Exception {
      // Load a key into the generic cache
      ManagedKeyData key = cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL,
        testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL).getKeyMetadata(), null);
      assertNotNull(key);
      assertEquals(1, cache.getGenericCacheEntryCount());

      // Eject the key - should remove from generic cache
      boolean ejected = cache.ejectKey(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadataHash());
      assertTrue("Key should be ejected when metadata matches", ejected);
      assertEquals(0, cache.getGenericCacheEntryCount());

      // Try to eject again - should return false since it's already gone from generic cache
      boolean ejectedAgain = cache.ejectKey(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadataHash());
      assertFalse("Should return false when key is already ejected", ejectedAgain);
      assertEquals(0, cache.getGenericCacheEntryCount());
    }

    @Test
    public void testEjectKey_Success() throws Exception {
      // Load a key into the active keys cache
      ManagedKeyData key = cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL);
      assertNotNull(key);
      String metadata = key.getKeyMetadata();
      assertEquals(1, cache.getActiveCacheEntryCount());

      // Also load into the generic cache
      ManagedKeyData keyFromGeneric = cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, metadata, null);
      assertNotNull(keyFromGeneric);
      assertEquals(1, cache.getGenericCacheEntryCount());

      // Eject the key with matching metadata - should remove from both caches
      boolean ejected = cache.ejectKey(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadataHash());
      assertTrue("Key should be ejected when metadata matches", ejected);
      assertEquals(0, cache.getActiveCacheEntryCount());
      assertEquals(0, cache.getGenericCacheEntryCount());

      // Try to eject again - should return false since it's already gone from active keys cache
      boolean ejectedAgain = cache.ejectKey(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadataHash());
      assertFalse("Should return false when key is already ejected", ejectedAgain);
      assertEquals(0, cache.getActiveCacheEntryCount());
      assertEquals(0, cache.getGenericCacheEntryCount());
    }

    @Test
    public void testEjectKey_MetadataMismatch() throws Exception {
      // Load a key into both caches
      ManagedKeyData key = cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL);
      assertNotNull(key);
      assertEquals(1, cache.getActiveCacheEntryCount());

      // Also load into the generic cache
      cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadataHash());
      assertEquals(1, cache.getGenericCacheEntryCount());

      // Try to eject with wrong metadata - should not eject from either cache
      String wrongMetadata = "wrong-metadata";
      boolean ejected = cache.ejectKey(CUST_ID, KEY_SPACE_GLOBAL,
        ManagedKeyData.constructMetadataHash(wrongMetadata));
      assertFalse("Key should not be ejected when metadata doesn't match", ejected);
      assertEquals(1, cache.getActiveCacheEntryCount());
      assertEquals(1, cache.getGenericCacheEntryCount());

      // Verify the key is still in both caches
      assertEquals(key, cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      assertEquals(key.getKeyMetadata(),
        cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadataHash()).getKeyMetadata());
    }

    @Test
    public void testEjectKey_KeyNotPresent() throws Exception {
      // Try to eject a key that doesn't exist in the cache
      String nonExistentMetadata = "non-existent-metadata";
      boolean ejected = cache.ejectKey(CUST_ID, "non-existent-namespace",
        ManagedKeyData.constructMetadataHash(nonExistentMetadata));
      assertFalse("Should return false when key is not present", ejected);
      assertEquals(0, cache.getActiveCacheEntryCount());
    }

    @Test
    public void testEjectKey_MultipleKeys() throws Exception {
      // Load multiple keys into both caches
      ManagedKeyData key1 = cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL);
      ManagedKeyData key2 = cache.getActiveEntry(CUST_ID, "namespace1");
      ManagedKeyData key3 = cache.getActiveEntry(CUST_ID, "namespace2");
      assertNotNull(key1);
      assertNotNull(key2);
      assertNotNull(key3);
      assertEquals(3, cache.getActiveCacheEntryCount());

      // Also load all keys into the generic cache
      cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key1.getKeyMetadata(), null);
      cache.getEntry(CUST_ID, "namespace1", key2.getKeyMetadata(), null);
      cache.getEntry(CUST_ID, "namespace2", key3.getKeyMetadata(), null);
      assertEquals(3, cache.getGenericCacheEntryCount());

      // Eject only the middle key from both caches
      boolean ejected = cache.ejectKey(CUST_ID, "namespace1", key2.getKeyMetadataHash());
      assertTrue("Key should be ejected from both caches", ejected);
      assertEquals(2, cache.getActiveCacheEntryCount());
      assertEquals(2, cache.getGenericCacheEntryCount());

      // Verify only key2 was ejected - key1 and key3 should still be there
      clearInvocations(testProvider);
      assertEquals(key1, cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      assertEquals(key3, cache.getActiveEntry(CUST_ID, "namespace2"));
      // These getActiveEntry() calls should not trigger provider calls since keys are still cached
      verify(testProvider, never()).getManagedKey(any(), any(String.class));

      // Verify generic cache still has key1 and key3
      assertEquals(key1.getKeyMetadata(),
        cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key1.getKeyMetadata(), null).getKeyMetadata());
      assertEquals(key3.getKeyMetadata(),
        cache.getEntry(CUST_ID, "namespace2", key3.getKeyMetadata(), null).getKeyMetadata());

      // Try to eject key2 again - should return false since it's already gone from both caches
      boolean ejectedAgain = cache.ejectKey(CUST_ID, "namespace1", key2.getKeyMetadataHash());
      assertFalse("Should return false when key is already ejected", ejectedAgain);
      assertEquals(2, cache.getActiveCacheEntryCount());
      assertEquals(2, cache.getGenericCacheEntryCount());
    }

    @Test
    public void testEjectKey_DifferentCustodian() throws Exception {
      // Load a key for one custodian into both caches
      ManagedKeyData key = cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL);
      assertNotNull(key);
      String metadata = key.getKeyMetadata();
      assertEquals(1, cache.getActiveCacheEntryCount());

      // Also load into the generic cache
      cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadataHash());
      assertEquals(1, cache.getGenericCacheEntryCount());

      // Try to eject with a different custodian - should not eject from either cache
      byte[] differentCustodian = "different-cust".getBytes();
      boolean ejected =
        cache.ejectKey(differentCustodian, KEY_SPACE_GLOBAL, key.getKeyMetadataHash());
      assertFalse("Should not eject key for different custodian", ejected);
      assertEquals(1, cache.getActiveCacheEntryCount());
      assertEquals(1, cache.getGenericCacheEntryCount());

      // Verify the original key is still in both caches
      assertEquals(key, cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      assertEquals(metadata,
        cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, metadata, null).getKeyMetadata());
    }

    @Test
    public void testEjectKey_AfterClearCache() throws Exception {
      // Load a key into both caches
      ManagedKeyData key = cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL);
      assertNotNull(key);
      String metadata = key.getKeyMetadata();
      assertEquals(1, cache.getActiveCacheEntryCount());

      // Also load into the generic cache
      cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, metadata, null);
      assertEquals(1, cache.getGenericCacheEntryCount());

      // Clear both caches
      cache.clearCache();
      assertEquals(0, cache.getActiveCacheEntryCount());
      assertEquals(0, cache.getGenericCacheEntryCount());

      // Try to eject the key after both caches are cleared
      boolean ejected = cache.ejectKey(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadataHash());
      assertFalse("Should return false when both caches are empty", ejected);
      assertEquals(0, cache.getActiveCacheEntryCount());
      assertEquals(0, cache.getGenericCacheEntryCount());
    }

    @Test
    public void testGetEntry_HashCollisionOrMismatchDetection() throws Exception {
      // Create a key and get it into the cache
      ManagedKeyData key1 = cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL);
      assertNotNull(key1);

      // Now simulate a hash collision by trying to get an entry with the same hash
      // but different custodian/namespace
      byte[] differentCust = "different-cust".getBytes();
      String differentNamespace = "different-namespace";

      // This should return null due to custodian/namespace mismatch (collision detection)
      ManagedKeyData result =
        cache.getEntry(differentCust, differentNamespace, key1.getKeyMetadata(), null);

      // Result should be null because of hash collision detection
      // The cache finds an entry with the same metadata hash, but custodian/namespace don't match
      assertNull("Should return null when hash collision is detected", result);
    }

    @Test
    public void testEjectKey_HashCollisionOrMismatchProtection() throws Exception {
      // Create two keys with potential hash collision scenario
      byte[] cust1 = "cust1".getBytes();
      byte[] cust2 = "cust2".getBytes();
      String namespace1 = "namespace1";

      // Load a key for cust1
      ManagedKeyData key1 = cache.getActiveEntry(cust1, namespace1);
      assertNotNull(key1);
      assertEquals(1, cache.getActiveCacheEntryCount());

      // Try to eject using same metadata hash but different custodian
      // This should not eject the key due to custodian mismatch protection
      boolean ejected = cache.ejectKey(cust2, namespace1, key1.getKeyMetadataHash());
      assertFalse("Should not eject key with different custodian even if hash matches", ejected);
      assertEquals(1, cache.getActiveCacheEntryCount());

      // Verify the original key is still there
      assertEquals(key1, cache.getActiveEntry(cust1, namespace1));
    }
  }

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestWithL2CacheAndNoDynamicLookup extends TestManagedKeyDataCache {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestWithL2CacheAndNoDynamicLookup.class);
    private KeymetaTableAccessor mockL2 = mock(KeymetaTableAccessor.class);

    @Before
    public void setUp() {
      super.setUp();
      conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_DYNAMIC_LOOKUP_ENABLED_CONF_KEY, false);
      cache = new ManagedKeyDataCache(conf, mockL2);
    }

    @Test
    public void testGenericCacheNonExistentKeyInL2Cache() throws Exception {
      assertNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, "test-metadata", null));
      verify(mockL2).getKey(any(), any(String.class), any(byte[].class));
      clearInvocations(mockL2);
      assertNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, "test-metadata", null));
      verify(mockL2, never()).getKey(any(), any(String.class), any(byte[].class));
    }

    @Test
    public void testGenericCacheRetrievalFromL2Cache() throws Exception {
      ManagedKeyData key = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      when(mockL2.getKey(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadataHash())).thenReturn(key);
      assertEquals(key, cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadata(), null));
      verify(mockL2).getKey(any(), any(String.class), any(byte[].class));
    }

    @Test
    public void testActiveKeysCacheNonExistentKeyInL2Cache() throws Exception {
      assertNull(cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      verify(mockL2).getActiveKey(any(), any(String.class));
      clearInvocations(mockL2);
      assertNull(cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      verify(mockL2, never()).getActiveKey(any(), any(String.class));
    }

    @Test
    public void testActiveKeysCacheRetrievalFromL2Cache() throws Exception {
      ManagedKeyData key = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      when(mockL2.getActiveKey(CUST_ID, KEY_SPACE_GLOBAL)).thenReturn(key);
      assertEquals(key, cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      verify(mockL2).getActiveKey(any(), any(String.class));
      clearInvocations(mockL2);
      assertEquals(key, cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      verify(mockL2, never()).getActiveKey(any(), any(String.class));
    }

    @Test
    public void testGenericCacheWithKeymetaAccessorException() throws Exception {
      when(mockL2.getKey(eq(CUST_ID), eq(KEY_SPACE_GLOBAL), any(byte[].class)))
        .thenThrow(new IOException("Test exception"));
      assertNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, "test-metadata", null));
      verify(mockL2).getKey(any(), any(String.class), any(byte[].class));
      clearInvocations(mockL2);
      assertNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, "test-metadata", null));
      verify(mockL2, never()).getKey(any(), any(String.class), any(byte[].class));
    }

    @Test
    public void testGetActiveEntryWithKeymetaAccessorException() throws Exception {
      when(mockL2.getActiveKey(CUST_ID, KEY_SPACE_GLOBAL))
        .thenThrow(new IOException("Test exception"));
      assertNull(cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      verify(mockL2).getActiveKey(any(), any(String.class));
      clearInvocations(mockL2);
      assertNull(cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      verify(mockL2, never()).getActiveKey(any(), any(String.class));
    }

    @Test
    public void testActiveKeysCacheUsesKeymetaAccessorWhenGenericCacheEmpty() throws Exception {
      // Ensure generic cache is empty
      cache.clearCache();

      // Mock the keymetaAccessor to return a key
      ManagedKeyData key = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      when(mockL2.getActiveKey(CUST_ID, KEY_SPACE_GLOBAL)).thenReturn(key);

      // Get the active entry - it should call keymetaAccessor since generic cache is empty
      assertEquals(key, cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      verify(mockL2).getActiveKey(any(), any(String.class));
    }
  }

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestWithL2CacheAndDynamicLookup extends TestManagedKeyDataCache {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestWithL2CacheAndDynamicLookup.class);
    private KeymetaTableAccessor mockL2 = mock(KeymetaTableAccessor.class);

    @Before
    public void setUp() {
      super.setUp();
      conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_DYNAMIC_LOOKUP_ENABLED_CONF_KEY, true);
      cache = new ManagedKeyDataCache(conf, mockL2);
    }

    @Test
    public void testGenericCacheRetrivalFromProviderWhenKeyNotFoundInL2Cache() throws Exception {
      ManagedKeyData key = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      doReturn(key).when(testProvider).unwrapKey(any(String.class), any());
      assertEquals(key, cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadata(), null));
      verify(mockL2).getKey(any(), any(String.class), any(byte[].class));
      verify(mockL2).addKey(any(ManagedKeyData.class));
    }

    @Test
    public void testAddKeyFailure() throws Exception {
      ManagedKeyData key = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      doReturn(key).when(testProvider).unwrapKey(any(String.class), any());
      doThrow(new IOException("Test exception")).when(mockL2).addKey(any(ManagedKeyData.class));
      assertEquals(key, cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadata(), null));
      verify(mockL2).addKey(any(ManagedKeyData.class));
    }

    @Test
    public void testGenericCacheDynamicLookupUnexpectedException() throws Exception {
      doThrow(new RuntimeException("Test exception")).when(testProvider)
        .unwrapKey(any(String.class), any());
      assertNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, "test-metadata", null));
      assertNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, "test-metadata", null));
      verify(mockL2).getKey(any(), any(String.class), any(byte[].class));
      verify(mockL2, never()).addKey(any(ManagedKeyData.class));
    }

    @Test
    public void testActiveKeysCacheDynamicLookupWithUnexpectedException() throws Exception {
      doThrow(new RuntimeException("Test exception")).when(testProvider).getManagedKey(any(),
        any(String.class));
      assertNull(cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      verify(testProvider).getManagedKey(any(), any(String.class));
      clearInvocations(testProvider);
      // A 2nd invocation should not result in a call to the provider.
      assertNull(cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      verify(testProvider, never()).getManagedKey(any(), any(String.class));
    }

    @Test
    public void testActiveKeysCacheRetrivalFromProviderWhenKeyNotFoundInL2Cache() throws Exception {
      ManagedKeyData key = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      doReturn(key).when(testProvider).getManagedKey(any(), any(String.class));
      assertEquals(key, cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      verify(mockL2).getActiveKey(any(), any(String.class));
    }

    @Test
    public void testGenericCacheUsesActiveKeysCacheFirst() throws Exception {
      // First populate the active keys cache with an active key
      ManagedKeyData key1 = cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL);
      verify(testProvider).getManagedKey(any(), any(String.class));
      clearInvocations(testProvider);

      // Now get the generic cache entry - it should use the active keys cache first, not call
      // keymetaAccessor
      assertEquals(key1, cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key1.getKeyMetadata(), null));
      verify(testProvider, never()).getManagedKey(any(), any(String.class));

      // Lookup a diffrent key.
      ManagedKeyData key2 = cache.getActiveEntry(CUST_ID, "namespace1");
      assertNotEquals(key1, key2);
      verify(testProvider).getManagedKey(any(), any(String.class));
      clearInvocations(testProvider);

      // Now get the generic cache entry - it should use the active keys cache first, not call
      // keymetaAccessor
      assertEquals(key2, cache.getEntry(CUST_ID, "namespace1", key2.getKeyMetadata(), null));
      verify(testProvider, never()).getManagedKey(any(), any(String.class));
    }

    @Test
    public void testGetOlderEntryFromGenericCache() throws Exception {
      // Get one version of the key in to ActiveKeysCache
      ManagedKeyData key1 = cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL);
      assertNotNull(key1);
      clearInvocations(testProvider);

      // Now try to lookup another version of the key, it should lookup and discard the active key.
      ManagedKeyData key2 = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      assertEquals(key2, cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key2.getKeyMetadata(), null));
      verify(testProvider).unwrapKey(any(String.class), any());
    }

    @Test
    public void testThatActiveKeysCache_PopulatedByGenericCache() throws Exception {
      // First populate the generic cache with an active key
      ManagedKeyData key = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      assertEquals(key, cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadata(), null));
      verify(testProvider).unwrapKey(any(String.class), any());

      // Clear invocations to reset the mock state
      clearInvocations(testProvider);

      // Now get the active entry - it should already be there due to the generic cache first
      assertEquals(key, cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      verify(testProvider, never()).unwrapKey(any(String.class), any());
    }
  }

  protected void assertGenericCacheEntries(ManagedKeyData... keys) throws Exception {
    for (ManagedKeyData key : keys) {
      assertEquals(key,
        cache.getEntry(key.getKeyCustodian(), key.getKeyNamespace(), key.getKeyMetadata(), null));
    }
    assertEquals(keys.length, cache.getGenericCacheEntryCount());
    int activeKeysCount =
      Arrays.stream(keys).filter(key -> key.getKeyState() == ManagedKeyState.ACTIVE)
        .map(key -> new ManagedKeyDataCache.ActiveKeysCacheKey(key.getKeyCustodian(),
          key.getKeyNamespace()))
        .collect(Collectors.toSet()).size();
    assertEquals(activeKeysCount, cache.getActiveCacheEntryCount());
  }
}
