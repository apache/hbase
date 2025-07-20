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
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.ACTIVE;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.FAILED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
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

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.matcher.ElementMatchers;
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    TestManagedKeyDataCache.TestGeneric.class,
    TestManagedKeyDataCache.TestWithoutL2Cache.class,
    TestManagedKeyDataCache.TestWithL2CacheAndNoDynamicLookup.class,
    TestManagedKeyDataCache.TestWithL2CacheAndDynamicLookup.class,
})
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
    providerClass = new ByteBuddy()
        .subclass(MockManagedKeyProvider.class)
        .name("org.apache.hadoop.hbase.io.crypto.MockManagedKeyProviderSpy")
        .method(ElementMatchers.any()) // Intercept all methods
        // Using a delegator instead of directly forwarding to testProvider to
        // facilitate switching the testProvider instance. Besides, it
        .intercept(MethodDelegation.to(new ForwardingInterceptor()))
        .make()
        .load(MockManagedKeyProvider.class.getClassLoader(), ClassLoadingStrategy.Default.INJECTION)
        .getLoaded();
  }

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    ForwardingInterceptor.setDelegate(testProvider);

    Encryption.clearKeyProviderCache();

    conf.set(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, "true");
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, providerClass.getName());

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
      byte[] custodian1 = new byte[] {1, 2, 3};
      byte[] custodian2 = new byte[] {1, 2, 3};
      byte[] custodian3 = new byte[] {4, 5, 6};
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
      assertEquals(globalKey1, cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL,
          globalKey1.getKeyMetadata(), null));
      verify(testProvider).getManagedKey(any(), any(String.class));
      clearInvocations(testProvider);
      ManagedKeyData globalKey2 = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      assertEquals(globalKey2, cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL,
          globalKey2.getKeyMetadata(), null));
      verify(testProvider).getManagedKey(any(), any(String.class));
      clearInvocations(testProvider);
      ManagedKeyData globalKey3 = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      assertEquals(globalKey3, cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL,
          globalKey3.getKeyMetadata(), null));
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
      ManagedKeyData globalKey2 = testProvider.getManagedKey(CUST_ID,
        KEY_SPACE_GLOBAL);
      assertGenericCacheEntries(globalKey2, nsKey1, globalKey1);
      ManagedKeyData nsKey2 = testProvider.getManagedKey(CUST_ID,
        "namespace1");
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

      cache.invalidateAll();
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
    public void testActiveKeysCacheSkippingProviderWhenGenericCacheEntriesExist() throws Exception {
      ManagedKeyData key1 = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      assertEquals(key1, cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key1.getKeyMetadata(), null));
      ManagedKeyData key2 = testProvider.getManagedKey(CUST_ID, "namespace1");
      assertEquals(key2, cache.getEntry(CUST_ID, "namespace1", key2.getKeyMetadata(), null));
      verify(testProvider, times(2)).getManagedKey(any(), any(String.class));
      clearInvocations(testProvider);
      assertEquals(key1, cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      // In this case, the provider is not called because the existing keys in generic cache are
      // used first (before checking keymetaAccessor).
      verify(testProvider, never()).getManagedKey(any(), any(String.class));
      assertEquals(1, cache.getActiveCacheEntryCount());
      cache.invalidateAll();
      assertEquals(0, cache.getActiveCacheEntryCount());
    }

    @Test
    public void testActiveKeysCacheIgnnoreFailedKeyInGenericCache() throws Exception {
      testProvider.setMockedKeyState(ALIAS, FAILED);
      ManagedKeyData key = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      assertNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadata(), null));
      clearInvocations(testProvider);
      testProvider.setMockedKeyState(ALIAS, ACTIVE);
      assertNotNull(cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      verify(testProvider).getManagedKey(any(), any(String.class));
    }

    @Test
    public void testActiveKeysCacheWithMultipleCustodiansInGenericCache() throws Exception {
      ManagedKeyData key1 = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      assertNotNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key1.getKeyMetadata(), null));
      String alias2 = "cust2";
      byte[] cust_id2 = alias2.getBytes();
      ManagedKeyData key2 = testProvider.getManagedKey(cust_id2, KEY_SPACE_GLOBAL);
      assertNotNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key2.getKeyMetadata(), null));
      assertNotNull(cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      assertEquals(1, cache.getActiveCacheEntryCount());
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
      verify(mockL2).getKey(any(), any(String.class), any(String.class));
      clearInvocations(mockL2);
      assertNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, "test-metadata", null));
      verify(mockL2, never()).getKey(any(), any(String.class), any(String.class));
    }

    @Test
    public void testGenericCacheRetrievalFromL2Cache() throws Exception {
      ManagedKeyData key = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      when(mockL2.getKey(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadata()))
          .thenReturn(key);
      assertEquals(key, cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadata(), null));
      verify(mockL2).getKey(any(), any(String.class), any(String.class));
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
      when(mockL2.getActiveKey(CUST_ID, KEY_SPACE_GLOBAL))
          .thenReturn(key);
      assertEquals(key, cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      verify(mockL2).getActiveKey(any(), any(String.class));
    }

    @Test
    public void testGenericCacheWithKeymetaAccessorException() throws Exception {
      when(mockL2.getKey(CUST_ID, KEY_SPACE_GLOBAL, "test-metadata"))
          .thenThrow(new IOException("Test exception"));
      assertNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, "test-metadata", null));
      verify(mockL2).getKey(any(), any(String.class), any(String.class));
      clearInvocations(mockL2);
      assertNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, "test-metadata", null));
      verify(mockL2, never()).getKey(any(), any(String.class), any(String.class));
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
      cache.invalidateAll();

      // Mock the keymetaAccessor to return a key
      ManagedKeyData key = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      when(mockL2.getActiveKey(CUST_ID, KEY_SPACE_GLOBAL))
          .thenReturn(key);

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
      verify(mockL2).getKey(any(), any(String.class), any(String.class));
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
      doThrow(new RuntimeException("Test exception")).when(testProvider).unwrapKey(any(String.class), any());
      assertNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, "test-metadata", null));
      assertNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, "test-metadata", null));
      verify(mockL2).getKey(any(), any(String.class), any(String.class));
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
    public void testActiveKeysCacheUsesGenericCacheFirst() throws Exception {
      // First populate the generic cache with an active key
      ManagedKeyData key = testProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      assertEquals(key, cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, key.getKeyMetadata(), null));

      // Clear invocations to reset the mock state
      clearInvocations(mockL2);

      // Now get the active entry - it should use the generic cache first, not call keymetaAccessor
      assertEquals(key, cache.getActiveEntry(CUST_ID, KEY_SPACE_GLOBAL));
      verify(mockL2, never()).getActiveKey(any(), any(String.class));
    }
  }

  protected void assertGenericCacheEntries(ManagedKeyData... keys) throws Exception {
    for (ManagedKeyData key: keys) {
      assertEquals(key, cache.getEntry(key.getKeyCustodian(), key.getKeyNamespace(),
          key.getKeyMetadata(), null));
    }
    assertEquals(keys.length, cache.getGenericCacheEntryCount());
    assertEquals(0, cache.getActiveCacheEntryCount());
  }
}
