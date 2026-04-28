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

import static org.apache.hadoop.hbase.io.crypto.ManagedKeyData.KEY_SPACE_GLOBAL_BYTES;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.ACTIVE;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.DISABLED;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.FAILED;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.INACTIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
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
import java.security.Key;
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
import org.apache.hadoop.hbase.util.Bytes;
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
  private static final Bytes CUST_ID_BYTES = new Bytes(CUST_ID);
  private static final ManagedKeyIdentity CUST_KEY_SPACE_GLOBAL_ID =
    new KeyIdentityPrefixBytesBacked(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES);
  private static final ManagedKeyIdentity CUST_NAMESPACE1_ID =
    new KeyIdentityPrefixBytesBacked(CUST_ID_BYTES, new Bytes(Bytes.toBytes("namespace1")));
  private static Class<? extends MockManagedKeyProvider> providerClass;

  /**
   * Build FullKeyIdentity for use with getEntry(FullKeyIdentity, String, byte[]). Uses
   * {@link KeyIdentityBytesBacked} for convenience; any {@link ManagedKeyIdentity} implementation
   * is interchangeable as cache keys because equality and hashCode are content-based (see
   * {@link ManagedKeyIdentity#contentEquals
   * contentEquals}/{@link ManagedKeyIdentity#contentHashCode contentHashCode}).
   * {@link TestManagedKeyIdentity} verifies cross-type equality, hashCode consistency, and map/set
   * interoperability, so parameterizing these tests by concrete type is not required.
   */
  private static ManagedKeyIdentity fullKeyIdentity(byte[] cust, byte[] ns, byte[] partial) {
    if (partial == null) {
      return new KeyIdentityPrefixBytesBacked(new Bytes(cust), new Bytes(ns));
    } else {
      return new KeyIdentityBytesBacked(new Bytes(cust), new Bytes(ns), new Bytes(partial));
    }
  }

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
    public void testGenericCacheForInvalidMetadata() throws Exception {
      assertNull(cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null),
        "test-metadata", null));
      verify(testProvider).unwrapKey(any(ManagedKeyIdentity.class), any(String.class), any());
    }

    @Test
    public void testWithInvalidProvider() throws Exception {
      ManagedKeyData globalKey1 = testProvider.getManagedKey(CUST_KEY_SPACE_GLOBAL_ID);
      doThrow(new IOException("Test exception")).when(testProvider)
        .unwrapKey(any(ManagedKeyIdentity.class), any(String.class), any());
      // With no L2 and invalid provider, there will be no entry.
      assertNull(cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null),
        globalKey1.getKeyMetadata(), null));
      verify(testProvider).unwrapKey(any(ManagedKeyIdentity.class), any(String.class), any());
      clearInvocations(testProvider);

      // A second call to getEntry should not result in a call to the provider due to -ve entry.
      assertNull(cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null),
        globalKey1.getKeyMetadata(), null));
      verify(testProvider, never()).unwrapKey(any(ManagedKeyIdentity.class), any(String.class),
        any());

      //
      doThrow(new IOException("Test exception")).when(testProvider).getManagedKey(any());
      assertNull(cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      verify(testProvider).getManagedKey(any());
      clearInvocations(testProvider);

      // A second call to getActiveEntry should not result in a call to the provider due to -ve
      // entry.
      assertNull(cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      verify(testProvider, never()).getManagedKey(any());
    }

    @Test
    public void testGenericCache() throws Exception {
      ManagedKeyData globalKey1 = testProvider.getManagedKey(CUST_KEY_SPACE_GLOBAL_ID);
      assertEquals(globalKey1,
        cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null),
          globalKey1.getKeyMetadata(), null));
      verify(testProvider).getManagedKey(any());
      clearInvocations(testProvider);
      ManagedKeyData globalKey2 = testProvider.getManagedKey(CUST_KEY_SPACE_GLOBAL_ID);
      assertEquals(globalKey2,
        cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null),
          globalKey2.getKeyMetadata(), null));
      verify(testProvider).getManagedKey(any());
      clearInvocations(testProvider);
      ManagedKeyData globalKey3 = testProvider.getManagedKey(CUST_KEY_SPACE_GLOBAL_ID);
      assertEquals(globalKey3,
        cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null),
          globalKey3.getKeyMetadata(), null));
      verify(testProvider).getManagedKey(any());
    }

    @Test
    public void testActiveKeysCache() throws Exception {
      assertNotNull(cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      verify(testProvider).getManagedKey(any());
      clearInvocations(testProvider);
      ManagedKeyData activeKey = cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES);
      assertNotNull(activeKey);
      assertEquals(activeKey, cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      verify(testProvider, never()).getManagedKey(any());
    }

    @Test
    public void testGenericCacheOperations() throws Exception {
      ManagedKeyData globalKey1 = testProvider.getManagedKey(CUST_KEY_SPACE_GLOBAL_ID);
      ManagedKeyData nsKey1 = testProvider.getManagedKey(CUST_NAMESPACE1_ID);
      assertGenericCacheEntries(nsKey1, globalKey1);
      ManagedKeyData globalKey2 = testProvider.getManagedKey(CUST_KEY_SPACE_GLOBAL_ID);
      assertGenericCacheEntries(globalKey2, nsKey1, globalKey1);
      ManagedKeyData nsKey2 = testProvider.getManagedKey(CUST_NAMESPACE1_ID);
      assertGenericCacheEntries(nsKey2, globalKey2, nsKey1, globalKey1);
    }

    @Test
    public void testActiveKeyGetNoActive() throws Exception {
      testProvider.setMockedKeyState(ALIAS, FAILED);
      assertNull(cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      verify(testProvider).getManagedKey(any());
      clearInvocations(testProvider);
      assertNull(cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      verify(testProvider, never()).getManagedKey(any());
    }

    @Test
    public void testActiveKeysCacheOperations() throws Exception {
      assertNotNull(cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      assertNotNull(cache.getActiveEntry(CUST_ID_BYTES, new Bytes(Bytes.toBytes("namespace1"))));
      assertEquals(2, cache.getActiveCacheEntryCount());

      cache.clearCache();
      assertEquals(0, cache.getActiveCacheEntryCount());
      assertNotNull(cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      assertEquals(1, cache.getActiveCacheEntryCount());
    }

    @Test
    public void testGenericCacheUsingActiveKeysCacheOverProvider() throws Exception {
      ManagedKeyData key = cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES);
      assertNotNull(key);
      assertEquals(key, cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null),
        key.getKeyMetadata(), null));
      verify(testProvider, never()).unwrapKey(any(ManagedKeyIdentity.class), any(String.class),
        any());
    }

    @Test
    public void testThatActiveKeysCache_SkipsProvider_WhenLoadedViaGenericCache() throws Exception {
      ManagedKeyData key1 = testProvider.getManagedKey(CUST_KEY_SPACE_GLOBAL_ID);
      assertEquals(key1, cache.getEntry(
        fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null), key1.getKeyMetadata(), null));
      ManagedKeyData key2 = testProvider.getManagedKey(CUST_NAMESPACE1_ID);
      assertEquals(key2, cache.getEntry(fullKeyIdentity(CUST_ID, Bytes.toBytes("namespace1"), null),
        key2.getKeyMetadata(), null));
      verify(testProvider, times(2)).getManagedKey(any());
      assertEquals(2, cache.getActiveCacheEntryCount());
      clearInvocations(testProvider);
      assertEquals(key1, cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      assertEquals(key2,
        cache.getActiveEntry(CUST_ID_BYTES, new Bytes(Bytes.toBytes("namespace1"))));
      // ACTIVE keys are automatically added to activeKeysCache when loaded
      // via getEntry, so getActiveEntry will find them there and won't call the provider
      verify(testProvider, never()).getManagedKey(any());
      cache.clearCache();
      assertEquals(0, cache.getActiveCacheEntryCount());
    }

    @Test
    public void testThatNonActiveKey_IsIgnored_WhenLoadedViaGenericCache() throws Exception {
      testProvider.setMockedKeyState(ALIAS, FAILED);
      ManagedKeyData key = testProvider.getManagedKey(CUST_KEY_SPACE_GLOBAL_ID);
      assertNull(cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null),
        key.getKeyMetadata(), null));
      assertEquals(0, cache.getActiveCacheEntryCount());

      testProvider.setMockedKeyState(ALIAS, DISABLED);
      key = testProvider.getManagedKey(CUST_KEY_SPACE_GLOBAL_ID);
      assertNull(cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null),
        key.getKeyMetadata(), null));
      assertEquals(0, cache.getActiveCacheEntryCount());

      testProvider.setMockedKeyState(ALIAS, INACTIVE);
      key = testProvider.getManagedKey(CUST_NAMESPACE1_ID);
      assertEquals(key, cache.getEntry(fullKeyIdentity(CUST_ID, Bytes.toBytes("namespace1"), null),
        key.getKeyMetadata(), null));
      assertEquals(0, cache.getActiveCacheEntryCount());
    }

    @Test
    public void testActiveKeysCacheWithMultipleCustodiansInGenericCache() throws Exception {
      ManagedKeyData key1 = testProvider.getManagedKey(CUST_KEY_SPACE_GLOBAL_ID);
      assertNotNull(cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null),
        key1.getKeyMetadata(), null));
      String alias2 = "cust2";
      byte[] cust_id2 = alias2.getBytes();
      ManagedKeyData key2 = testProvider.getManagedKey(
        new KeyIdentityPrefixBytesBacked(new Bytes(cust_id2), KEY_SPACE_GLOBAL_BYTES));
      assertNotNull(cache.getEntry(fullKeyIdentity(cust_id2, KEY_SPACE_GLOBAL_BYTES.get(), null),
        key2.getKeyMetadata(), null));
      assertNotNull(cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      // ACTIVE keys are automatically added to activeKeysCache when loaded.
      assertEquals(2, cache.getActiveCacheEntryCount());
    }

    @Test
    public void testActiveKeysCacheWithMultipleNamespaces() throws Exception {
      ManagedKeyData key1 = cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES);
      assertNotNull(key1);
      assertEquals(key1, cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      ManagedKeyData key2 =
        cache.getActiveEntry(CUST_ID_BYTES, new Bytes(Bytes.toBytes("namespace1")));
      assertNotNull(key2);
      assertEquals(key2,
        cache.getActiveEntry(CUST_ID_BYTES, new Bytes(Bytes.toBytes("namespace1"))));
      ManagedKeyData key3 =
        cache.getActiveEntry(CUST_ID_BYTES, new Bytes(Bytes.toBytes("namespace2")));
      assertNotNull(key3);
      assertEquals(key3,
        cache.getActiveEntry(CUST_ID_BYTES, new Bytes(Bytes.toBytes("namespace2"))));
      verify(testProvider, times(3)).getManagedKey(any());
      assertEquals(3, cache.getActiveCacheEntryCount());
    }

    @Test
    public void testEjectKey_ActiveKeysCacheOnly() throws Exception {
      // Load a key into the active keys cache
      ManagedKeyData key = cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES);
      assertNotNull(key);
      assertEquals(1, cache.getActiveCacheEntryCount());

      // Eject the key - should remove from active keys cache
      boolean ejected = cache
        .ejectKey(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), key.getPartialIdentity()));
      assertTrue("Key should be ejected when metadata matches", ejected);
      assertEquals(0, cache.getActiveCacheEntryCount());

      // Try to eject again - should return false since it's already gone from active keys cache
      boolean ejectedAgain = cache
        .ejectKey(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), key.getPartialIdentity()));
      assertFalse("Should return false when key is already ejected", ejectedAgain);
      assertEquals(0, cache.getActiveCacheEntryCount());
    }

    @Test
    public void testEjectKey_GenericCacheOnly() throws Exception {
      // Load a key into the generic cache
      ManagedKeyData key =
        cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null),
          testProvider.getManagedKey(CUST_KEY_SPACE_GLOBAL_ID).getKeyMetadata(), null);
      assertNotNull(key);
      assertEquals(1, cache.getGenericCacheEntryCount());

      // Eject the key - should remove from generic cache
      boolean ejected = cache
        .ejectKey(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), key.getPartialIdentity()));
      assertTrue("Key should be ejected when metadata matches", ejected);
      assertEquals(0, cache.getGenericCacheEntryCount());

      // Try to eject again - should return false since it's already gone from generic cache
      boolean ejectedAgain = cache
        .ejectKey(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), key.getPartialIdentity()));
      assertFalse("Should return false when key is already ejected", ejectedAgain);
      assertEquals(0, cache.getGenericCacheEntryCount());
    }

    @Test
    public void testEjectKey_Success() throws Exception {
      // Load a key into the active keys cache
      ManagedKeyData key = cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES);
      assertNotNull(key);
      String metadata = key.getKeyMetadata();
      assertEquals(1, cache.getActiveCacheEntryCount());

      // Also load into the generic cache
      ManagedKeyData keyFromGeneric = cache
        .getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null), metadata, null);
      assertNotNull(keyFromGeneric);
      assertEquals(1, cache.getGenericCacheEntryCount());

      // Eject the key with matching metadata - should remove from both caches
      boolean ejected = cache
        .ejectKey(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), key.getPartialIdentity()));
      assertTrue("Key should be ejected when metadata matches", ejected);
      assertEquals(0, cache.getActiveCacheEntryCount());
      assertEquals(0, cache.getGenericCacheEntryCount());

      // Try to eject again - should return false since it's already gone from active keys cache
      boolean ejectedAgain = cache
        .ejectKey(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), key.getPartialIdentity()));
      assertFalse("Should return false when key is already ejected", ejectedAgain);
      assertEquals(0, cache.getActiveCacheEntryCount());
      assertEquals(0, cache.getGenericCacheEntryCount());
    }

    @Test
    public void testEjectKey_MetadataMismatch() throws Exception {
      // Load a key into both caches
      ManagedKeyData key = cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES);
      assertNotNull(key);
      assertEquals(1, cache.getActiveCacheEntryCount());

      // Also load into the generic cache
      cache.getEntry(
        fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), key.getPartialIdentity()), null,
        null);
      assertEquals(1, cache.getGenericCacheEntryCount());

      // Try to eject with wrong metadata - should not eject from either cache
      String wrongMetadata = "wrong-metadata";
      boolean ejected = cache.ejectKey(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(),
        ManagedKeyIdentityUtils.constructMetadataHash(wrongMetadata)));
      assertFalse("Key should not be ejected when metadata doesn't match", ejected);
      assertEquals(1, cache.getActiveCacheEntryCount());
      assertEquals(1, cache.getGenericCacheEntryCount());

      // Verify the key is still in both caches
      assertEquals(key, cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      assertEquals(key.getKeyMetadata(),
        cache.getEntry(
          fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), key.getPartialIdentity()), null,
          null).getKeyMetadata());
    }

    @Test
    public void testEjectKey_KeyNotPresent() throws Exception {
      // Try to eject a key that doesn't exist in the cache
      String nonExistentMetadata = "non-existent-metadata";
      boolean ejected =
        cache.ejectKey(fullKeyIdentity(CUST_ID, Bytes.toBytes("non-existent-namespace"),
          ManagedKeyIdentityUtils.constructMetadataHash(nonExistentMetadata)));
      assertFalse("Should return false when key is not present", ejected);
      assertEquals(0, cache.getActiveCacheEntryCount());
    }

    @Test
    public void testEjectKey_MultipleKeys() throws Exception {
      // Load multiple keys into both caches
      ManagedKeyData key1 = cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES);
      ManagedKeyData key2 =
        cache.getActiveEntry(CUST_ID_BYTES, new Bytes(Bytes.toBytes("namespace1")));
      ManagedKeyData key3 =
        cache.getActiveEntry(CUST_ID_BYTES, new Bytes(Bytes.toBytes("namespace2")));
      assertNotNull(key1);
      assertNotNull(key2);
      assertNotNull(key3);
      assertEquals(3, cache.getActiveCacheEntryCount());

      // Also load all keys into the generic cache
      cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null),
        key1.getKeyMetadata(), null);
      cache.getEntry(fullKeyIdentity(CUST_ID, Bytes.toBytes("namespace1"), null),
        key2.getKeyMetadata(), null);
      cache.getEntry(fullKeyIdentity(CUST_ID, Bytes.toBytes("namespace2"), null),
        key3.getKeyMetadata(), null);
      assertEquals(3, cache.getGenericCacheEntryCount());

      // Eject only the middle key from both caches
      boolean ejected = cache
        .ejectKey(fullKeyIdentity(CUST_ID, Bytes.toBytes("namespace1"), key2.getPartialIdentity()));
      assertTrue("Key should be ejected from both caches", ejected);
      assertEquals(2, cache.getActiveCacheEntryCount());
      assertEquals(2, cache.getGenericCacheEntryCount());

      // Verify only key2 was ejected - key1 and key3 should still be there
      clearInvocations(testProvider);
      assertEquals(key1, cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      assertEquals(key3,
        cache.getActiveEntry(CUST_ID_BYTES, new Bytes(Bytes.toBytes("namespace2"))));
      // These getActiveEntry() calls should not trigger provider calls since keys are still cached
      verify(testProvider, never()).getManagedKey(any());

      // Verify generic cache still has key1 and key3
      assertEquals(key1.getKeyMetadata(),
        cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null),
          key1.getKeyMetadata(), null).getKeyMetadata());
      assertEquals(key3.getKeyMetadata(),
        cache.getEntry(fullKeyIdentity(CUST_ID, Bytes.toBytes("namespace2"), null),
          key3.getKeyMetadata(), null).getKeyMetadata());

      // Try to eject key2 again - should return false since it's already gone from both caches
      boolean ejectedAgain = cache
        .ejectKey(fullKeyIdentity(CUST_ID, Bytes.toBytes("namespace1"), key2.getPartialIdentity()));
      assertFalse("Should return false when key is already ejected", ejectedAgain);
      assertEquals(2, cache.getActiveCacheEntryCount());
      assertEquals(2, cache.getGenericCacheEntryCount());
    }

    @Test
    public void testEjectKey_DifferentCustodian() throws Exception {
      // Load a key for one custodian into both caches
      ManagedKeyData key = cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES);
      assertNotNull(key);
      String metadata = key.getKeyMetadata();
      assertEquals(1, cache.getActiveCacheEntryCount());

      // Also load into the generic cache
      cache.getEntry(
        fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), key.getPartialIdentity()), null,
        null);
      assertEquals(1, cache.getGenericCacheEntryCount());

      // Try to eject with a different custodian - should not eject from either cache
      byte[] differentCustodian = "different-cust".getBytes();
      boolean ejected = cache.ejectKey(fullKeyIdentity(differentCustodian,
        KEY_SPACE_GLOBAL_BYTES.get(), key.getPartialIdentity()));
      assertFalse("Should not eject key for different custodian", ejected);
      assertEquals(1, cache.getActiveCacheEntryCount());
      assertEquals(1, cache.getGenericCacheEntryCount());

      // Verify the original key is still in both caches
      assertEquals(key, cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      assertEquals(metadata,
        cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null), metadata, null)
          .getKeyMetadata());
    }

    @Test
    public void testEjectKey_AfterClearCache() throws Exception {
      // Load a key into both caches
      ManagedKeyData key = cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES);
      assertNotNull(key);
      String metadata = key.getKeyMetadata();
      assertEquals(1, cache.getActiveCacheEntryCount());

      // Also load into the generic cache
      cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null), metadata, null);
      assertEquals(1, cache.getGenericCacheEntryCount());

      // Clear both caches
      cache.clearCache();
      assertEquals(0, cache.getActiveCacheEntryCount());
      assertEquals(0, cache.getGenericCacheEntryCount());

      // Try to eject the key after both caches are cleared
      boolean ejected = cache
        .ejectKey(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), key.getPartialIdentity()));
      assertFalse("Should return false when both caches are empty", ejected);
      assertEquals(0, cache.getActiveCacheEntryCount());
      assertEquals(0, cache.getGenericCacheEntryCount());
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
      assertNull(cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null),
        "test-metadata", null));
      verify(mockL2).getKey(any(ManagedKeyIdentity.class));
      clearInvocations(mockL2);
      assertNull(cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null),
        "test-metadata", null));
      verify(mockL2, never()).getKey(any(ManagedKeyIdentity.class));
    }

    @Test
    public void testGenericCacheRetrievalFromL2Cache() throws Exception {
      ManagedKeyData key = testProvider.getManagedKey(CUST_KEY_SPACE_GLOBAL_ID);
      ManagedKeyIdentity l2LookupIdentity = ManagedKeyIdentityUtils
        .fullKeyIdentityFromMetadata(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES, key.getKeyMetadata());
      when(mockL2.getKey(l2LookupIdentity)).thenReturn(key);
      assertEquals(key, cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null),
        key.getKeyMetadata(), null));
      verify(mockL2).getKey(any(ManagedKeyIdentity.class));
    }

    @Test
    public void testActiveKeysCacheNonExistentKeyInL2Cache() throws Exception {
      assertNull(cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      verify(mockL2).getKeyManagementStateMarker(any());
      clearInvocations(mockL2);
      assertNull(cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      verify(mockL2, never()).getKeyManagementStateMarker(any());
    }

    @Test
    public void testActiveKeysCacheRetrievalFromL2Cache() throws Exception {
      ManagedKeyData key = testProvider.getManagedKey(CUST_KEY_SPACE_GLOBAL_ID);
      when(mockL2.getKeyManagementStateMarker(
        new KeyIdentityPrefixBytesBacked(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES))).thenReturn(key);
      assertEquals(key, cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      verify(mockL2).getKeyManagementStateMarker(any(ManagedKeyIdentity.class));
      clearInvocations(mockL2);
      assertEquals(key, cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      verify(mockL2, never()).getKeyManagementStateMarker(any(ManagedKeyIdentity.class));
    }

    @Test
    public void testGenericCacheWithKeymetaAccessorException() throws Exception {
      ManagedKeyIdentity keyIdentity = ManagedKeyIdentityUtils
        .fullKeyIdentityFromMetadata(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES, "test-metadata");
      when(mockL2.getKey(keyIdentity)).thenThrow(new IOException("Test exception"));
      assertNull(cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null),
        "test-metadata", null));
      verify(mockL2).getKey(keyIdentity);
      clearInvocations(mockL2);
      assertNull(cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null),
        "test-metadata", null));
      verify(mockL2, never()).getKey(keyIdentity);
    }

    @Test
    public void testGetActiveEntryWithKeymetaAccessorException() throws Exception {
      when(mockL2.getKeyManagementStateMarker(any())).thenThrow(new IOException("Test exception"));
      assertNull(cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      verify(mockL2).getKeyManagementStateMarker(any());
      clearInvocations(mockL2);
      assertNull(cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      verify(mockL2, never()).getKeyManagementStateMarker(any());
    }

    @Test
    public void testActiveKeysCacheUsesKeymetaAccessorWhenGenericCacheEmpty() throws Exception {
      // Ensure generic cache is empty
      cache.clearCache();

      // Mock L2: getActiveEntry uses prefix identity (clone of custodian/namespace), not full key
      // identity
      ManagedKeyData key = testProvider.getManagedKey(CUST_KEY_SPACE_GLOBAL_ID);
      when(mockL2.getKeyManagementStateMarker(any(ManagedKeyIdentity.class))).thenReturn(key);

      // Get the active entry - it should call keymetaAccessor since generic cache is empty
      assertEquals(key, cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      verify(mockL2).getKeyManagementStateMarker(any());
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
      ManagedKeyData key = testProvider.getManagedKey(CUST_KEY_SPACE_GLOBAL_ID);
      doReturn(key).when(testProvider).unwrapKey(any(ManagedKeyIdentity.class), any(String.class),
        any());
      assertEquals(key, cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null),
        key.getKeyMetadata(), null));
      verify(mockL2).getKey(any(ManagedKeyIdentity.class));
      verify(mockL2).addKey(any(ManagedKeyData.class));
    }

    @Test
    public void testAddKeyFailure() throws Exception {
      ManagedKeyData key = testProvider.getManagedKey(CUST_KEY_SPACE_GLOBAL_ID);
      doReturn(key).when(testProvider).unwrapKey(any(ManagedKeyIdentity.class), any(String.class),
        any());
      doThrow(new IOException("Test exception")).when(mockL2).addKey(any(ManagedKeyData.class));
      assertNull(cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null),
        key.getKeyMetadata(), null));
      verify(mockL2).addKey(any(ManagedKeyData.class));
    }

    @Test
    public void testActiveKeysCacheDynamicLookupWithUnexpectedException() throws Exception {
      doThrow(new RuntimeException("Test exception")).when(testProvider).getManagedKey(any());
      assertNull(cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      verify(testProvider).getManagedKey(any());
      clearInvocations(testProvider);
      // A 2nd invocation should not result in a call to the provider.
      assertNull(cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      verify(testProvider, never()).getManagedKey(any());
    }

    @Test
    public void testActiveKeysCacheRetrivalFromProviderWhenKeyNotFoundInL2Cache() throws Exception {
      ManagedKeyData key = testProvider.getManagedKey(CUST_KEY_SPACE_GLOBAL_ID);
      doReturn(key).when(testProvider).getManagedKey(any());
      assertEquals(key, cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      verify(mockL2).getKeyManagementStateMarker(any());
    }

    @Test
    public void testGenericCacheUsesActiveKeysCacheFirst() throws Exception {
      // First populate the active keys cache with an active key
      ManagedKeyData key1 = cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES);
      verify(testProvider).getManagedKey(any());
      clearInvocations(testProvider);

      // Now get the generic cache entry - it should use the active keys cache first, not call
      // keymetaAccessor
      assertEquals(key1, cache.getEntry(
        fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null), key1.getKeyMetadata(), null));
      verify(testProvider, never()).getManagedKey(any());

      // Lookup a diffrent key.
      ManagedKeyData key2 =
        cache.getActiveEntry(CUST_ID_BYTES, new Bytes(Bytes.toBytes("namespace1")));
      assertNotEquals(key1, key2);
      verify(testProvider).getManagedKey(any());
      clearInvocations(testProvider);

      // Now get the generic cache entry - it should use the active keys cache first, not call
      // keymetaAccessor
      assertEquals(key2, cache.getEntry(fullKeyIdentity(CUST_ID, Bytes.toBytes("namespace1"), null),
        key2.getKeyMetadata(), null));
      verify(testProvider, never()).getManagedKey(any());
    }

    @Test
    public void testGetOlderEntryFromGenericCache() throws Exception {
      // Get one version of the key in to ActiveKeysCache
      ManagedKeyData key1 = cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES);
      assertNotNull(key1);
      clearInvocations(testProvider);

      // Now try to lookup another version of the key, it should lookup and discard the active key.
      ManagedKeyData key2 = testProvider.getManagedKey(CUST_KEY_SPACE_GLOBAL_ID);
      assertEquals(key2, cache.getEntry(
        fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null), key2.getKeyMetadata(), null));
      verify(testProvider).unwrapKey(any(ManagedKeyIdentity.class), any(String.class), any());
    }

    @Test
    public void testThatActiveKeysCache_PopulatedByGenericCache() throws Exception {
      // First populate the generic cache with an active key
      ManagedKeyData key = testProvider.getManagedKey(CUST_KEY_SPACE_GLOBAL_ID);
      assertEquals(key, cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null),
        key.getKeyMetadata(), null));
      verify(testProvider).unwrapKey(any(ManagedKeyIdentity.class), any(String.class), any());

      // Clear invocations to reset the mock state
      clearInvocations(testProvider);

      // Now get the active entry - it should already be there due to the generic cache first
      assertEquals(key, cache.getActiveEntry(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES));
      verify(testProvider, never()).unwrapKey(any(ManagedKeyIdentity.class), any(String.class),
        any());
    }

    @Test
    public void testGetEntry_BothPartialIdentityAndKeyMetadataNull_Throws() {
      assertThrows(IllegalArgumentException.class, () -> {
        cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null), null, null);
      });
    }

    @Test
    public void testGetEntry_BothPartialIdentityAndKeyMetadataNonNull_ExistingEntry()
      throws Exception {
      ManagedKeyData key = testProvider.getManagedKey(CUST_KEY_SPACE_GLOBAL_ID);
      byte[] partialIdentity = key.getPartialIdentity();
      String keyMetadata = key.getKeyMetadata();
      cache.getEntry(fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), null), keyMetadata,
        null);
      assertEquals(1, cache.getGenericCacheEntryCount());
      ManagedKeyData found = cache.getEntry(
        fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), partialIdentity), keyMetadata, null);
      assertEquals(key, found);
    }

    @Test
    public void testGetEntry_BothPartialIdentityAndKeyMetadataNonNull_NoExistingEntry()
      throws Exception {
      // Use explicit keyMetadata so retrieveKey validation sees matching metadata.
      String keyMetadata = "cust1:cust1:*:0";
      Key key = MockManagedKeyProvider.generateSecretKey();
      ManagedKeyData keyFromProvider =
        new ManagedKeyData(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), key, ACTIVE, keyMetadata);
      byte[] partialIdentity = ManagedKeyIdentityUtils.constructMetadataHash(keyMetadata);
      cache.clearCache();
      doReturn(keyFromProvider).when(testProvider).unwrapKey(any(ManagedKeyIdentity.class),
        any(String.class), any());
      ManagedKeyData found = cache.getEntry(
        fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), partialIdentity), keyMetadata, null);
      assertNotNull(found);
      assertEquals(keyFromProvider, found);
    }

    @Test
    public void testGetEntry_WithPartialIdentityOnly_ReturnsNullWithoutProviderLookup()
      throws Exception {
      byte[] partialIdentity = ManagedKeyIdentityUtils.constructMetadataHash("missing-metadata");

      ManagedKeyData found = cache.getEntry(
        fullKeyIdentity(CUST_ID, KEY_SPACE_GLOBAL_BYTES.get(), partialIdentity), null, null);

      assertNull(found);
      verify(mockL2).getKey(any(ManagedKeyIdentity.class));
      verify(testProvider, never()).unwrapKey(any(ManagedKeyIdentity.class), any(String.class),
        any());
    }
  }

  protected void assertGenericCacheEntries(ManagedKeyData... keys) throws Exception {
    for (ManagedKeyData key : keys) {
      assertEquals(key,
        cache.getEntry(fullKeyIdentity(key.getKeyCustodian(), key.getKeyNamespaceBytes(), null),
          key.getKeyMetadata(), null));
    }
    assertEquals(keys.length, cache.getGenericCacheEntryCount());
    int activeKeysCount =
      Arrays.stream(keys).filter(key -> key.getKeyState() == ManagedKeyState.ACTIVE)
        .map(key -> new KeyIdentityPrefixBytesBacked(new Bytes(key.getKeyCustodian()),
          new Bytes(key.getKeyNamespaceBytes())))
        .collect(Collectors.toSet()).size();
    assertEquals(activeKeysCount, cache.getActiveCacheEntryCount());
  }
}
