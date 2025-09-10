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
package org.apache.hadoop.hbase.io.crypto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.apache.hadoop.hbase.io.crypto.KeymetaTestUtils.ALIAS;
import static org.apache.hadoop.hbase.io.crypto.KeymetaTestUtils.PASSWORD;

import java.security.Key;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category({ MiscTests.class, SmallTests.class })
@RunWith(Parameterized.class)
public class TestKeyStoreKeyProvider {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestKeyStoreKeyProvider.class);

  static final HBaseCommonTestingUtil TEST_UTIL = new HBaseCommonTestingUtil();

  static byte[] KEY;

  protected KeyProvider provider;

  @Parameterized.Parameter(0)
  public boolean withPasswordOnAlias;
  @Parameterized.Parameter(1)
  public boolean withPasswordFile;

  @Parameterized.Parameters(name = "withPasswordOnAlias={0} withPasswordFile={1}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
      { Boolean.TRUE, Boolean.TRUE },
      { Boolean.TRUE, Boolean.FALSE },
      { Boolean.FALSE, Boolean.TRUE },
      { Boolean.FALSE, Boolean.FALSE },
    });
  }

  @Before
  public void setUp() throws Exception {
    KEY = MessageDigest.getInstance("SHA-256").digest(Bytes.toBytes(ALIAS));
    String providerParams = KeymetaTestUtils.setupTestKeyStore(TEST_UTIL, withPasswordOnAlias,
      withPasswordFile, store -> {
        Properties p = new Properties();
        try {
          store.setEntry(ALIAS, new KeyStore.SecretKeyEntry(new SecretKeySpec(KEY, "AES")),
            new KeyStore.PasswordProtection(withPasswordOnAlias ? PASSWORD.toCharArray()
            : new char[0]));
          addCustomEntries(store, p);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return p;
      });
    provider = createProvider();
    provider.init(providerParams);
  }

  protected KeyProvider createProvider() {
    return new KeyStoreKeyProvider();
  }

  protected void addCustomEntries(KeyStore store, Properties passwdProps) throws Exception {
    passwdProps.setProperty(ALIAS, PASSWORD);
  }

  @Test
  public void testKeyStoreKeyProvider() throws Exception {
    Key key = provider.getKey(ALIAS);
    assertNotNull(key);
    byte[] keyBytes = key.getEncoded();
    assertEquals(keyBytes.length, KEY.length);
    for (int i = 0; i < KEY.length; i++) {
      assertEquals(keyBytes[i], KEY[i]);
    }
  }
}
