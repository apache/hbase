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
package org.apache.hadoop.hbase.io.crypto.tls;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.security.Security;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.net.ssl.SSLContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.exceptions.X509Exception;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * This file has been copied from the Apache ZooKeeper project.
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/master/zookeeper-server/src/test/java/org/apache/zookeeper/common/X509UtilTest.java">Base
 *      revision</a>
 */
@RunWith(Parameterized.class)
@Category({ MiscTests.class, SmallTests.class })
public class TestX509Util extends BaseX509ParameterizedTestCase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestX509Util.class);

  @Parameterized.Parameter()
  public X509KeyType caKeyType;

  @Parameterized.Parameter(value = 1)
  public X509KeyType certKeyType;

  @Parameterized.Parameter(value = 2)
  public String keyPassword;

  @Parameterized.Parameter(value = 3)
  public Integer paramIndex;

  @Parameterized.Parameters(
      name = "{index}: caKeyType={0}, certKeyType={1}, keyPassword={2}, paramIndex={3}")
  public static Collection<Object[]> data() {
    List<Object[]> params = new ArrayList<>();
    int paramIndex = 0;
    for (X509KeyType caKeyType : X509KeyType.values()) {
      for (X509KeyType certKeyType : X509KeyType.values()) {
        for (String keyPassword : new String[] { KEY_EMPTY_PASSWORD, KEY_NON_EMPTY_PASSWORD }) {
          params.add(new Object[] { caKeyType, certKeyType, keyPassword, paramIndex++ });
        }
      }
    }
    return params;
  }

  private X509Util x509Util;
  private Configuration hbaseConf;

  @Override
  public void init(X509KeyType caKeyType, X509KeyType certKeyType, String keyPassword,
    Integer paramIndex) throws Exception {
    super.init(caKeyType, certKeyType, keyPassword, paramIndex);
    x509TestContext.setSystemProperties(KeyStoreFileType.JKS, KeyStoreFileType.JKS);
    x509Util = new X509Util(x509TestContext.getHbaseConf());
    hbaseConf = x509TestContext.getHbaseConf();
  }

  @After
  public void cleanUp() {
    x509TestContext.clearSystemProperties();
    x509TestContext.getHbaseConf().unset(X509Util.TLS_CONFIG_OCSP);
    x509TestContext.getHbaseConf().unset(X509Util.TLS_CONFIG_CLR);
    x509TestContext.getHbaseConf().unset(X509Util.TLS_CONFIG_PROTOCOL);
    System.clearProperty("com.sun.net.ssl.checkRevocation");
    System.clearProperty("com.sun.security.enableCRLDP");
    Security.setProperty("ocsp.enable", Boolean.FALSE.toString());
    Security.setProperty("com.sun.security.enableCRLDP", Boolean.FALSE.toString());
  }

  @Test
  public void testCreateSSLContextWithoutCustomProtocol() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    SSLContext sslContext = x509Util.getDefaultSSLContextAndOptions().getSSLContext();
    assertEquals(X509Util.DEFAULT_PROTOCOL, sslContext.getProtocol());
  }

  @Test
  public void testCreateSSLContextWithCustomProtocol() throws Exception {
    final String protocol = "TLSv1.1";
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    hbaseConf.set(X509Util.TLS_CONFIG_PROTOCOL, protocol);
    SSLContext sslContext = x509Util.getDefaultSSLContextAndOptions().getSSLContext();
    assertEquals(protocol, sslContext.getProtocol());
  }

  @Test
  public void testCreateSSLContextWithoutKeyStoreLocation() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    hbaseConf.unset(X509Util.TLS_CONFIG_KEYSTORE_LOCATION);
    x509Util.getDefaultSSLContextAndOptions();
  }

  @Test(expected = X509Exception.SSLContextException.class)
  public void testCreateSSLContextWithoutKeyStorePassword() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    if (!x509TestContext.isKeyStoreEncrypted()) {
      throw new X509Exception.SSLContextException("");
    }
    hbaseConf.unset(X509Util.TLS_CONFIG_KEYSTORE_PASSWORD);
    x509Util.getDefaultSSLContextAndOptions();
  }

  // It would be great to test the value of PKIXBuilderParameters#setRevocationEnabled,
  // but it does not appear to be possible
  @Test
  public void testCRLEnabled() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    hbaseConf.setBoolean(X509Util.TLS_CONFIG_CLR, true);
    x509Util.getDefaultSSLContextAndOptions();
    assertTrue(Boolean.valueOf(System.getProperty("com.sun.net.ssl.checkRevocation")));
    assertTrue(Boolean.valueOf(System.getProperty("com.sun.security.enableCRLDP")));
    assertFalse(Boolean.valueOf(Security.getProperty("ocsp.enable")));
  }

  @Test
  public void testCRLDisabled() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    x509Util.getDefaultSSLContextAndOptions();
    assertFalse(Boolean.valueOf(System.getProperty("com.sun.net.ssl.checkRevocation")));
    assertFalse(Boolean.valueOf(System.getProperty("com.sun.security.enableCRLDP")));
    assertFalse(Boolean.valueOf(Security.getProperty("ocsp.enable")));
  }

  @Test
  public void testOCSPEnabled() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    hbaseConf.setBoolean(X509Util.TLS_CONFIG_OCSP, true);
    x509Util.getDefaultSSLContextAndOptions();
    assertTrue(Boolean.valueOf(System.getProperty("com.sun.net.ssl.checkRevocation")));
    assertFalse(Boolean.valueOf(System.getProperty("com.sun.security.enableCRLDP")));
    assertTrue(Boolean.valueOf(Security.getProperty("ocsp.enable")));
  }

  @Test
  public void testLoadJKSKeyStore() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    // Make sure we can instantiate a key manager from the JKS file on disk
    X509Util.createKeyManager(
      x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
      x509TestContext.getKeyStorePassword(), KeyStoreFileType.JKS.getPropertyValue());
  }

  @Test
  public void testLoadJKSKeyStoreNullPassword() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    if (!x509TestContext.getKeyStorePassword().isEmpty()) {
      return;
    }
    // Make sure that empty password and null password are treated the same
    X509Util.createKeyManager(
      x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath(), null,
      KeyStoreFileType.JKS.getPropertyValue());
  }

  @Test
  public void testLoadJKSKeyStoreFileTypeDefaultToJks() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    // Make sure we can instantiate a key manager from the JKS file on disk
    X509Util.createKeyManager(
      x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
      x509TestContext.getKeyStorePassword(),
      null /* null StoreFileType means 'autodetect from file extension' */);
  }

  @Test
  public void testLoadJKSKeyStoreWithWrongPassword() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    assertThrows(X509Exception.KeyManagerException.class, () -> {
      // Attempting to load with the wrong key password should fail
      X509Util.createKeyManager(
        x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath(), "wrong password",
        KeyStoreFileType.JKS.getPropertyValue());
    });
  }

  @Test
  public void testLoadJKSTrustStore() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    // Make sure we can instantiate a trust manager from the JKS file on disk
    X509Util.createTrustManager(
      x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
      x509TestContext.getTrustStorePassword(), KeyStoreFileType.JKS.getPropertyValue(), true, true);
  }

  @Test
  public void testLoadJKSTrustStoreNullPassword() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    if (!x509TestContext.getTrustStorePassword().isEmpty()) {
      return;
    }
    // Make sure that empty password and null password are treated the same
    X509Util.createTrustManager(
      x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath(), null,
      KeyStoreFileType.JKS.getPropertyValue(), false, false);
  }

  @Test
  public void testLoadJKSTrustStoreFileTypeDefaultToJks() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    // Make sure we can instantiate a trust manager from the JKS file on disk
    X509Util.createTrustManager(
      x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
      x509TestContext.getTrustStorePassword(), null, // null StoreFileType means 'autodetect from
                                                     // file extension'
      true, true);
  }

  @Test
  public void testLoadJKSTrustStoreWithWrongPassword() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    assertThrows(X509Exception.TrustManagerException.class, () -> {
      // Attempting to load with the wrong key password should fail
      X509Util.createTrustManager(
        x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath(), "wrong password",
        KeyStoreFileType.JKS.getPropertyValue(), true, true);
    });
  }

  @Test
  public void testLoadPKCS12KeyStore() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    // Make sure we can instantiate a key manager from the PKCS12 file on disk
    X509Util.createKeyManager(
      x509TestContext.getKeyStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
      x509TestContext.getKeyStorePassword(), KeyStoreFileType.PKCS12.getPropertyValue());
  }

  @Test
  public void testLoadPKCS12KeyStoreNullPassword() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    if (!x509TestContext.getKeyStorePassword().isEmpty()) {
      return;
    }
    // Make sure that empty password and null password are treated the same
    X509Util.createKeyManager(
      x509TestContext.getKeyStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(), null,
      KeyStoreFileType.PKCS12.getPropertyValue());
  }

  @Test
  public void testLoadPKCS12KeyStoreWithWrongPassword() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    assertThrows(X509Exception.KeyManagerException.class, () -> {
      // Attempting to load with the wrong key password should fail
      X509Util.createKeyManager(
        x509TestContext.getKeyStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
        "wrong password", KeyStoreFileType.PKCS12.getPropertyValue());
    });
  }

  @Test
  public void testLoadPKCS12TrustStore() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    // Make sure we can instantiate a trust manager from the PKCS12 file on disk
    X509Util.createTrustManager(
      x509TestContext.getTrustStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
      x509TestContext.getTrustStorePassword(), KeyStoreFileType.PKCS12.getPropertyValue(), true,
      true);
  }

  @Test
  public void testLoadPKCS12TrustStoreNullPassword() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    if (!x509TestContext.getTrustStorePassword().isEmpty()) {
      return;
    }
    // Make sure that empty password and null password are treated the same
    X509Util.createTrustManager(
      x509TestContext.getTrustStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(), null,
      KeyStoreFileType.PKCS12.getPropertyValue(), false, false);
  }

  @Test
  public void testLoadPKCS12TrustStoreWithWrongPassword() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    assertThrows(X509Exception.TrustManagerException.class, () -> {
      // Attempting to load with the wrong key password should fail
      X509Util.createTrustManager(
        x509TestContext.getTrustStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
        "wrong password", KeyStoreFileType.PKCS12.getPropertyValue(), true, true);
    });
  }

  @Test
  public void testGetDefaultCipherSuitesJava8() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    String[] cipherSuites = X509Util.getDefaultCipherSuitesForJavaVersion("1.8");
    // Java 8 default should have the CBC suites first
    assertTrue(cipherSuites[0].contains("CBC"));
  }

  @Test
  public void testGetDefaultCipherSuitesJava9() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    String[] cipherSuites = X509Util.getDefaultCipherSuitesForJavaVersion("9");
    // Java 9+ default should have the GCM suites first
    assertTrue(cipherSuites[0].contains("GCM"));
  }

  @Test
  public void testGetDefaultCipherSuitesJava10() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    String[] cipherSuites = X509Util.getDefaultCipherSuitesForJavaVersion("10");
    // Java 9+ default should have the GCM suites first
    assertTrue(cipherSuites[0].contains("GCM"));
  }

  @Test
  public void testGetDefaultCipherSuitesJava11() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    String[] cipherSuites = X509Util.getDefaultCipherSuitesForJavaVersion("11");
    // Java 9+ default should have the GCM suites first
    assertTrue(cipherSuites[0].contains("GCM"));
  }

  @Test
  public void testGetDefaultCipherSuitesUnknownVersion() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    String[] cipherSuites = X509Util.getDefaultCipherSuitesForJavaVersion("notaversion");
    // If version can't be parsed, use the more conservative Java 8 default
    assertTrue(cipherSuites[0].contains("CBC"));
  }

  @Test
  public void testGetDefaultCipherSuitesNullVersion() throws Exception {
    init(caKeyType, certKeyType, keyPassword, paramIndex);
    assertThrows(NullPointerException.class, () -> {
      X509Util.getDefaultCipherSuitesForJavaVersion(null);
    });
  }
}
