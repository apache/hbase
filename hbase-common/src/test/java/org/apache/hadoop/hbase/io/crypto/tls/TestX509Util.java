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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.exceptions.KeyManagerException;
import org.apache.hadoop.hbase.exceptions.SSLContextException;
import org.apache.hadoop.hbase.exceptions.TrustManagerException;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hbase.thirdparty.io.netty.buffer.ByteBufAllocator;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.SslContext;

/**
 * This file has been copied from the Apache ZooKeeper project.
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/master/zookeeper-server/src/test/java/org/apache/zookeeper/common/X509UtilTest.java">Base
 *      revision</a>
 */
@RunWith(Parameterized.class)
@Category({ MiscTests.class, SmallTests.class })
public class TestX509Util {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestX509Util.class);

  private static final HBaseCommonTestingUtility UTIL = new HBaseCommonTestingUtility();
  private static final char[] EMPTY_CHAR_ARRAY = new char[0];

  private static X509TestContextProvider PROVIDER;

  @Parameterized.Parameter()
  public X509KeyType caKeyType;

  @Parameterized.Parameter(value = 1)
  public X509KeyType certKeyType;

  @Parameterized.Parameter(value = 2)
  public char[] keyPassword;

  @Parameterized.Parameter(value = 3)
  public Integer paramIndex;

  private X509TestContext x509TestContext;

  private Configuration conf;

  @Parameterized.Parameters(
      name = "{index}: caKeyType={0}, certKeyType={1}, keyPassword={2}, paramIndex={3}")
  public static Collection<Object[]> data() {
    List<Object[]> params = new ArrayList<>();
    int paramIndex = 0;
    for (X509KeyType caKeyType : X509KeyType.values()) {
      for (X509KeyType certKeyType : X509KeyType.values()) {
        for (char[] keyPassword : new char[][] { "".toCharArray(), "pa$$w0rd".toCharArray() }) {
          params.add(new Object[] { caKeyType, certKeyType, keyPassword, paramIndex++ });
        }
      }
    }
    return params;
  }

  @BeforeClass
  public static void setUpBeforeClass() throws IOException {
    Security.addProvider(new BouncyCastleProvider());
    File dir = new File(UTIL.getDataTestDir(TestX509Util.class.getSimpleName()).toString())
      .getCanonicalFile();
    FileUtils.forceMkdir(dir);
    PROVIDER = new X509TestContextProvider(UTIL.getConfiguration(), dir);
  }

  @AfterClass
  public static void tearDownAfterClass() {
    Security.removeProvider(BouncyCastleProvider.PROVIDER_NAME);
    UTIL.cleanupTestDir();
  }

  @Before
  public void setUp() throws IOException {
    x509TestContext = PROVIDER.get(caKeyType, certKeyType, keyPassword);
    x509TestContext.setConfigurations(KeyStoreFileType.JKS, KeyStoreFileType.JKS);
    conf = new Configuration(UTIL.getConfiguration());
  }

  @After
  public void cleanUp() {
    x509TestContext.clearConfigurations();
    x509TestContext.getConf().unset(X509Util.TLS_CONFIG_OCSP);
    x509TestContext.getConf().unset(X509Util.TLS_CONFIG_CLR);
    x509TestContext.getConf().unset(X509Util.TLS_CONFIG_PROTOCOL);
    System.clearProperty("com.sun.net.ssl.checkRevocation");
    System.clearProperty("com.sun.security.enableCRLDP");
    Security.setProperty("ocsp.enable", Boolean.FALSE.toString());
    Security.setProperty("com.sun.security.enableCRLDP", Boolean.FALSE.toString());
  }

  @Test
  public void testCreateSSLContextWithoutCustomProtocol() throws Exception {
    SslContext sslContext = X509Util.createSslContextForClient(conf);
    ByteBufAllocator byteBufAllocatorMock = mock(ByteBufAllocator.class);
    assertArrayEquals(new String[] { X509Util.DEFAULT_PROTOCOL },
      sslContext.newEngine(byteBufAllocatorMock).getEnabledProtocols());
  }

  @Test
  public void testCreateSSLContextWithCustomProtocol() throws Exception {
    final String protocol = "TLSv1.1";
    conf.set(X509Util.TLS_CONFIG_PROTOCOL, protocol);
    ByteBufAllocator byteBufAllocatorMock = mock(ByteBufAllocator.class);
    SslContext sslContext = X509Util.createSslContextForServer(conf);
    assertEquals(Collections.singletonList(protocol),
      Arrays.asList(sslContext.newEngine(byteBufAllocatorMock).getEnabledProtocols()));
  }

  @Test(expected = SSLContextException.class)
  public void testCreateSSLContextWithoutKeyStoreLocationServer() throws Exception {
    conf.unset(X509Util.TLS_CONFIG_KEYSTORE_LOCATION);
    X509Util.createSslContextForServer(conf);
  }

  @Test
  public void testCreateSSLContextWithoutKeyStoreLocationClient() throws Exception {
    conf.unset(X509Util.TLS_CONFIG_KEYSTORE_LOCATION);
    X509Util.createSslContextForClient(conf);
  }

  @Test
  public void testCreateSSLContextWithoutTrustStoreLocationClient() throws Exception {
    conf.unset(X509Util.TLS_CONFIG_TRUSTSTORE_LOCATION);
    X509Util.createSslContextForClient(conf);
  }

  @Test
  public void testCreateSSLContextWithoutTrustStoreLocationServer() throws Exception {
    conf.unset(X509Util.TLS_CONFIG_TRUSTSTORE_LOCATION);
    X509Util.createSslContextForServer(conf);
  }

  // It would be great to test the value of PKIXBuilderParameters#setRevocationEnabled,
  // but it does not appear to be possible
  @Test
  public void testCRLEnabled() throws Exception {
    conf.setBoolean(X509Util.TLS_CONFIG_CLR, true);
    X509Util.createSslContextForServer(conf);
    assertTrue(Boolean.valueOf(System.getProperty("com.sun.net.ssl.checkRevocation")));
    assertTrue(Boolean.valueOf(System.getProperty("com.sun.security.enableCRLDP")));
    assertFalse(Boolean.valueOf(Security.getProperty("ocsp.enable")));
  }

  @Test
  public void testCRLDisabled() throws Exception {
    X509Util.createSslContextForServer(conf);
    assertFalse(Boolean.valueOf(System.getProperty("com.sun.net.ssl.checkRevocation")));
    assertFalse(Boolean.valueOf(System.getProperty("com.sun.security.enableCRLDP")));
    assertFalse(Boolean.valueOf(Security.getProperty("ocsp.enable")));
  }

  @Test
  public void testLoadJKSKeyStore() throws Exception {
    // Make sure we can instantiate a key manager from the JKS file on disk
    X509Util.createKeyManager(
      x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
      x509TestContext.getKeyStorePassword(), KeyStoreFileType.JKS.getPropertyValue());
  }

  @Test
  public void testLoadJKSKeyStoreNullPassword() throws Exception {
    assumeThat(x509TestContext.getKeyStorePassword(), equalTo(EMPTY_CHAR_ARRAY));
    // Make sure that empty password and null password are treated the same
    X509Util.createKeyManager(
      x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath(), null,
      KeyStoreFileType.JKS.getPropertyValue());
  }

  @Test
  public void testLoadJKSKeyStoreFileTypeDefaultToJks() throws Exception {
    // Make sure we can instantiate a key manager from the JKS file on disk
    X509Util.createKeyManager(
      x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
      x509TestContext.getKeyStorePassword(),
      null /* null StoreFileType means 'autodetect from file extension' */);
  }

  @Test
  public void testLoadJKSKeyStoreWithWrongPassword() {
    assertThrows(KeyManagerException.class, () -> {
      // Attempting to load with the wrong key password should fail
      X509Util.createKeyManager(
        x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
        "wrong password".toCharArray(), KeyStoreFileType.JKS.getPropertyValue());
    });
  }

  @Test
  public void testLoadJKSTrustStore() throws Exception {
    // Make sure we can instantiate a trust manager from the JKS file on disk
    X509Util.createTrustManager(
      x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
      x509TestContext.getTrustStorePassword(), KeyStoreFileType.JKS.getPropertyValue(), true, true);
  }

  @Test
  public void testLoadJKSTrustStoreNullPassword() throws Exception {
    assumeThat(x509TestContext.getTrustStorePassword(), equalTo(EMPTY_CHAR_ARRAY));
    // Make sure that empty password and null password are treated the same
    X509Util.createTrustManager(
      x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath(), null,
      KeyStoreFileType.JKS.getPropertyValue(), false, false);
  }

  @Test
  public void testLoadJKSTrustStoreFileTypeDefaultToJks() throws Exception {
    // Make sure we can instantiate a trust manager from the JKS file on disk
    X509Util.createTrustManager(
      x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
      // null StoreFileType means 'autodetect from file extension'
      x509TestContext.getTrustStorePassword(), null, true, true);
  }

  @Test
  public void testLoadJKSTrustStoreWithWrongPassword() throws Exception {
    assertThrows(TrustManagerException.class, () -> {
      // Attempting to load with the wrong key password should fail
      X509Util.createTrustManager(
        x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
        "wrong password".toCharArray(), KeyStoreFileType.JKS.getPropertyValue(), true, true);
    });
  }

  @Test
  public void testLoadPKCS12KeyStore() throws Exception {
    // Make sure we can instantiate a key manager from the PKCS12 file on disk
    X509Util.createKeyManager(
      x509TestContext.getKeyStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
      x509TestContext.getKeyStorePassword(), KeyStoreFileType.PKCS12.getPropertyValue());
  }

  @Test
  public void testLoadPKCS12KeyStoreNullPassword() throws Exception {
    assumeThat(x509TestContext.getKeyStorePassword(), equalTo(EMPTY_CHAR_ARRAY));
    // Make sure that empty password and null password are treated the same
    X509Util.createKeyManager(
      x509TestContext.getKeyStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(), null,
      KeyStoreFileType.PKCS12.getPropertyValue());
  }

  @Test
  public void testLoadPKCS12KeyStoreWithWrongPassword() throws Exception {
    assertThrows(KeyManagerException.class, () -> {
      // Attempting to load with the wrong key password should fail
      X509Util.createKeyManager(
        x509TestContext.getKeyStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
        "wrong password".toCharArray(), KeyStoreFileType.PKCS12.getPropertyValue());
    });
  }

  @Test
  public void testLoadPKCS12TrustStore() throws Exception {
    // Make sure we can instantiate a trust manager from the PKCS12 file on disk
    X509Util.createTrustManager(
      x509TestContext.getTrustStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
      x509TestContext.getTrustStorePassword(), KeyStoreFileType.PKCS12.getPropertyValue(), true,
      true);
  }

  @Test
  public void testLoadPKCS12TrustStoreNullPassword() throws Exception {
    assumeThat(x509TestContext.getTrustStorePassword(), equalTo(EMPTY_CHAR_ARRAY));
    // Make sure that empty password and null password are treated the same
    X509Util.createTrustManager(
      x509TestContext.getTrustStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(), null,
      KeyStoreFileType.PKCS12.getPropertyValue(), false, false);
  }

  @Test
  public void testLoadPKCS12TrustStoreWithWrongPassword() throws Exception {
    assertThrows(TrustManagerException.class, () -> {
      // Attempting to load with the wrong key password should fail
      X509Util.createTrustManager(
        x509TestContext.getTrustStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
        "wrong password".toCharArray(), KeyStoreFileType.PKCS12.getPropertyValue(), true, true);
    });
  }

  @Test
  public void testGetDefaultCipherSuitesJava8() throws Exception {
    String[] cipherSuites = X509Util.getDefaultCipherSuitesForJavaVersion("1.8");
    // Java 8 default should have the CBC suites first
    assertThat(cipherSuites[0], containsString("CBC"));
  }

  @Test
  public void testGetDefaultCipherSuitesJava9() throws Exception {
    String[] cipherSuites = X509Util.getDefaultCipherSuitesForJavaVersion("9");
    // Java 9+ default should have the GCM suites first
    assertThat(cipherSuites[0], containsString("GCM"));
  }

  @Test
  public void testGetDefaultCipherSuitesJava10() throws Exception {
    String[] cipherSuites = X509Util.getDefaultCipherSuitesForJavaVersion("10");
    // Java 9+ default should have the GCM suites first
    assertThat(cipherSuites[0], containsString("GCM"));
  }

  @Test
  public void testGetDefaultCipherSuitesJava11() throws Exception {
    String[] cipherSuites = X509Util.getDefaultCipherSuitesForJavaVersion("11");
    // Java 9+ default should have the GCM suites first
    assertThat(cipherSuites[0], containsString("GCM"));
  }

  @Test
  public void testGetDefaultCipherSuitesUnknownVersion() throws Exception {
    String[] cipherSuites = X509Util.getDefaultCipherSuitesForJavaVersion("notaversion");
    // If version can't be parsed, use the more conservative Java 8 default
    assertThat(cipherSuites[0], containsString("CBC"));
  }

  @Test
  public void testGetDefaultCipherSuitesNullVersion() throws Exception {
    assertThrows(NullPointerException.class, () -> {
      X509Util.getDefaultCipherSuitesForJavaVersion(null);
    });
  }
}
