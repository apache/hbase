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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;

import java.security.Security;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.exceptions.KeyManagerException;
import org.apache.hadoop.hbase.exceptions.SSLContextException;
import org.apache.hadoop.hbase.exceptions.TrustManagerException;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;

import org.apache.hbase.thirdparty.io.netty.buffer.ByteBufAllocator;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.SslContext;

/**
 * This file has been copied from the Apache ZooKeeper project.
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/master/zookeeper-server/src/test/java/org/apache/zookeeper/common/X509UtilTest.java">Base
 *      revision</a>
 */
@Tag(SecurityTests.TAG)
@Tag(SmallTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: caKeyType={0}, certKeyType={1}, keyPassword={2}")
public class TestX509Util extends AbstractTestX509Parameterized {

  private static final char[] EMPTY_CHAR_ARRAY = new char[0];

  public TestX509Util(X509KeyType caKeyType, X509KeyType certKeyType, char[] keyPassword) {
    super(caKeyType, certKeyType, keyPassword);
  }

  @TestTemplate
  public void testCreateSSLContextWithClientAuthDefault() throws Exception {
    SslContext sslContext = X509Util.createSslContextForServer(conf);
    ByteBufAllocator byteBufAllocatorMock = mock(ByteBufAllocator.class);
    assertTrue(sslContext.newEngine(byteBufAllocatorMock).getNeedClientAuth());
  }

  @TestTemplate
  public void testCreateSSLContextWithClientAuthNEED() throws Exception {
    conf.set(X509Util.HBASE_SERVER_NETTY_TLS_CLIENT_AUTH_MODE, X509Util.ClientAuth.NEED.name());
    SslContext sslContext = X509Util.createSslContextForServer(conf);
    ByteBufAllocator byteBufAllocatorMock = mock(ByteBufAllocator.class);
    assertTrue(sslContext.newEngine(byteBufAllocatorMock).getNeedClientAuth());
  }

  @TestTemplate
  public void testCreateSSLContextWithClientAuthWANT() throws Exception {
    conf.set(X509Util.HBASE_SERVER_NETTY_TLS_CLIENT_AUTH_MODE, X509Util.ClientAuth.WANT.name());
    SslContext sslContext = X509Util.createSslContextForServer(conf);
    ByteBufAllocator byteBufAllocatorMock = mock(ByteBufAllocator.class);
    assertTrue(sslContext.newEngine(byteBufAllocatorMock).getWantClientAuth());
  }

  @TestTemplate
  public void testCreateSSLContextWithClientAuthNONE() throws Exception {
    conf.set(X509Util.HBASE_SERVER_NETTY_TLS_CLIENT_AUTH_MODE, X509Util.ClientAuth.NONE.name());
    SslContext sslContext = X509Util.createSslContextForServer(conf);
    ByteBufAllocator byteBufAllocatorMock = mock(ByteBufAllocator.class);
    assertFalse(sslContext.newEngine(byteBufAllocatorMock).getNeedClientAuth());
    assertFalse(sslContext.newEngine(byteBufAllocatorMock).getWantClientAuth());
  }

  @TestTemplate
  public void testCreateSSLContextWithoutCustomProtocol() throws Exception {
    SslContext sslContext = X509Util.createSslContextForClient(conf);
    ByteBufAllocator byteBufAllocatorMock = mock(ByteBufAllocator.class);
    assertArrayEquals(new String[] { "TLSv1.3", "TLSv1.2" },
      sslContext.newEngine(byteBufAllocatorMock).getEnabledProtocols());
  }

  @TestTemplate
  public void testCreateTcNativeSSLContextWithoutCustomProtocol() throws Exception {
    conf.set(X509Util.TLS_USE_OPENSSL, "true");
    SslContext sslContext = X509Util.createSslContextForClient(conf);
    ByteBufAllocator byteBufAllocatorMock = mock(ByteBufAllocator.class);
    assertArrayEquals(new String[] { "TLSv1.3", "TLSv1.2" },
      sslContext.newEngine(byteBufAllocatorMock).getEnabledProtocols());
  }

  @TestTemplate
  public void testCreateSSLContextWithCustomProtocol() throws Exception {
    final String protocol = "TLSv1.1";
    conf.set(X509Util.TLS_CONFIG_PROTOCOL, protocol);
    ByteBufAllocator byteBufAllocatorMock = mock(ByteBufAllocator.class);
    SslContext sslContext = X509Util.createSslContextForServer(conf);
    assertEquals(Collections.singletonList(protocol),
      Arrays.asList(sslContext.newEngine(byteBufAllocatorMock).getEnabledProtocols()));
  }

  @TestTemplate
  public void testCreateSSLContextWithoutKeyStoreLocationServer() {
    conf.unset(X509Util.TLS_CONFIG_KEYSTORE_LOCATION);
    assertThrows(SSLContextException.class, () -> {
      X509Util.createSslContextForServer(conf);
    });
  }

  @TestTemplate
  public void testCreateSSLContextWithoutKeyStoreLocationClient() throws Exception {
    conf.unset(X509Util.TLS_CONFIG_KEYSTORE_LOCATION);
    X509Util.createSslContextForClient(conf);
  }

  @TestTemplate
  public void testCreateSSLContextWithoutTrustStoreLocationClient() throws Exception {
    conf.unset(X509Util.TLS_CONFIG_TRUSTSTORE_LOCATION);
    X509Util.createSslContextForClient(conf);
  }

  @TestTemplate
  public void testCreateSSLContextWithoutTrustStoreLocationServer() throws Exception {
    conf.unset(X509Util.TLS_CONFIG_TRUSTSTORE_LOCATION);
    X509Util.createSslContextForServer(conf);
  }

  // It would be great to test the value of PKIXBuilderParameters#setRevocationEnabled,
  // but it does not appear to be possible
  @TestTemplate
  public void testCRLEnabled() throws Exception {
    conf.setBoolean(X509Util.TLS_CONFIG_CLR, true);
    X509Util.createSslContextForServer(conf);
    assertTrue(Boolean.valueOf(System.getProperty("com.sun.net.ssl.checkRevocation")));
    assertTrue(Boolean.valueOf(System.getProperty("com.sun.security.enableCRLDP")));
    assertFalse(Boolean.valueOf(Security.getProperty("ocsp.enable")));
  }

  @TestTemplate
  public void testCRLDisabled() throws Exception {
    X509Util.createSslContextForServer(conf);
    assertFalse(Boolean.valueOf(System.getProperty("com.sun.net.ssl.checkRevocation")));
    assertFalse(Boolean.valueOf(System.getProperty("com.sun.security.enableCRLDP")));
    assertFalse(Boolean.valueOf(Security.getProperty("ocsp.enable")));
  }

  @TestTemplate
  public void testLoadPEMKeyStore() throws Exception {
    // Make sure we can instantiate a key manager from the PEM file on disk
    X509Util.createKeyManager(
      x509TestContext.getKeyStoreFile(KeyStoreFileType.PEM).getAbsolutePath(),
      x509TestContext.getKeyStorePassword(), KeyStoreFileType.PEM.getPropertyValue());
  }

  @TestTemplate
  public void testLoadPEMKeyStoreNullPassword() throws Exception {
    assumeTrue(Arrays.equals(x509TestContext.getKeyStorePassword(), EMPTY_CHAR_ARRAY));
    // Make sure that empty password and null password are treated the same
    X509Util.createKeyManager(
      x509TestContext.getKeyStoreFile(KeyStoreFileType.PEM).getAbsolutePath(), null,
      KeyStoreFileType.PEM.getPropertyValue());
  }

  @TestTemplate
  public void testLoadPEMKeyStoreAutodetectStoreFileType() throws Exception {
    // Make sure we can instantiate a key manager from the PEM file on disk
    X509Util.createKeyManager(
      x509TestContext.getKeyStoreFile(KeyStoreFileType.PEM).getAbsolutePath(),
      x509TestContext.getKeyStorePassword(),
      null /* null StoreFileType means 'autodetect from file extension' */);
  }

  @TestTemplate
  public void testLoadPEMKeyStoreWithWrongPassword() {
    assertThrows(KeyManagerException.class, () -> {
      // Attempting to load with the wrong key password should fail
      X509Util.createKeyManager(
        x509TestContext.getKeyStoreFile(KeyStoreFileType.PEM).getAbsolutePath(),
        "wrong password".toCharArray(), // intentionally use the wrong password
        KeyStoreFileType.PEM.getPropertyValue());
    });
  }

  @TestTemplate
  public void testLoadPEMTrustStore() throws Exception {
    // Make sure we can instantiate a trust manager from the PEM file on disk
    X509Util.createTrustManager(
      x509TestContext.getTrustStoreFile(KeyStoreFileType.PEM).getAbsolutePath(),
      x509TestContext.getTrustStorePassword(), KeyStoreFileType.PEM.getPropertyValue(), false,
      false, true, true);
  }

  @TestTemplate
  public void testLoadPEMTrustStoreNullPassword() throws Exception {
    assumeTrue(Arrays.equals(x509TestContext.getTrustStorePassword(), EMPTY_CHAR_ARRAY));
    // Make sure that empty password and null password are treated the same
    X509Util.createTrustManager(
      x509TestContext.getTrustStoreFile(KeyStoreFileType.PEM).getAbsolutePath(), null,
      KeyStoreFileType.PEM.getPropertyValue(), false, false, true, true);
  }

  @TestTemplate
  public void testLoadPEMTrustStoreAutodetectStoreFileType() throws Exception {
    // Make sure we can instantiate a trust manager from the PEM file on disk
    X509Util.createTrustManager(
      x509TestContext.getTrustStoreFile(KeyStoreFileType.PEM).getAbsolutePath(),
      x509TestContext.getTrustStorePassword(), null, // null StoreFileType means 'autodetect from
                                                     // file extension'
      false, false, true, true);
  }

  @TestTemplate
  public void testLoadJKSKeyStore() throws Exception {
    // Make sure we can instantiate a key manager from the JKS file on disk
    X509Util.createKeyManager(
      x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
      x509TestContext.getKeyStorePassword(), KeyStoreFileType.JKS.getPropertyValue());
  }

  @TestTemplate
  public void testLoadJKSKeyStoreNullPassword() throws Exception {
    assumeTrue(Arrays.equals(x509TestContext.getKeyStorePassword(), EMPTY_CHAR_ARRAY));
    // Make sure that empty password and null password are treated the same
    X509Util.createKeyManager(
      x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath(), null,
      KeyStoreFileType.JKS.getPropertyValue());
  }

  @TestTemplate
  public void testLoadJKSKeyStoreAutodetectStoreFileType() throws Exception {
    // Make sure we can instantiate a key manager from the JKS file on disk
    X509Util.createKeyManager(
      x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
      x509TestContext.getKeyStorePassword(),
      null /* null StoreFileType means 'autodetect from file extension' */);
  }

  @TestTemplate
  public void testLoadJKSKeyStoreWithWrongPassword() {
    assertThrows(KeyManagerException.class, () -> {
      // Attempting to load with the wrong key password should fail
      X509Util.createKeyManager(
        x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
        "wrong password".toCharArray(), KeyStoreFileType.JKS.getPropertyValue());
    });
  }

  @TestTemplate
  public void testLoadJKSTrustStore() throws Exception {
    // Make sure we can instantiate a trust manager from the JKS file on disk
    X509Util.createTrustManager(
      x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
      x509TestContext.getTrustStorePassword(), KeyStoreFileType.JKS.getPropertyValue(), true, true,
      true, true);
  }

  @TestTemplate
  public void testLoadJKSTrustStoreNullPassword() throws Exception {
    assumeTrue(Arrays.equals(x509TestContext.getTrustStorePassword(), EMPTY_CHAR_ARRAY));
    // Make sure that empty password and null password are treated the same
    X509Util.createTrustManager(
      x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath(), null,
      KeyStoreFileType.JKS.getPropertyValue(), false, false, true, true);
  }

  @TestTemplate
  public void testLoadJKSTrustStoreAutodetectStoreFileType() throws Exception {
    // Make sure we can instantiate a trust manager from the JKS file on disk
    // null StoreFileType means 'autodetect from file extension'
    X509Util.createTrustManager(
      x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
      x509TestContext.getTrustStorePassword(), null, true, true, true, true);
  }

  @TestTemplate
  public void testLoadJKSTrustStoreWithWrongPassword() {
    assertThrows(TrustManagerException.class, () -> {
      // Attempting to load with the wrong key password should fail
      X509Util.createTrustManager(
        x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
        "wrong password".toCharArray(), KeyStoreFileType.JKS.getPropertyValue(), true, true, true,
        true);
    });
  }

  @TestTemplate
  public void testLoadPKCS12KeyStore() throws Exception {
    // Make sure we can instantiate a key manager from the PKCS12 file on disk
    X509Util.createKeyManager(
      x509TestContext.getKeyStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
      x509TestContext.getKeyStorePassword(), KeyStoreFileType.PKCS12.getPropertyValue());
  }

  @TestTemplate
  public void testLoadPKCS12KeyStoreNullPassword() throws Exception {
    assumeTrue(Arrays.equals(x509TestContext.getKeyStorePassword(), EMPTY_CHAR_ARRAY));
    // Make sure that empty password and null password are treated the same
    X509Util.createKeyManager(
      x509TestContext.getKeyStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(), null,
      KeyStoreFileType.PKCS12.getPropertyValue());
  }

  @TestTemplate
  public void testLoadPKCS12KeyStoreAutodetectStoreFileType() throws Exception {
    // Make sure we can instantiate a key manager from the PKCS12 file on disk
    X509Util.createKeyManager(
      x509TestContext.getKeyStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
      x509TestContext.getKeyStorePassword(),
      null /* null StoreFileType means 'autodetect from file extension' */);
  }

  @TestTemplate
  public void testLoadPKCS12KeyStoreWithWrongPassword() {
    assertThrows(KeyManagerException.class, () -> {
      // Attempting to load with the wrong key password should fail
      X509Util.createKeyManager(
        x509TestContext.getKeyStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
        "wrong password".toCharArray(), KeyStoreFileType.PKCS12.getPropertyValue());
    });
  }

  @TestTemplate
  public void testLoadPKCS12TrustStore() throws Exception {
    // Make sure we can instantiate a trust manager from the PKCS12 file on disk
    X509Util.createTrustManager(
      x509TestContext.getTrustStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
      x509TestContext.getTrustStorePassword(), KeyStoreFileType.PKCS12.getPropertyValue(), true,
      true, true, true);
  }

  @TestTemplate
  public void testLoadPKCS12TrustStoreNullPassword() throws Exception {
    assumeTrue(Arrays.equals(x509TestContext.getTrustStorePassword(), EMPTY_CHAR_ARRAY));
    // Make sure that empty password and null password are treated the same
    X509Util.createTrustManager(
      x509TestContext.getTrustStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(), null,
      KeyStoreFileType.PKCS12.getPropertyValue(), false, false, true, true);
  }

  @TestTemplate
  public void testLoadPKCS12TrustStoreAutodetectStoreFileType() throws Exception {
    // Make sure we can instantiate a trust manager from the PKCS12 file on disk
    X509Util.createTrustManager(
      x509TestContext.getTrustStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
      x509TestContext.getTrustStorePassword(), null, // null StoreFileType means 'autodetect from
                                                     // file extension'
      true, true, true, true);
  }

  @TestTemplate
  public void testLoadPKCS12TrustStoreWithWrongPassword() {
    assertThrows(TrustManagerException.class, () -> {
      // Attempting to load with the wrong key password should fail
      X509Util.createTrustManager(
        x509TestContext.getTrustStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
        "wrong password".toCharArray(), KeyStoreFileType.PKCS12.getPropertyValue(), true, true,
        true, true);
    });
  }

}
