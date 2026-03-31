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

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.Socket;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.stubbing.Answer;

//
/**
 * Test cases taken and adapted from Apache ZooKeeper Project. We can only test calls to
 * HBaseTrustManager using Sockets (not SSLEngines). This can be fine since the logic is the same.
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/c74658d398cdc1d207aa296cb6e20de00faec03e/zookeeper-server/src/test/java/org/apache/zookeeper/common/HBaseTrustManagerTest.java">Base
 *      revision</a>
 */
@Category({ MiscTests.class, SmallTests.class })
public class TestHBaseTrustManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHBaseTrustManager.class);

  private static KeyPair keyPair;

  private X509ExtendedTrustManager mockX509ExtendedTrustManager;
  private static final String IP_ADDRESS = "127.0.0.1";
  private static final String HOSTNAME = "localhost";

  private InetAddress mockInetAddressWithoutHostname;
  private InetAddress mockInetAddressWithHostname;
  private Socket mockSocketWithoutHostname;
  private Socket mockSocketWithHostname;
  private SSLEngine mockSSLEngineWithoutPeerhost;
  private SSLEngine mockSSLEngineWithPeerhost;

  @BeforeClass
  public static void createKeyPair() throws Exception {
    Security.addProvider(new BouncyCastleProvider());
    KeyPairGenerator keyPairGenerator =
      KeyPairGenerator.getInstance("RSA", BouncyCastleProvider.PROVIDER_NAME);
    keyPairGenerator.initialize(4096);
    keyPair = keyPairGenerator.genKeyPair();
  }

  @AfterClass
  public static void removeBouncyCastleProvider() throws Exception {
    Security.removeProvider("BC");
  }

  @Before
  public void setup() throws Exception {
    mockX509ExtendedTrustManager = mock(X509ExtendedTrustManager.class);

    mockInetAddressWithoutHostname = mock(InetAddress.class);
    when(mockInetAddressWithoutHostname.getHostAddress())
      .thenAnswer((Answer<?>) invocationOnMock -> IP_ADDRESS);
    when(mockInetAddressWithoutHostname.toString())
      .thenAnswer((Answer<?>) invocationOnMock -> "/" + IP_ADDRESS);

    mockInetAddressWithHostname = mock(InetAddress.class);
    when(mockInetAddressWithHostname.getHostAddress())
      .thenAnswer((Answer<?>) invocationOnMock -> IP_ADDRESS);
    when(mockInetAddressWithHostname.getHostName())
      .thenAnswer((Answer<?>) invocationOnMock -> HOSTNAME);
    when(mockInetAddressWithHostname.toString())
      .thenAnswer((Answer<?>) invocationOnMock -> HOSTNAME + "/" + IP_ADDRESS);

    mockSocketWithoutHostname = mock(Socket.class);
    when(mockSocketWithoutHostname.getInetAddress())
      .thenAnswer((Answer<?>) invocationOnMock -> mockInetAddressWithoutHostname);

    mockSocketWithHostname = mock(Socket.class);
    when(mockSocketWithHostname.getInetAddress())
      .thenAnswer((Answer<?>) invocationOnMock -> mockInetAddressWithHostname);

    mockSSLEngineWithoutPeerhost = mock(SSLEngine.class);
    doReturn(null).when(mockSSLEngineWithoutPeerhost).getPeerHost();

    mockSSLEngineWithPeerhost = mock(SSLEngine.class);
    doReturn(IP_ADDRESS).when(mockSSLEngineWithPeerhost).getPeerHost();
  }

  @SuppressWarnings("JavaUtilDate")
  private X509Certificate[] createSelfSignedCertificateChain(String ipAddress, String hostname)
    throws Exception {
    X500NameBuilder nameBuilder = new X500NameBuilder(BCStyle.INSTANCE);
    nameBuilder.addRDN(BCStyle.CN, "NOT_LOCALHOST");
    Date notBefore = new Date();
    Calendar cal = Calendar.getInstance();
    cal.setTime(notBefore);
    cal.add(Calendar.YEAR, 1);
    Date notAfter = cal.getTime();
    BigInteger serialNumber = new BigInteger(128, new Random());

    X509v3CertificateBuilder certificateBuilder =
      new JcaX509v3CertificateBuilder(nameBuilder.build(), serialNumber, notBefore, notAfter,
        nameBuilder.build(), keyPair.getPublic())
        .addExtension(Extension.basicConstraints, true, new BasicConstraints(0))
        .addExtension(Extension.keyUsage, true,
          new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyCertSign | KeyUsage.cRLSign));

    List<GeneralName> generalNames = new ArrayList<>();
    if (ipAddress != null) {
      generalNames.add(new GeneralName(GeneralName.iPAddress, ipAddress));
    }
    if (hostname != null) {
      generalNames.add(new GeneralName(GeneralName.dNSName, hostname));
    }

    if (!generalNames.isEmpty()) {
      certificateBuilder.addExtension(Extension.subjectAlternativeName, true,
        new GeneralNames(generalNames.toArray(new GeneralName[] {})));
    }

    ContentSigner contentSigner =
      new JcaContentSignerBuilder("SHA256WithRSAEncryption").build(keyPair.getPrivate());

    return new X509Certificate[] {
      new JcaX509CertificateConverter().getCertificate(certificateBuilder.build(contentSigner)) };
  }

  @Test
  public void testServerHostnameVerificationWithHostnameVerificationDisabled() throws Exception {
    HBaseTrustManager trustManager =
      new HBaseTrustManager(mockX509ExtendedTrustManager, false, false);

    X509Certificate[] certificateChain = createSelfSignedCertificateChain(IP_ADDRESS, HOSTNAME);
    trustManager.checkServerTrusted(certificateChain, null, mockSocketWithHostname);

    verify(mockInetAddressWithHostname, times(0)).getHostAddress();
    verify(mockInetAddressWithHostname, times(0)).getHostName();

    verify(mockX509ExtendedTrustManager, times(1)).checkServerTrusted(certificateChain, null,
      mockSocketWithHostname);
  }

  @SuppressWarnings("checkstyle:linelength")
  @Test
  public void testServerTrustedWithHostnameVerificationDisabled() throws Exception {
    HBaseTrustManager trustManager =
      new HBaseTrustManager(mockX509ExtendedTrustManager, false, false);

    X509Certificate[] certificateChain = createSelfSignedCertificateChain(IP_ADDRESS, HOSTNAME);
    trustManager.checkServerTrusted(certificateChain, null, mockSocketWithHostname);

    verify(mockInetAddressWithHostname, times(0)).getHostAddress();
    verify(mockInetAddressWithHostname, times(0)).getHostName();

    verify(mockX509ExtendedTrustManager, times(1)).checkServerTrusted(certificateChain, null,
      mockSocketWithHostname);
  }

  @Test
  public void testServerTrustedWithHostnameVerificationEnabled() throws Exception {
    HBaseTrustManager trustManager =
      new HBaseTrustManager(mockX509ExtendedTrustManager, true, true);

    X509Certificate[] certificateChain = createSelfSignedCertificateChain(null, HOSTNAME);
    trustManager.checkServerTrusted(certificateChain, null, mockSocketWithHostname);

    verify(mockInetAddressWithHostname, times(1)).getHostAddress();
    verify(mockInetAddressWithHostname, times(1)).getHostName();

    verify(mockX509ExtendedTrustManager, times(1)).checkServerTrusted(certificateChain, null,
      mockSocketWithHostname);
  }

  @Test
  public void testServerTrustedWithHostnameVerificationEnabledUsingIpAddress() throws Exception {
    HBaseTrustManager trustManager =
      new HBaseTrustManager(mockX509ExtendedTrustManager, true, true);

    X509Certificate[] certificateChain = createSelfSignedCertificateChain(IP_ADDRESS, null);
    trustManager.checkServerTrusted(certificateChain, null, mockSocketWithHostname);

    verify(mockInetAddressWithHostname, times(1)).getHostAddress();
    verify(mockInetAddressWithHostname, times(0)).getHostName();

    verify(mockX509ExtendedTrustManager, times(1)).checkServerTrusted(certificateChain, null,
      mockSocketWithHostname);
  }

  @Test
  public void testServerTrustedWithHostnameVerificationEnabledNoReverseLookup() throws Exception {
    HBaseTrustManager trustManager =
      new HBaseTrustManager(mockX509ExtendedTrustManager, true, false);

    X509Certificate[] certificateChain = createSelfSignedCertificateChain(null, HOSTNAME);

    // We only include hostname in the cert above, but the socket passed in is for an ip address.
    // This mismatch would succeed if reverse lookup is enabled, but here fails since it's
    // not enabled.
    assertThrows(CertificateException.class,
      () -> trustManager.checkServerTrusted(certificateChain, null, mockSocketWithoutHostname));

    verify(mockInetAddressWithoutHostname, times(1)).getHostAddress();
    verify(mockInetAddressWithoutHostname, times(0)).getHostName();

    verify(mockX509ExtendedTrustManager, times(1)).checkServerTrusted(certificateChain, null,
      mockSocketWithoutHostname);
  }

  @Test
  public void testServerTrustedWithHostnameVerificationEnabledWithHostnameNoReverseLookup()
    throws Exception {
    HBaseTrustManager trustManager =
      new HBaseTrustManager(mockX509ExtendedTrustManager, true, false);

    X509Certificate[] certificateChain = createSelfSignedCertificateChain(null, HOSTNAME);

    // since the socket inetAddress already has a hostname, we don't need reverse lookup.
    // so this succeeds
    trustManager.checkServerTrusted(certificateChain, null, mockSocketWithHostname);

    verify(mockInetAddressWithHostname, times(1)).getHostAddress();
    verify(mockInetAddressWithHostname, times(1)).getHostName();

    verify(mockX509ExtendedTrustManager, times(1)).checkServerTrusted(certificateChain, null,
      mockSocketWithHostname);
  }

  @Test
  public void testClientTrustedWithHostnameVerificationDisabled() throws Exception {
    HBaseTrustManager trustManager =
      new HBaseTrustManager(mockX509ExtendedTrustManager, false, false);

    X509Certificate[] certificateChain = createSelfSignedCertificateChain(null, HOSTNAME);
    trustManager.checkClientTrusted(certificateChain, null, mockSocketWithHostname);

    verify(mockInetAddressWithHostname, times(0)).getHostAddress();
    verify(mockInetAddressWithHostname, times(0)).getHostName();

    verify(mockX509ExtendedTrustManager, times(1)).checkClientTrusted(certificateChain, null,
      mockSocketWithHostname);
  }

  @Test
  public void testClientTrustedWithHostnameVerificationEnabled() throws Exception {
    HBaseTrustManager trustManager =
      new HBaseTrustManager(mockX509ExtendedTrustManager, true, true);

    X509Certificate[] certificateChain = createSelfSignedCertificateChain(null, HOSTNAME);
    trustManager.checkClientTrusted(certificateChain, null, mockSocketWithHostname);

    verify(mockInetAddressWithHostname, times(1)).getHostAddress();
    verify(mockInetAddressWithHostname, times(1)).getHostName();

    verify(mockX509ExtendedTrustManager, times(1)).checkClientTrusted(certificateChain, null,
      mockSocketWithHostname);
  }

  @Test
  public void testClientTrustedWithHostnameVerificationEnabledUsingIpAddress() throws Exception {
    HBaseTrustManager trustManager =
      new HBaseTrustManager(mockX509ExtendedTrustManager, true, true);

    X509Certificate[] certificateChain = createSelfSignedCertificateChain(IP_ADDRESS, null);
    trustManager.checkClientTrusted(certificateChain, null, mockSocketWithHostname);

    verify(mockInetAddressWithHostname, times(1)).getHostAddress();
    verify(mockInetAddressWithHostname, times(0)).getHostName();

    verify(mockX509ExtendedTrustManager, times(1)).checkClientTrusted(certificateChain, null,
      mockSocketWithHostname);
  }

  @Test
  public void testClientTrustedWithHostnameVerificationEnabledWithoutReverseLookup()
    throws Exception {
    HBaseTrustManager trustManager =
      new HBaseTrustManager(mockX509ExtendedTrustManager, true, false);

    X509Certificate[] certificateChain = createSelfSignedCertificateChain(null, HOSTNAME);

    // We only include hostname in the cert above, but the socket passed in is for an ip address.
    // This mismatch would succeed if reverse lookup is enabled, but here fails since it's
    // not enabled.
    assertThrows(CertificateException.class,
      () -> trustManager.checkClientTrusted(certificateChain, null, mockSocketWithoutHostname));

    verify(mockInetAddressWithoutHostname, times(1)).getHostAddress();
    verify(mockInetAddressWithoutHostname, times(0)).getHostName();

    verify(mockX509ExtendedTrustManager, times(1)).checkClientTrusted(certificateChain, null,
      mockSocketWithoutHostname);
  }

  @Test
  public void testClientTrustedWithHostnameVerificationEnabledWithHostnameNoReverseLookup()
    throws Exception {
    HBaseTrustManager trustManager =
      new HBaseTrustManager(mockX509ExtendedTrustManager, true, false);

    X509Certificate[] certificateChain = createSelfSignedCertificateChain(null, HOSTNAME);

    // since the socket inetAddress already has a hostname, we don't need reverse lookup.
    // so this succeeds
    trustManager.checkClientTrusted(certificateChain, null, mockSocketWithHostname);

    verify(mockInetAddressWithHostname, times(1)).getHostAddress();
    verify(mockInetAddressWithHostname, times(1)).getHostName();

    verify(mockX509ExtendedTrustManager, times(1)).checkClientTrusted(certificateChain, null,
      mockSocketWithHostname);
  }

  @Test
  public void testClientTrustedSslEngineWithPeerHostReverseLookup() throws Exception {
    HBaseTrustManager trustManager =
      new HBaseTrustManager(mockX509ExtendedTrustManager, true, true);
    X509Certificate[] certificateChain = createSelfSignedCertificateChain(null, HOSTNAME);
    trustManager.checkClientTrusted(certificateChain, null, mockSSLEngineWithPeerhost);
  }

  @Test(expected = CertificateException.class)
  public void testClientTrustedSslEngineWithPeerHostNoReverseLookup() throws Exception {
    HBaseTrustManager trustManager =
      new HBaseTrustManager(mockX509ExtendedTrustManager, true, false);
    X509Certificate[] certificateChain = createSelfSignedCertificateChain(null, HOSTNAME);
    trustManager.checkClientTrusted(certificateChain, null, mockSSLEngineWithPeerhost);
  }

  @Test
  public void testClientTrustedSslEngineWithoutPeerHost() throws Exception {
    HBaseTrustManager trustManager =
      new HBaseTrustManager(mockX509ExtendedTrustManager, true, false);
    X509Certificate[] certificateChain = createSelfSignedCertificateChain(null, HOSTNAME);
    trustManager.checkClientTrusted(certificateChain, null, mockSSLEngineWithoutPeerhost);
  }

  @Test
  public void testClientTrustedSslEngineNotAvailable() throws Exception {
    HBaseTrustManager trustManager =
      new HBaseTrustManager(mockX509ExtendedTrustManager, true, false);
    X509Certificate[] certificateChain = createSelfSignedCertificateChain(null, HOSTNAME);
    trustManager.checkClientTrusted(certificateChain, null, (SSLEngine) null);
  }
}
