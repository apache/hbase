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

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.security.KeyPair;
import java.security.Security;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.net.InetAddresses;

/**
 * Test cases taken and adapted from Apache ZooKeeper Project
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/5820d10d9dc58c8e12d2e25386fdf92acb360359/zookeeper-server/src/test/java/org/apache/zookeeper/common/ZKHostnameVerifierTest.java">Base
 *      revision</a>
 */
@Category({ MiscTests.class, SmallTests.class })
public class TestHBaseHostnameVerifier {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHBaseHostnameVerifier.class);
  private static CertificateCreator certificateCreator;
  private HBaseHostnameVerifier impl;

  @BeforeClass
  public static void setupClass() throws Exception {
    Security.addProvider(new BouncyCastleProvider());
    X500NameBuilder caNameBuilder = new X500NameBuilder(BCStyle.INSTANCE);
    caNameBuilder.addRDN(BCStyle.CN,
      MethodHandles.lookup().lookupClass().getCanonicalName() + " Root CA");
    KeyPair keyPair = X509TestHelpers.generateKeyPair(X509KeyType.EC);
    X509Certificate caCert = X509TestHelpers.newSelfSignedCACert(caNameBuilder.build(), keyPair);
    certificateCreator = new CertificateCreator(keyPair, caCert);
  }

  @Before
  public void setup() {
    impl = new HBaseHostnameVerifier();
  }

  private static class CertificateCreator {
    private final KeyPair caCertPair;
    private final X509Certificate caCert;

    public CertificateCreator(KeyPair caCertPair, X509Certificate caCert) {
      this.caCertPair = caCertPair;
      this.caCert = caCert;
    }

    public byte[] newCert(String cn, String... subjectAltName) throws Exception {
      return X509TestHelpers.newCert(caCert, caCertPair, cn == null ? null : new X500Name(cn),
        caCertPair.getPublic(), parseSubjectAltNames(subjectAltName)).getEncoded();
    }

    private GeneralNames parseSubjectAltNames(String... subjectAltName) {
      if (subjectAltName == null || subjectAltName.length == 0) {
        return null;
      }
      GeneralName[] names = new GeneralName[subjectAltName.length];
      for (int i = 0; i < subjectAltName.length; i++) {
        String current = subjectAltName[i];
        int type;
        if (InetAddresses.isInetAddress(current)) {
          type = GeneralName.iPAddress;
        } else if (current.startsWith("email:")) {
          type = GeneralName.rfc822Name;
        } else {
          type = GeneralName.dNSName;
        }
        names[i] = new GeneralName(type, subjectAltName[i]);
      }
      return new GeneralNames(names);
    }

  }

  @Test
  public void testVerify() throws Exception {
    final CertificateFactory cf = CertificateFactory.getInstance("X.509");
    InputStream in;
    X509Certificate x509;

    in = new ByteArrayInputStream(certificateCreator.newCert("CN=foo.com"));
    x509 = (X509Certificate) cf.generateCertificate(in);

    impl.verify("foo.com", x509);
    exceptionPlease(impl, "a.foo.com", x509);
    exceptionPlease(impl, "bar.com", x509);

    in = new ByteArrayInputStream(certificateCreator.newCert("CN=\u82b1\u5b50.co.jp"));
    x509 = (X509Certificate) cf.generateCertificate(in);
    impl.verify("\u82b1\u5b50.co.jp", x509);
    exceptionPlease(impl, "a.\u82b1\u5b50.co.jp", x509);

    in = new ByteArrayInputStream(certificateCreator.newCert("CN=foo.com", "bar.com"));
    x509 = (X509Certificate) cf.generateCertificate(in);
    exceptionPlease(impl, "foo.com", x509);
    exceptionPlease(impl, "a.foo.com", x509);
    impl.verify("bar.com", x509);
    exceptionPlease(impl, "a.bar.com", x509);

    in = new ByteArrayInputStream(
      certificateCreator.newCert("CN=foo.com", "bar.com", "\u82b1\u5b50.co.jp"));
    x509 = (X509Certificate) cf.generateCertificate(in);
    exceptionPlease(impl, "foo.com", x509);
    exceptionPlease(impl, "a.foo.com", x509);
    impl.verify("bar.com", x509);
    exceptionPlease(impl, "a.bar.com", x509);

    /*
     * Java isn't extracting international subjectAlts properly. (Or OpenSSL isn't storing them
     * properly).
     */
    // DEFAULT.verify("\u82b1\u5b50.co.jp", x509 );
    // impl.verify("\u82b1\u5b50.co.jp", x509 );
    exceptionPlease(impl, "a.\u82b1\u5b50.co.jp", x509);

    in = new ByteArrayInputStream(certificateCreator.newCert("CN=", "foo.com"));
    x509 = (X509Certificate) cf.generateCertificate(in);
    impl.verify("foo.com", x509);
    exceptionPlease(impl, "a.foo.com", x509);

    in = new ByteArrayInputStream(
      certificateCreator.newCert("CN=foo.com, CN=bar.com, CN=\u82b1\u5b50.co.jp"));
    x509 = (X509Certificate) cf.generateCertificate(in);
    exceptionPlease(impl, "foo.com", x509);
    exceptionPlease(impl, "a.foo.com", x509);
    exceptionPlease(impl, "bar.com", x509);
    exceptionPlease(impl, "a.bar.com", x509);
    impl.verify("\u82b1\u5b50.co.jp", x509);
    exceptionPlease(impl, "a.\u82b1\u5b50.co.jp", x509);

    in = new ByteArrayInputStream(certificateCreator.newCert("CN=*.foo.com"));
    x509 = (X509Certificate) cf.generateCertificate(in);
    exceptionPlease(impl, "foo.com", x509);
    impl.verify("www.foo.com", x509);
    impl.verify("\u82b1\u5b50.foo.com", x509);
    exceptionPlease(impl, "a.b.foo.com", x509);

    in = new ByteArrayInputStream(certificateCreator.newCert("CN=*.co.jp"));
    x509 = (X509Certificate) cf.generateCertificate(in);
    // Silly test because no-one would ever be able to lookup an IP address
    // using "*.co.jp".
    impl.verify("*.co.jp", x509);
    impl.verify("foo.co.jp", x509);
    impl.verify("\u82b1\u5b50.co.jp", x509);

    in = new ByteArrayInputStream(
      certificateCreator.newCert("CN=*.foo.com", "*.bar.com", "*.\u82b1\u5b50.co.jp"));
    x509 = (X509Certificate) cf.generateCertificate(in);
    // try the foo.com variations
    exceptionPlease(impl, "foo.com", x509);
    exceptionPlease(impl, "www.foo.com", x509);
    exceptionPlease(impl, "\u82b1\u5b50.foo.com", x509);
    exceptionPlease(impl, "a.b.foo.com", x509);
    // try the bar.com variations
    exceptionPlease(impl, "bar.com", x509);
    impl.verify("www.bar.com", x509);
    impl.verify("\u82b1\u5b50.bar.com", x509);
    exceptionPlease(impl, "a.b.bar.com", x509);

    in = new ByteArrayInputStream(certificateCreator.newCert("CN=repository.infonotary.com"));
    x509 = (X509Certificate) cf.generateCertificate(in);
    impl.verify("repository.infonotary.com", x509);

    in = new ByteArrayInputStream(certificateCreator.newCert("CN=*.google.com"));
    x509 = (X509Certificate) cf.generateCertificate(in);
    impl.verify("*.google.com", x509);

    in = new ByteArrayInputStream(certificateCreator.newCert("CN=*.google.com"));
    x509 = (X509Certificate) cf.generateCertificate(in);
    impl.verify("*.Google.com", x509);

    in = new ByteArrayInputStream(certificateCreator.newCert("CN=dummy-value.com", "1.1.1.1"));
    x509 = (X509Certificate) cf.generateCertificate(in);
    impl.verify("1.1.1.1", x509);

    exceptionPlease(impl, "1.1.1.2", x509);
    exceptionPlease(impl, "2001:0db8:85a3:0000:0000:8a2e:0370:1111", x509);
    exceptionPlease(impl, "dummy-value.com", x509);

    in = new ByteArrayInputStream(
      certificateCreator.newCert("CN=dummy-value.com", "2001:0db8:85a3:0000:0000:8a2e:0370:7334"));
    x509 = (X509Certificate) cf.generateCertificate(in);
    impl.verify("2001:0db8:85a3:0000:0000:8a2e:0370:7334", x509);

    exceptionPlease(impl, "1.1.1.2", x509);
    exceptionPlease(impl, "2001:0db8:85a3:0000:0000:8a2e:0370:1111", x509);
    exceptionPlease(impl, "dummy-value.com", x509);

    in = new ByteArrayInputStream(
      certificateCreator.newCert("CN=dummy-value.com", "2001:0db8:85a3:0000:0000:8a2e:0370:7334"));
    x509 = (X509Certificate) cf.generateCertificate(in);
    impl.verify("2001:0db8:85a3:0000:0000:8a2e:0370:7334", x509);
    impl.verify("[2001:0db8:85a3:0000:0000:8a2e:0370:7334]", x509);

    exceptionPlease(impl, "1.1.1.2", x509);
    exceptionPlease(impl, "2001:0db8:85a3:0000:0000:8a2e:0370:1111", x509);
    exceptionPlease(impl, "dummy-value.com", x509);

    in = new ByteArrayInputStream(
      certificateCreator.newCert("CN=www.company.com", "email:email@example.com"));
    x509 = (X509Certificate) cf.generateCertificate(in);
    impl.verify("www.company.com", x509);
  }

  private void exceptionPlease(final HBaseHostnameVerifier hv, final String host,
    final X509Certificate x509) {
    try {
      hv.verify(host, x509);
      fail("HostnameVerifier shouldn't allow [" + host + "]");
    } catch (final SSLException e) {
      // whew! we're okay!
    }
  }
}
