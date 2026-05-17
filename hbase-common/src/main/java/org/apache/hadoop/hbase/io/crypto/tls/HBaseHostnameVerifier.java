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

import java.net.InetAddress;
import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import javax.naming.InvalidNameException;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.net.InetAddresses;

/**
 * When enabled in {@link X509Util}, handles verifying that the hostname of a peer matches the
 * certificate it presents.
 * <p/>
 * This file has been copied from the Apache ZooKeeper project.
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/5820d10d9dc58c8e12d2e25386fdf92acb360359/zookeeper-server/src/main/java/org/apache/zookeeper/common/ZKHostnameVerifier.java">Base
 *      revision</a>
 */
@InterfaceAudience.Private
final class HBaseHostnameVerifier implements HostnameVerifier {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseHostnameVerifier.class);

  /**
   * Note: copied from Apache httpclient with some minor modifications. We want host verification,
   * but depending on the httpclient jar caused unexplained performance regressions (even when the
   * code was not used).
   */
  private static final class SubjectName {

    static final int DNS = 2;
    static final int IP = 7;

    private final String value;
    private final int type;

    SubjectName(final String value, final int type) {
      if (type != DNS && type != IP) {
        throw new IllegalArgumentException("Invalid type: " + type);
      }
      this.value = Objects.requireNonNull(value);
      this.type = type;
    }

    private int getType() {
      return type;
    }

    private String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return value;
    }

  }

  @Override
  public boolean verify(final String host, final SSLSession session) {
    try {
      final Certificate[] certs = session.getPeerCertificates();
      final X509Certificate x509 = (X509Certificate) certs[0];
      verify(host, x509);
      return true;
    } catch (final SSLException ex) {
      LOG.debug("Unexpected exception", ex);
      return false;
    }
  }

  void verify(final String host, final X509Certificate cert) throws SSLException {
    final List<SubjectName> subjectAlts = getSubjectAltNames(cert);
    if (subjectAlts != null && !subjectAlts.isEmpty()) {
      Optional<InetAddress> inetAddress = parseIpAddress(host);
      if (inetAddress.isPresent()) {
        matchIPAddress(host, inetAddress.get(), subjectAlts);
      } else {
        matchDNSName(host, subjectAlts);
      }
    } else {
      // CN matching has been deprecated by rfc2818 and can be used
      // as fallback only when no subjectAlts are available
      final X500Principal subjectPrincipal = cert.getSubjectX500Principal();
      final String cn = extractCN(subjectPrincipal.getName(X500Principal.RFC2253));
      if (cn == null) {
        throw new SSLException("Certificate subject for <" + host + "> doesn't contain "
          + "a common name and does not have alternative names");
      }
      matchCN(host, cn);
    }
  }

  private static void matchIPAddress(final String host, final InetAddress inetAddress,
    final List<SubjectName> subjectAlts) throws SSLException {
    for (final SubjectName subjectAlt : subjectAlts) {
      if (subjectAlt.getType() == SubjectName.IP) {
        Optional<InetAddress> parsed = parseIpAddress(subjectAlt.getValue());
        if (parsed.filter(altAddr -> altAddr.equals(inetAddress)).isPresent()) {
          return;
        }
      }
    }
    throw new SSLPeerUnverifiedException("Certificate for <" + host + "> doesn't match any "
      + "of the subject alternative names: " + subjectAlts);
  }

  private static void matchDNSName(final String host, final List<SubjectName> subjectAlts)
    throws SSLException {
    final String normalizedHost = host.toLowerCase(Locale.ROOT);
    for (final SubjectName subjectAlt : subjectAlts) {
      if (subjectAlt.getType() == SubjectName.DNS) {
        final String normalizedSubjectAlt = subjectAlt.getValue().toLowerCase(Locale.ROOT);
        if (matchIdentityStrict(normalizedHost, normalizedSubjectAlt)) {
          return;
        }
      }
    }
    throw new SSLPeerUnverifiedException("Certificate for <" + host + "> doesn't match any "
      + "of the subject alternative names: " + subjectAlts);
  }

  private static void matchCN(final String host, final String cn) throws SSLException {
    final String normalizedHost = host.toLowerCase(Locale.ROOT);
    final String normalizedCn = cn.toLowerCase(Locale.ROOT);
    if (!matchIdentityStrict(normalizedHost, normalizedCn)) {
      throw new SSLPeerUnverifiedException("Certificate for <" + host + "> doesn't match "
        + "common name of the certificate subject: " + cn);
    }
  }

  private static boolean matchIdentity(final String host, final String identity,
    final boolean strict) {
    // RFC 2818, 3.1. Server Identity
    // "...Names may contain the wildcard
    // character * which is considered to match any single domain name
    // component or component fragment..."
    // Based on this statement presuming only singular wildcard is legal
    final int asteriskIdx = identity.indexOf('*');
    if (asteriskIdx != -1) {
      final String prefix = identity.substring(0, asteriskIdx);
      final String suffix = identity.substring(asteriskIdx + 1);
      if (!prefix.isEmpty() && !host.startsWith(prefix)) {
        return false;
      }
      if (!suffix.isEmpty() && !host.endsWith(suffix)) {
        return false;
      }
      // Additional sanity checks on content selected by wildcard can be done here
      if (strict) {
        final String remainder = host.substring(prefix.length(), host.length() - suffix.length());
        return !remainder.contains(".");
      }
      return true;
    }
    return host.equalsIgnoreCase(identity);
  }

  private static boolean matchIdentityStrict(final String host, final String identity) {
    return matchIdentity(host, identity, true);
  }

  private static String extractCN(final String subjectPrincipal) throws SSLException {
    if (subjectPrincipal == null) {
      return null;
    }
    try {
      final LdapName subjectDN = new LdapName(subjectPrincipal);
      final List<Rdn> rdns = subjectDN.getRdns();
      for (int i = rdns.size() - 1; i >= 0; i--) {
        final Rdn rds = rdns.get(i);
        final Attributes attributes = rds.toAttributes();
        final Attribute cn = attributes.get("cn");
        if (cn != null) {
          try {
            final Object value = cn.get();
            if (value != null) {
              return value.toString();
            }
          } catch (final NoSuchElementException ignore) {
            // ignore exception
          } catch (final NamingException ignore) {
            // ignore exception
          }
        }
      }
      return null;
    } catch (final InvalidNameException e) {
      throw new SSLException(subjectPrincipal + " is not a valid X500 distinguished name");
    }
  }

  private static Optional<InetAddress> parseIpAddress(String host) {
    host = host.trim();
    // Uri strings only work for ipv6 and are wrapped with brackets
    // Unfortunately InetAddresses can't handle a mixed input, so we
    // check here and choose which parse method to use.
    if (host.startsWith("[") && host.endsWith("]")) {
      return parseIpAddressUriString(host);
    } else {
      return parseIpAddressString(host);
    }
  }

  private static Optional<InetAddress> parseIpAddressUriString(String host) {
    if (InetAddresses.isUriInetAddress(host)) {
      return Optional.of(InetAddresses.forUriString(host));
    }
    return Optional.empty();
  }

  private static Optional<InetAddress> parseIpAddressString(String host) {
    if (InetAddresses.isInetAddress(host)) {
      return Optional.of(InetAddresses.forString(host));
    }
    return Optional.empty();
  }

  @SuppressWarnings("MixedMutabilityReturnType")
  private static List<SubjectName> getSubjectAltNames(final X509Certificate cert) {
    try {
      final Collection<List<?>> entries = cert.getSubjectAlternativeNames();
      if (entries == null) {
        return Collections.emptyList();
      }
      final List<SubjectName> result = new ArrayList<SubjectName>();
      for (List<?> entry : entries) {
        final Integer type = entry.size() >= 2 ? (Integer) entry.get(0) : null;
        if (type != null) {
          if (type == SubjectName.DNS || type == SubjectName.IP) {
            final Object o = entry.get(1);
            if (o instanceof String) {
              result.add(new SubjectName((String) o, type));
            } else {
              LOG.debug("non-string Subject Alt Name type detected, not currently supported: {}",
                o);
            }
          }
        }
      }
      return result;
    } catch (final CertificateParsingException ignore) {
      return Collections.emptyList();
    }
  }
}
