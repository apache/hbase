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
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.X509ExtendedTrustManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A custom TrustManager that supports hostname verification We attempt to perform verification
 * using just the IP address first and if that fails will attempt to perform a reverse DNS lookup
 * and verify using the hostname. This file has been copied from the Apache ZooKeeper project.
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/c74658d398cdc1d207aa296cb6e20de00faec03e/zookeeper-server/src/main/java/org/apache/zookeeper/common/ZKTrustManager.java">Base
 *      revision</a>
 */
@InterfaceAudience.Private
public class HBaseTrustManager extends X509ExtendedTrustManager {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseTrustManager.class);

  private final X509ExtendedTrustManager x509ExtendedTrustManager;
  private final boolean hostnameVerificationEnabled;
  private final boolean allowReverseDnsLookup;

  private final HBaseHostnameVerifier hostnameVerifier;

  /**
   * Instantiate a new HBaseTrustManager.
   * @param x509ExtendedTrustManager    The trustmanager to use for
   *                                    checkClientTrusted/checkServerTrusted logic
   * @param hostnameVerificationEnabled If true, this TrustManager should verify hostnames of peers
   *                                    when checking trust.
   * @param allowReverseDnsLookup       If true, we will fall back on reverse dns if resolving of
   *                                    host fails
   */
  HBaseTrustManager(X509ExtendedTrustManager x509ExtendedTrustManager,
    boolean hostnameVerificationEnabled, boolean allowReverseDnsLookup) {
    this.x509ExtendedTrustManager = x509ExtendedTrustManager;
    this.hostnameVerificationEnabled = hostnameVerificationEnabled;
    this.allowReverseDnsLookup = allowReverseDnsLookup;
    this.hostnameVerifier = new HBaseHostnameVerifier();
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return x509ExtendedTrustManager.getAcceptedIssuers();
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
    throws CertificateException {
    x509ExtendedTrustManager.checkClientTrusted(chain, authType, socket);
    if (hostnameVerificationEnabled) {
      performHostVerification(socket.getInetAddress(), chain[0]);
    }
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
    throws CertificateException {
    x509ExtendedTrustManager.checkServerTrusted(chain, authType, socket);
    if (hostnameVerificationEnabled) {
      performHostVerification(socket.getInetAddress(), chain[0]);
    }
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
    throws CertificateException {
    x509ExtendedTrustManager.checkClientTrusted(chain, authType, engine);
    if (hostnameVerificationEnabled && engine != null) {
      try {
        if (engine.getPeerHost() == null) {
          LOG.warn(
            "Cannot perform client hostname verification, because peer information is not available");
          return;
        }
        performHostVerification(InetAddress.getByName(engine.getPeerHost()), chain[0]);
      } catch (UnknownHostException e) {
        throw new CertificateException("Failed to verify host", e);
      }
    }
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
    throws CertificateException {
    x509ExtendedTrustManager.checkServerTrusted(chain, authType, engine);
    if (hostnameVerificationEnabled) {
      try {
        performHostVerification(InetAddress.getByName(engine.getPeerHost()), chain[0]);
      } catch (UnknownHostException e) {
        throw new CertificateException("Failed to verify host", e);
      }
    }
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType)
    throws CertificateException {
    x509ExtendedTrustManager.checkClientTrusted(chain, authType);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType)
    throws CertificateException {
    x509ExtendedTrustManager.checkServerTrusted(chain, authType);
  }

  /**
   * Compares peer's hostname with the one stored in the provided client certificate. Performs
   * verification with the help of provided HostnameVerifier.
   * @param inetAddress Peer's inet address.
   * @param certificate Peer's certificate
   * @throws CertificateException Thrown if the provided certificate doesn't match the peer
   *                              hostname.
   */
  private void performHostVerification(InetAddress inetAddress, X509Certificate certificate)
    throws CertificateException {
    String hostAddress = "";
    String hostName = "";
    try {
      hostAddress = inetAddress.getHostAddress();
      hostnameVerifier.verify(hostAddress, certificate);
    } catch (SSLException addressVerificationException) {
      // If we fail with hostAddress, we should try the hostname.
      // The inetAddress may have been created with a hostname, in which case getHostName() will
      // return quickly below. If not, a reverse lookup will happen, which can be expensive.
      // We provide the option to skip the reverse lookup if preferring to fail fast.

      // Handle logging here to aid debugging. The easiest way to check for an existing
      // hostname is through toString, see javadoc.
      String inetAddressString = inetAddress.toString();
      if (!inetAddressString.startsWith("/")) {
        LOG.debug(
          "Failed to verify host address: {}, but inetAddress {} has a hostname, trying that",
          hostAddress, inetAddressString, addressVerificationException);
      } else if (allowReverseDnsLookup) {
        LOG.debug(
          "Failed to verify host address: {}, attempting to verify host name with reverse dns",
          hostAddress, addressVerificationException);
      } else {
        LOG.debug("Failed to verify host address: {}, but reverse dns lookup is disabled",
          hostAddress, addressVerificationException);
        throw new CertificateException(
          "Failed to verify host address, and reverse lookup is disabled",
          addressVerificationException);
      }

      try {
        hostName = inetAddress.getHostName();
        hostnameVerifier.verify(hostName, certificate);
      } catch (SSLException hostnameVerificationException) {
        LOG.error("Failed to verify host address: {}", hostAddress, addressVerificationException);
        LOG.error("Failed to verify hostname: {}", hostName, hostnameVerificationException);
        throw new CertificateException("Failed to verify both host address and host name",
          hostnameVerificationException);
      }
    }
  }

}
