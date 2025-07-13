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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.X509CertSelector;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.CertPathTrustManagerParameters;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.exceptions.KeyManagerException;
import org.apache.hadoop.hbase.exceptions.SSLContextException;
import org.apache.hadoop.hbase.exceptions.TrustManagerException;
import org.apache.hadoop.hbase.exceptions.X509Exception;
import org.apache.hadoop.hbase.io.FileChangeWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.io.netty.handler.ssl.OpenSsl;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.SslContext;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.SslContextBuilder;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.SslProvider;

/**
 * Utility code for X509 handling Default cipher suites: Performance testing done by Facebook
 * engineers shows that on Intel x86_64 machines, Java9 performs better with GCM and Java8 performs
 * better with CBC, so these seem like reasonable defaults.
 * <p/>
 * This file has been copied from the Apache ZooKeeper project.
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/c74658d398cdc1d207aa296cb6e20de00faec03e/zookeeper-server/src/main/java/org/apache/zookeeper/common/X509Util.java">Base
 *      revision</a>
 */
@InterfaceAudience.Private
public final class X509Util {

  private static final Logger LOG = LoggerFactory.getLogger(X509Util.class);
  private static final char[] EMPTY_CHAR_ARRAY = new char[0];

  //
  // Common tls configs across both server and client
  //
  static final String CONFIG_PREFIX = "hbase.rpc.tls.";
  public static final String TLS_CONFIG_PROTOCOL = CONFIG_PREFIX + "protocol";
  public static final String TLS_CONFIG_KEYSTORE_LOCATION = CONFIG_PREFIX + "keystore.location";
  public static final String TLS_CONFIG_KEYSTORE_TYPE = CONFIG_PREFIX + "keystore.type";
  public static final String TLS_CONFIG_KEYSTORE_PASSWORD = CONFIG_PREFIX + "keystore.password";
  public static final String TLS_CONFIG_TRUSTSTORE_LOCATION = CONFIG_PREFIX + "truststore.location";
  public static final String TLS_CONFIG_TRUSTSTORE_TYPE = CONFIG_PREFIX + "truststore.type";
  public static final String TLS_CONFIG_TRUSTSTORE_PASSWORD = CONFIG_PREFIX + "truststore.password";
  public static final String TLS_CONFIG_CLR = CONFIG_PREFIX + "clr";
  public static final String TLS_CONFIG_OCSP = CONFIG_PREFIX + "ocsp";
  public static final String TLS_CONFIG_REVERSE_DNS_LOOKUP_ENABLED =
    CONFIG_PREFIX + "host-verification.reverse-dns.enabled";
  public static final String TLS_ENABLED_PROTOCOLS = CONFIG_PREFIX + "enabledProtocols";
  public static final String TLS_CIPHER_SUITES = CONFIG_PREFIX + "ciphersuites";
  public static final String TLS_CERT_RELOAD = CONFIG_PREFIX + "certReload";
  public static final String TLS_USE_OPENSSL = CONFIG_PREFIX + "useOpenSsl";
  public static final String DEFAULT_PROTOCOL = "TLSv1.2";

  //
  // Server-side specific configs
  //
  public static final String HBASE_SERVER_NETTY_TLS_ENABLED = "hbase.server.netty.tls.enabled";
  public static final String HBASE_SERVER_NETTY_TLS_CLIENT_AUTH_MODE =
    "hbase.server.netty.tls.client.auth.mode";
  public static final String HBASE_SERVER_NETTY_TLS_VERIFY_CLIENT_HOSTNAME =
    "hbase.server.netty.tls.verify.client.hostname";
  public static final String HBASE_SERVER_NETTY_TLS_SUPPORTPLAINTEXT =
    "hbase.server.netty.tls.supportplaintext";

  /**
   * Set the SSL wrapSize for netty. This is only a maximum wrap size. Buffers smaller than this
   * will not be consolidated, but buffers larger than this will be split into multiple wrap
   * buffers. The netty default of 16k is not great for hbase which tends to return larger payloads
   * than that, meaning most responses end up getting chunked up. This leads to more memory
   * contention in netty's PoolArena. See https://github.com/netty/netty/pull/13551
   */
  public static final String HBASE_SERVER_NETTY_TLS_WRAP_SIZE = "hbase.server.netty.tls.wrapSize";
  public static final int DEFAULT_HBASE_SERVER_NETTY_TLS_WRAP_SIZE = 1024 * 1024;
  //
  // Client-side specific configs
  //
  public static final String HBASE_CLIENT_NETTY_TLS_ENABLED = "hbase.client.netty.tls.enabled";
  public static final String HBASE_CLIENT_NETTY_TLS_VERIFY_SERVER_HOSTNAME =
    "hbase.client.netty.tls.verify.server.hostname";
  public static final String HBASE_CLIENT_NETTY_TLS_HANDSHAKETIMEOUT =
    "hbase.client.netty.tls.handshaketimeout";
  public static final int DEFAULT_HANDSHAKE_DETECTION_TIMEOUT_MILLIS = 5000;

  public static final String HBASE_TLS_FILEPOLL_INTERVAL_MILLIS =
    CONFIG_PREFIX + "filepoll.interval.millis";
  // 1 minute
  private static final long DEFAULT_FILE_POLL_INTERVAL = Duration.ofSeconds(60).toMillis();

  /**
   * Enum specifying the client auth requirement of server-side TLS sockets created by this
   * X509Util.
   * <ul>
   * <li>NONE - do not request a client certificate.</li>
   * <li>WANT - request a client certificate, but allow anonymous clients to connect.</li>
   * <li>NEED - require a client certificate, disconnect anonymous clients.</li>
   * </ul>
   * If the config property is not set, the default value is NEED.
   */
  public enum ClientAuth {
    NONE(org.apache.hbase.thirdparty.io.netty.handler.ssl.ClientAuth.NONE),
    WANT(org.apache.hbase.thirdparty.io.netty.handler.ssl.ClientAuth.OPTIONAL),
    NEED(org.apache.hbase.thirdparty.io.netty.handler.ssl.ClientAuth.REQUIRE);

    private final org.apache.hbase.thirdparty.io.netty.handler.ssl.ClientAuth nettyAuth;

    ClientAuth(org.apache.hbase.thirdparty.io.netty.handler.ssl.ClientAuth nettyAuth) {
      this.nettyAuth = nettyAuth;
    }

    /**
     * Converts a property value to a ClientAuth enum. If the input string is empty or null, returns
     * <code>ClientAuth.NEED</code>.
     * @param prop the property string.
     * @return the ClientAuth.
     * @throws IllegalArgumentException if the property value is not "NONE", "WANT", "NEED", or
     *                                  empty/null.
     */
    public static ClientAuth fromPropertyValue(String prop) {
      if (prop == null || prop.length() == 0) {
        return NEED;
      }
      return ClientAuth.valueOf(prop.toUpperCase());
    }

    public org.apache.hbase.thirdparty.io.netty.handler.ssl.ClientAuth toNettyClientAuth() {
      return nettyAuth;
    }
  }

  private X509Util() {
    // disabled
  }

  public static SslContext createSslContextForClient(Configuration config)
    throws X509Exception, IOException {

    SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();

    configureOpenSslIfAvailable(sslContextBuilder, config);
    String keyStoreLocation = config.get(TLS_CONFIG_KEYSTORE_LOCATION, "");
    char[] keyStorePassword = config.getPassword(TLS_CONFIG_KEYSTORE_PASSWORD);
    String keyStoreType = config.get(TLS_CONFIG_KEYSTORE_TYPE, "");

    if (keyStoreLocation.isEmpty()) {
      LOG.warn(TLS_CONFIG_KEYSTORE_LOCATION + " not specified");
    } else {
      sslContextBuilder
        .keyManager(createKeyManager(keyStoreLocation, keyStorePassword, keyStoreType));
    }

    String trustStoreLocation = config.get(TLS_CONFIG_TRUSTSTORE_LOCATION, "");
    char[] trustStorePassword = config.getPassword(TLS_CONFIG_TRUSTSTORE_PASSWORD);
    String trustStoreType = config.get(TLS_CONFIG_TRUSTSTORE_TYPE, "");

    boolean sslCrlEnabled = config.getBoolean(TLS_CONFIG_CLR, false);
    boolean sslOcspEnabled = config.getBoolean(TLS_CONFIG_OCSP, false);

    boolean verifyServerHostname =
      config.getBoolean(HBASE_CLIENT_NETTY_TLS_VERIFY_SERVER_HOSTNAME, true);
    boolean allowReverseDnsLookup = config.getBoolean(TLS_CONFIG_REVERSE_DNS_LOOKUP_ENABLED, true);

    if (trustStoreLocation.isEmpty()) {
      LOG.warn(TLS_CONFIG_TRUSTSTORE_LOCATION + " not specified");
    } else {
      sslContextBuilder
        .trustManager(createTrustManager(trustStoreLocation, trustStorePassword, trustStoreType,
          sslCrlEnabled, sslOcspEnabled, verifyServerHostname, allowReverseDnsLookup));
    }

    sslContextBuilder.enableOcsp(sslOcspEnabled);
    sslContextBuilder.protocols(getEnabledProtocols(config));
    String[] cipherSuites = getCipherSuites(config);
    if (cipherSuites != null) {
      sslContextBuilder.ciphers(Arrays.asList(cipherSuites));
    }

    return sslContextBuilder.build();
  }

  /**
   * Adds SslProvider.OPENSSL if OpenSsl is available and enabled. In order to make it available,
   * one must ensure that a properly shaded netty-tcnative is on the classpath. Properly shaded
   * means relocated to be prefixed with "org.apache.hbase.thirdparty" like the rest of the netty
   * classes. We make available org.apache.hbase:hbase-openssl as a convenience module which one can
   * use to pull in a shaded netty-tcnative statically linked against boringssl.
   */
  private static boolean configureOpenSslIfAvailable(SslContextBuilder sslContextBuilder,
    Configuration conf) {
    if (OpenSsl.isAvailable() && conf.getBoolean(TLS_USE_OPENSSL, true)) {
      LOG.debug("Using netty-tcnative to accelerate SSL handling");
      sslContextBuilder.sslProvider(SslProvider.OPENSSL);
      return true;
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using default JDK SSL provider because netty-tcnative is not {}",
          OpenSsl.isAvailable() ? "enabled" : "available");
      }
      sslContextBuilder.sslProvider(SslProvider.JDK);
      return false;
    }
  }

  public static SslContext createSslContextForServer(Configuration config)
    throws X509Exception, IOException {
    String keyStoreLocation = config.get(TLS_CONFIG_KEYSTORE_LOCATION, "");
    char[] keyStorePassword = config.getPassword(TLS_CONFIG_KEYSTORE_PASSWORD);
    String keyStoreType = config.get(TLS_CONFIG_KEYSTORE_TYPE, "");

    if (keyStoreLocation.isEmpty()) {
      throw new SSLContextException(
        "Keystore is required for SSL server: " + TLS_CONFIG_KEYSTORE_LOCATION);
    }

    SslContextBuilder sslContextBuilder;
    sslContextBuilder = SslContextBuilder
      .forServer(createKeyManager(keyStoreLocation, keyStorePassword, keyStoreType));

    configureOpenSslIfAvailable(sslContextBuilder, config);
    String trustStoreLocation = config.get(TLS_CONFIG_TRUSTSTORE_LOCATION, "");
    char[] trustStorePassword = config.getPassword(TLS_CONFIG_TRUSTSTORE_PASSWORD);
    String trustStoreType = config.get(TLS_CONFIG_TRUSTSTORE_TYPE, "");

    boolean sslCrlEnabled = config.getBoolean(TLS_CONFIG_CLR, false);
    boolean sslOcspEnabled = config.getBoolean(TLS_CONFIG_OCSP, false);

    ClientAuth clientAuth =
      ClientAuth.fromPropertyValue(config.get(HBASE_SERVER_NETTY_TLS_CLIENT_AUTH_MODE));
    boolean verifyClientHostname =
      config.getBoolean(HBASE_SERVER_NETTY_TLS_VERIFY_CLIENT_HOSTNAME, true);
    boolean allowReverseDnsLookup = config.getBoolean(TLS_CONFIG_REVERSE_DNS_LOOKUP_ENABLED, true);

    if (trustStoreLocation.isEmpty()) {
      LOG.warn(TLS_CONFIG_TRUSTSTORE_LOCATION + " not specified");
    } else {
      sslContextBuilder
        .trustManager(createTrustManager(trustStoreLocation, trustStorePassword, trustStoreType,
          sslCrlEnabled, sslOcspEnabled, verifyClientHostname, allowReverseDnsLookup));
    }

    sslContextBuilder.enableOcsp(sslOcspEnabled);
    sslContextBuilder.protocols(getEnabledProtocols(config));
    String[] cipherSuites = getCipherSuites(config);
    if (cipherSuites != null) {
      sslContextBuilder.ciphers(Arrays.asList(cipherSuites));
    }
    sslContextBuilder.clientAuth(clientAuth.toNettyClientAuth());

    return sslContextBuilder.build();
  }

  /**
   * Creates a key manager by loading the key store from the given file of the given type,
   * optionally decrypting it using the given password.
   * @param keyStoreLocation the location of the key store file.
   * @param keyStorePassword optional password to decrypt the key store. If empty, assumes the key
   *                         store is not encrypted.
   * @param keyStoreType     must be JKS, PEM, PKCS12, BCFKS or null. If null, attempts to
   *                         autodetect the key store type from the file extension (e.g. .jks /
   *                         .pem).
   * @return the key manager.
   * @throws KeyManagerException if something goes wrong.
   */
  static X509KeyManager createKeyManager(String keyStoreLocation, char[] keyStorePassword,
    String keyStoreType) throws KeyManagerException {

    if (keyStorePassword == null) {
      keyStorePassword = EMPTY_CHAR_ARRAY;
    }

    try {
      KeyStoreFileType storeFileType =
        KeyStoreFileType.fromPropertyValueOrFileName(keyStoreType, keyStoreLocation);
      KeyStore ks = FileKeyStoreLoaderBuilderProvider.getBuilderForKeyStoreFileType(storeFileType)
        .setKeyStorePath(keyStoreLocation).setKeyStorePassword(keyStorePassword).build()
        .loadKeyStore();

      KeyManagerFactory kmf = KeyManagerFactory.getInstance("PKIX");
      kmf.init(ks, keyStorePassword);

      for (KeyManager km : kmf.getKeyManagers()) {
        if (km instanceof X509KeyManager) {
          return (X509KeyManager) km;
        }
      }
      throw new KeyManagerException("Couldn't find X509KeyManager");
    } catch (IOException | GeneralSecurityException | IllegalArgumentException e) {
      throw new KeyManagerException(e);
    }
  }

  /**
   * Creates a trust manager by loading the trust store from the given file of the given type,
   * optionally decrypting it using the given password.
   * @param trustStoreLocation    the location of the trust store file.
   * @param trustStorePassword    optional password to decrypt the trust store (only applies to JKS
   *                              trust stores). If empty, assumes the trust store is not encrypted.
   * @param trustStoreType        must be JKS, PEM, PKCS12, BCFKS or null. If null, attempts to
   *                              autodetect the trust store type from the file extension (e.g. .jks
   *                              / .pem).
   * @param crlEnabled            enable CRL (certificate revocation list) checks.
   * @param ocspEnabled           enable OCSP (online certificate status protocol) checks.
   * @param verifyHostName        if true, ssl peer hostname must match name in certificate
   * @param allowReverseDnsLookup if true, allow falling back to reverse dns lookup in verifying
   *                              hostname
   * @return the trust manager.
   * @throws TrustManagerException if something goes wrong.
   */
  static X509TrustManager createTrustManager(String trustStoreLocation, char[] trustStorePassword,
    String trustStoreType, boolean crlEnabled, boolean ocspEnabled, boolean verifyHostName,
    boolean allowReverseDnsLookup) throws TrustManagerException {

    if (trustStorePassword == null) {
      trustStorePassword = EMPTY_CHAR_ARRAY;
    }

    try {
      KeyStoreFileType storeFileType =
        KeyStoreFileType.fromPropertyValueOrFileName(trustStoreType, trustStoreLocation);
      KeyStore ts = FileKeyStoreLoaderBuilderProvider.getBuilderForKeyStoreFileType(storeFileType)
        .setTrustStorePath(trustStoreLocation).setTrustStorePassword(trustStorePassword).build()
        .loadTrustStore();

      PKIXBuilderParameters pbParams = new PKIXBuilderParameters(ts, new X509CertSelector());
      if (crlEnabled || ocspEnabled) {
        pbParams.setRevocationEnabled(true);
        System.setProperty("com.sun.net.ssl.checkRevocation", "true");
        if (crlEnabled) {
          System.setProperty("com.sun.security.enableCRLDP", "true");
        }
        if (ocspEnabled) {
          Security.setProperty("ocsp.enable", "true");
        }
      } else {
        pbParams.setRevocationEnabled(false);
      }

      // Revocation checking is only supported with the PKIX algorithm
      TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
      tmf.init(new CertPathTrustManagerParameters(pbParams));

      for (final TrustManager tm : tmf.getTrustManagers()) {
        if (tm instanceof X509ExtendedTrustManager) {
          return new HBaseTrustManager((X509ExtendedTrustManager) tm, verifyHostName,
            allowReverseDnsLookup);
        }
      }
      throw new TrustManagerException("Couldn't find X509TrustManager");
    } catch (IOException | GeneralSecurityException | IllegalArgumentException e) {
      throw new TrustManagerException(e);
    }
  }

  private static String[] getEnabledProtocols(Configuration config) {
    String enabledProtocolsInput = config.get(TLS_ENABLED_PROTOCOLS);
    if (enabledProtocolsInput == null) {
      return new String[] { config.get(TLS_CONFIG_PROTOCOL, DEFAULT_PROTOCOL) };
    }
    return enabledProtocolsInput.split(",");
  }

  private static String[] getCipherSuites(Configuration config) {
    String cipherSuitesInput = config.get(TLS_CIPHER_SUITES);
    if (cipherSuitesInput == null) {
      return null;
    } else {
      return cipherSuitesInput.split(",");
    }
  }

  /**
   * Enable certificate file reloading by creating FileWatchers for keystore and truststore.
   * AtomicReferences will be set with the new instances. resetContext - if not null - will be
   * called when the file has been modified.
   * @param keystoreWatcher   Reference to keystoreFileWatcher.
   * @param trustStoreWatcher Reference to truststoreFileWatcher.
   * @param resetContext      Callback for file changes.
   */
  public static void enableCertFileReloading(Configuration config,
    AtomicReference<FileChangeWatcher> keystoreWatcher,
    AtomicReference<FileChangeWatcher> trustStoreWatcher, Runnable resetContext)
    throws IOException {
    String keyStoreLocation = config.get(TLS_CONFIG_KEYSTORE_LOCATION, "");
    keystoreWatcher.set(newFileChangeWatcher(config, keyStoreLocation, resetContext));
    String trustStoreLocation = config.get(TLS_CONFIG_TRUSTSTORE_LOCATION, "");
    // we are using the same callback for both. there's no reason to kick off two
    // threads if keystore/truststore are both at the same location
    if (!keyStoreLocation.equals(trustStoreLocation)) {
      trustStoreWatcher.set(newFileChangeWatcher(config, trustStoreLocation, resetContext));
    }
  }

  private static FileChangeWatcher newFileChangeWatcher(Configuration config, String fileLocation,
    Runnable resetContext) throws IOException {
    if (fileLocation == null || fileLocation.isEmpty() || resetContext == null) {
      return null;
    }
    final Path filePath = Paths.get(fileLocation).toAbsolutePath();
    FileChangeWatcher fileChangeWatcher =
      new FileChangeWatcher(filePath, Objects.toString(filePath.getFileName()),
        Duration
          .ofMillis(config.getLong(HBASE_TLS_FILEPOLL_INTERVAL_MILLIS, DEFAULT_FILE_POLL_INTERVAL)),
        watchEventFilePath -> handleWatchEvent(watchEventFilePath, resetContext));
    fileChangeWatcher.start();
    return fileChangeWatcher;
  }

  /**
   * Handler for watch events that let us know a file we may care about has changed on disk.
   */
  private static void handleWatchEvent(Path filePath, Runnable resetContext) {
    LOG.info("Attempting to reset default SSL context after receiving watch event on file {}",
      filePath);
    resetContext.run();
  }
}
