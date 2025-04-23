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

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.OperatorCreationException;

/**
 * This class simplifies the creation of certificates and private keys for SSL/TLS connections.
 * <p/>
 * This file has been copied from the Apache ZooKeeper project.
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/c74658d398cdc1d207aa296cb6e20de00faec03e/zookeeper-server/src/test/java/org/apache/zookeeper/common/X509TestContext.java">Base
 *      revision</a>
 */
@InterfaceAudience.Private
public final class X509TestContext {

  private static final String TRUST_STORE_PREFIX = "hbase_test_ca";
  private static final String KEY_STORE_PREFIX = "hbase_test_key";

  private final File tempDir;
  private final Configuration conf;

  private X509Certificate trustStoreCertificate;
  private final char[] trustStorePassword;
  private KeyPair trustStoreKeyPair;
  private File trustStoreJksFile;
  private File trustStorePemFile;
  private File trustStorePkcs12File;
  private File trustStoreBcfksFile;

  private KeyPair keyStoreKeyPair;
  private X509Certificate keyStoreCertificate;
  private final char[] keyStorePassword;
  private File keyStoreJksFile;
  private File keyStorePemFile;
  private File keyStorePkcs12File;
  private File keyStoreBcfksFile;

  /**
   * Constructor is intentionally private, use the Builder class instead.
   * @param conf               the configuration
   * @param tempDir            the directory in which key store and trust store temp files will be
   *                           written.
   * @param trustStoreKeyPair  the key pair for the trust store.
   * @param trustStorePassword the password to protect a JKS trust store (ignored for PEM trust
   *                           stores).
   * @param keyStoreKeyPair    the key pair for the key store.
   * @param keyStorePassword   the password to protect the key store private key.
   */
  private X509TestContext(Configuration conf, File tempDir, KeyPair trustStoreKeyPair,
    char[] trustStorePassword, KeyPair keyStoreKeyPair, char[] keyStorePassword)
    throws IOException, GeneralSecurityException, OperatorCreationException {
    if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
      throw new IllegalStateException("BC Security provider was not found");
    }
    this.conf = conf;
    this.tempDir = requireNonNull(tempDir);
    if (!tempDir.isDirectory()) {
      throw new IllegalArgumentException("Not a directory: " + tempDir);
    }

    this.trustStoreKeyPair = trustStoreKeyPair;
    this.trustStorePassword = requireNonNull(trustStorePassword);
    this.keyStoreKeyPair = requireNonNull(keyStoreKeyPair);
    this.keyStorePassword = requireNonNull(keyStorePassword);

    createCertificates();

    trustStorePkcs12File = null;
    trustStorePemFile = null;
    trustStoreJksFile = null;
    keyStorePkcs12File = null;
    keyStorePemFile = null;
    keyStoreJksFile = null;
  }

  /**
   * Used by {@link #cloneWithNewKeystoreCert(X509Certificate)}. Should set all fields except
   * generated keystore path fields
   */
  private X509TestContext(File tempDir, Configuration conf, X509Certificate trustStoreCertificate,
    char[] trustStorePassword, KeyPair trustStoreKeyPair, File trustStoreJksFile,
    File trustStorePemFile, File trustStorePkcs12File, KeyPair keyStoreKeyPair,
    char[] keyStorePassword, X509Certificate keyStoreCertificate) {
    this.tempDir = tempDir;
    this.conf = conf;
    this.trustStoreCertificate = trustStoreCertificate;
    this.trustStorePassword = trustStorePassword;
    this.trustStoreKeyPair = trustStoreKeyPair;
    this.trustStoreJksFile = trustStoreJksFile;
    this.trustStorePemFile = trustStorePemFile;
    this.trustStorePkcs12File = trustStorePkcs12File;
    this.keyStoreKeyPair = keyStoreKeyPair;
    this.keyStoreCertificate = keyStoreCertificate;
    this.keyStorePassword = keyStorePassword;
    keyStorePkcs12File = null;
    keyStorePemFile = null;
    keyStoreJksFile = null;
  }

  /**
   * Generates a new certificate using this context's CA and keystoreKeyPair. By default, the cert
   * will have localhost in the subjectAltNames. This can be overridden by passing one or more
   * string arguments after the cert name. The expectation for those arguments is that they are
   * valid DNS names.
   */
  public X509Certificate newCert(X500Name name, String... subjectAltNames)
    throws GeneralSecurityException, IOException, OperatorCreationException {
    if (subjectAltNames.length == 0) {
      return X509TestHelpers.newCert(trustStoreCertificate, trustStoreKeyPair, name,
        keyStoreKeyPair.getPublic());
    }
    GeneralName[] names = new GeneralName[subjectAltNames.length];
    for (int i = 0; i < subjectAltNames.length; i++) {
      names[i] = new GeneralName(GeneralName.dNSName, subjectAltNames[i]);
    }
    return X509TestHelpers.newCert(trustStoreCertificate, trustStoreKeyPair, name,
      keyStoreKeyPair.getPublic(), new GeneralNames(names));
  }

  public File getTempDir() {
    return tempDir;
  }

  public char[] getTrustStorePassword() {
    return trustStorePassword;
  }

  /**
   * Returns the path to the trust store file in the given format (JKS or PEM). Note that the file
   * is created lazily, the first time this method is called. The trust store file is temporary and
   * will be deleted on exit.
   * @param storeFileType the store file type (JKS or PEM).
   * @return the path to the trust store file.
   * @throws IOException if there is an error creating the trust store file.
   */
  public File getTrustStoreFile(KeyStoreFileType storeFileType) throws IOException {
    switch (storeFileType) {
      case JKS:
        return getTrustStoreJksFile();
      case PEM:
        return getTrustStorePemFile();
      case PKCS12:
        return getTrustStorePkcs12File();
      case BCFKS:
        return getTrustStoreBcfksFile();
      default:
        throw new IllegalArgumentException("Invalid trust store type: " + storeFileType
          + ", must be one of: " + Arrays.toString(KeyStoreFileType.values()));
    }
  }

  private File getTrustStoreJksFile() throws IOException {
    if (trustStoreJksFile == null) {
      trustStoreJksFile = File.createTempFile(TRUST_STORE_PREFIX,
        KeyStoreFileType.JKS.getDefaultFileExtension(), tempDir);
      trustStoreJksFile.deleteOnExit();
      generateTrustStoreJksFile();
    }
    return trustStoreJksFile;
  }

  private void generateTrustStoreJksFile() throws IOException {
    try (final FileOutputStream trustStoreOutputStream = new FileOutputStream(trustStoreJksFile)) {
      byte[] bytes =
        X509TestHelpers.certToJavaTrustStoreBytes(trustStoreCertificate, trustStorePassword);
      trustStoreOutputStream.write(bytes);
      trustStoreOutputStream.flush();
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
  }

  private File getTrustStorePemFile() throws IOException {
    if (trustStorePemFile == null) {
      trustStorePemFile = File.createTempFile(TRUST_STORE_PREFIX,
        KeyStoreFileType.PEM.getDefaultFileExtension(), tempDir);
      trustStorePemFile.deleteOnExit();
      generateTrustStorePemFile();
    }
    return trustStorePemFile;
  }

  private void generateTrustStorePemFile() throws IOException {
    FileUtils.writeStringToFile(trustStorePemFile,
      X509TestHelpers.pemEncodeX509Certificate(trustStoreCertificate), StandardCharsets.US_ASCII,
      false);
  }

  private File getTrustStorePkcs12File() throws IOException {
    if (trustStorePkcs12File == null) {
      trustStorePkcs12File = File.createTempFile(TRUST_STORE_PREFIX,
        KeyStoreFileType.PKCS12.getDefaultFileExtension(), tempDir);
      trustStorePkcs12File.deleteOnExit();
      generateTrustStorePkcs12File();
    }
    return trustStorePkcs12File;
  }

  private void generateTrustStorePkcs12File() throws IOException {
    try (
      final FileOutputStream trustStoreOutputStream = new FileOutputStream(trustStorePkcs12File)) {
      byte[] bytes =
        X509TestHelpers.certToPKCS12TrustStoreBytes(trustStoreCertificate, trustStorePassword);
      trustStoreOutputStream.write(bytes);
      trustStoreOutputStream.flush();
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
  }

  private File getTrustStoreBcfksFile() throws IOException {
    if (trustStoreBcfksFile == null) {
      trustStoreBcfksFile = File.createTempFile(TRUST_STORE_PREFIX,
        KeyStoreFileType.BCFKS.getDefaultFileExtension(), tempDir);
      trustStoreBcfksFile.deleteOnExit();
      generateTrustStoreBcfksFile();
    }
    return trustStoreBcfksFile;
  }

  private void generateTrustStoreBcfksFile() throws IOException {
    try (
      final FileOutputStream trustStoreOutputStream = new FileOutputStream(trustStoreBcfksFile)) {
      byte[] bytes =
        X509TestHelpers.certToBCFKSTrustStoreBytes(trustStoreCertificate, trustStorePassword);
      trustStoreOutputStream.write(bytes);
      trustStoreOutputStream.flush();
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
  }

  public X509Certificate getKeyStoreCertificate() {
    return keyStoreCertificate;
  }

  public char[] getKeyStorePassword() {
    return keyStorePassword;
  }

  public boolean isKeyStoreEncrypted() {
    return keyStorePassword != null;
  }

  public Configuration getConf() {
    return conf;
  }

  /**
   * Returns the path to the key store file in the given format (JKS, PEM, ...). Note that the file
   * is created lazily, the first time this method is called. The key store file is temporary and
   * will be deleted on exit.
   * @param storeFileType the store file type (JKS, PEM, ...).
   * @return the path to the key store file.
   * @throws IOException if there is an error creating the key store file.
   */
  public File getKeyStoreFile(KeyStoreFileType storeFileType) throws IOException {
    switch (storeFileType) {
      case JKS:
        return getKeyStoreJksFile();
      case PEM:
        return getKeyStorePemFile();
      case PKCS12:
        return getKeyStorePkcs12File();
      case BCFKS:
        return getKeyStoreBcfksFile();
      default:
        throw new IllegalArgumentException("Invalid key store type: " + storeFileType
          + ", must be one of: " + Arrays.toString(KeyStoreFileType.values()));
    }
  }

  private File getKeyStoreJksFile() throws IOException {
    if (keyStoreJksFile == null) {
      keyStoreJksFile = File.createTempFile(KEY_STORE_PREFIX,
        KeyStoreFileType.JKS.getDefaultFileExtension(), tempDir);
      keyStoreJksFile.deleteOnExit();
      generateKeyStoreJksFile();
    }
    return keyStoreJksFile;
  }

  private void generateKeyStoreJksFile() throws IOException {
    try (final FileOutputStream keyStoreOutputStream = new FileOutputStream(keyStoreJksFile)) {
      byte[] bytes = X509TestHelpers.certAndPrivateKeyToJavaKeyStoreBytes(keyStoreCertificate,
        keyStoreKeyPair.getPrivate(), keyStorePassword);
      keyStoreOutputStream.write(bytes);
      keyStoreOutputStream.flush();
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
  }

  private File getKeyStorePemFile() throws IOException {
    if (keyStorePemFile == null) {
      try {
        keyStorePemFile = File.createTempFile(KEY_STORE_PREFIX,
          KeyStoreFileType.PEM.getDefaultFileExtension(), tempDir);
        keyStorePemFile.deleteOnExit();
        generateKeyStorePemFile();
      } catch (OperatorCreationException e) {
        throw new IOException(e);
      }
    }
    return keyStorePemFile;
  }

  private void generateKeyStorePemFile() throws IOException, OperatorCreationException {
    FileUtils.writeStringToFile(keyStorePemFile,
      X509TestHelpers.pemEncodeCertAndPrivateKey(keyStoreCertificate, keyStoreKeyPair.getPrivate(),
        keyStorePassword),
      StandardCharsets.US_ASCII, false);
  }

  private File getKeyStorePkcs12File() throws IOException {
    if (keyStorePkcs12File == null) {
      keyStorePkcs12File = File.createTempFile(KEY_STORE_PREFIX,
        KeyStoreFileType.PKCS12.getDefaultFileExtension(), tempDir);
      keyStorePkcs12File.deleteOnExit();
      generateKeyStorePkcs12File();
    }
    return keyStorePkcs12File;
  }

  private void generateKeyStorePkcs12File() throws IOException {
    try (final FileOutputStream keyStoreOutputStream = new FileOutputStream(keyStorePkcs12File)) {
      byte[] bytes = X509TestHelpers.certAndPrivateKeyToPKCS12Bytes(keyStoreCertificate,
        keyStoreKeyPair.getPrivate(), keyStorePassword);
      keyStoreOutputStream.write(bytes);
      keyStoreOutputStream.flush();
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
  }

  private File getKeyStoreBcfksFile() throws IOException {
    if (keyStoreBcfksFile == null) {
      keyStoreBcfksFile = File.createTempFile(KEY_STORE_PREFIX,
        KeyStoreFileType.BCFKS.getDefaultFileExtension(), tempDir);
      keyStoreBcfksFile.deleteOnExit();
      generateKeyStoreBcfksFile();
    }
    return keyStoreBcfksFile;
  }

  private void generateKeyStoreBcfksFile() throws IOException {
    try (final FileOutputStream keyStoreOutputStream = new FileOutputStream(keyStoreBcfksFile)) {
      byte[] bytes = X509TestHelpers.certAndPrivateKeyToBCFKSBytes(keyStoreCertificate,
        keyStoreKeyPair.getPrivate(), keyStorePassword);
      keyStoreOutputStream.write(bytes);
      keyStoreOutputStream.flush();
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
  }

  /**
   * Sets the SSL system properties such that the given X509Util object can be used to create SSL
   * Contexts that will use the trust store and key store files created by this test context.
   * Example usage:
   *
   * <pre>
   *     X509TestContext testContext = ...; // create the test context
   *     X509Util x509Util = new QuorumX509Util();
   *     testContext.setSystemProperties(x509Util, KeyStoreFileType.JKS, KeyStoreFileType.JKS);
   *     // The returned context will use the key store and trust store created by the test context.
   *     SSLContext ctx = x509Util.getDefaultSSLContext();
   * </pre>
   *
   * @param keyStoreFileType   the store file type to use for the key store (JKS, PEM, ...).
   * @param trustStoreFileType the store file type to use for the trust store (JKS, PEM, ...).
   * @throws IOException if there is an error creating the key store file or trust store file.
   */
  public void setConfigurations(KeyStoreFileType keyStoreFileType,
    KeyStoreFileType trustStoreFileType) throws IOException {
    setKeystoreConfigurations(keyStoreFileType, conf);
    conf.set(X509Util.TLS_CONFIG_TRUSTSTORE_LOCATION,
      this.getTrustStoreFile(trustStoreFileType).getAbsolutePath());
    conf.set(X509Util.TLS_CONFIG_TRUSTSTORE_PASSWORD, String.valueOf(this.getTrustStorePassword()));
    conf.set(X509Util.TLS_CONFIG_TRUSTSTORE_TYPE, trustStoreFileType.getPropertyValue());
  }

  /**
   * Sets the KeyStore-related SSL system properties onto the given Configuration such that X509Util
   * can be used to create SSL Contexts using that KeyStore. This can be used in special
   * circumstances to inject a "bad" certificate where the keystore doesn't match the CA in the
   * truststore. Or use it to create a connection without a truststore.
   * @see #setConfigurations(KeyStoreFileType, KeyStoreFileType) which sets both keystore and
   *      truststore and is more applicable to general use.
   */
  public void setKeystoreConfigurations(KeyStoreFileType keyStoreFileType, Configuration confToSet)
    throws IOException {

    confToSet.set(X509Util.TLS_CONFIG_KEYSTORE_LOCATION,
      this.getKeyStoreFile(keyStoreFileType).getAbsolutePath());
    confToSet.set(X509Util.TLS_CONFIG_KEYSTORE_PASSWORD,
      String.valueOf(this.getKeyStorePassword()));
    confToSet.set(X509Util.TLS_CONFIG_KEYSTORE_TYPE, keyStoreFileType.getPropertyValue());
  }

  public void clearConfigurations() {
    conf.unset(X509Util.TLS_CONFIG_KEYSTORE_LOCATION);
    conf.unset(X509Util.TLS_CONFIG_KEYSTORE_PASSWORD);
    conf.unset(X509Util.TLS_CONFIG_KEYSTORE_TYPE);
    conf.unset(X509Util.TLS_CONFIG_TRUSTSTORE_LOCATION);
    conf.unset(X509Util.TLS_CONFIG_TRUSTSTORE_PASSWORD);
    conf.unset(X509Util.TLS_CONFIG_TRUSTSTORE_TYPE);
  }

  /**
   * Creates a clone of the current context, but injecting the passed certificate as the KeyStore
   * cert. The new context's keystore path fields are nulled, so the next call to
   * {@link #setConfigurations(KeyStoreFileType, KeyStoreFileType)},
   * {@link #setKeystoreConfigurations(KeyStoreFileType, Configuration)} , or
   * {@link #getKeyStoreFile(KeyStoreFileType)} will create a new keystore with this certificate in
   * place.
   * @param cert the cert to replace
   */
  public X509TestContext cloneWithNewKeystoreCert(X509Certificate cert) {
    return new X509TestContext(tempDir, conf, trustStoreCertificate, trustStorePassword,
      trustStoreKeyPair, trustStoreJksFile, trustStorePemFile, trustStorePkcs12File,
      keyStoreKeyPair, keyStorePassword, cert);
  }

  public void regenerateStores(X509KeyType keyStoreKeyType, X509KeyType trustStoreKeyType,
    KeyStoreFileType keyStoreFileType, KeyStoreFileType trustStoreFileType,
    String... subjectAltNames)
    throws GeneralSecurityException, IOException, OperatorCreationException {

    trustStoreKeyPair = X509TestHelpers.generateKeyPair(trustStoreKeyType);
    keyStoreKeyPair = X509TestHelpers.generateKeyPair(keyStoreKeyType);
    createCertificates(subjectAltNames);

    switch (keyStoreFileType) {
      case JKS:
        generateKeyStoreJksFile();
        break;
      case PEM:
        generateKeyStorePemFile();
        break;
      case BCFKS:
        generateKeyStoreBcfksFile();
        break;
      case PKCS12:
        generateKeyStorePkcs12File();
        break;
    }

    switch (trustStoreFileType) {
      case JKS:
        generateTrustStoreJksFile();
        break;
      case PEM:
        generateTrustStorePemFile();
        break;
      case PKCS12:
        generateTrustStorePkcs12File();
        break;
      case BCFKS:
        generateTrustStoreBcfksFile();
        break;
    }
  }

  private void createCertificates(String... subjectAltNames)
    throws GeneralSecurityException, IOException, OperatorCreationException {
    X500NameBuilder caNameBuilder = new X500NameBuilder(BCStyle.INSTANCE);
    caNameBuilder.addRDN(BCStyle.CN,
      MethodHandles.lookup().lookupClass().getCanonicalName() + " Root CA");
    trustStoreCertificate =
      X509TestHelpers.newSelfSignedCACert(caNameBuilder.build(), trustStoreKeyPair);

    X500NameBuilder nameBuilder = new X500NameBuilder(BCStyle.INSTANCE);
    nameBuilder.addRDN(BCStyle.CN,
      MethodHandles.lookup().lookupClass().getCanonicalName() + " Zookeeper Test");
    keyStoreCertificate = newCert(nameBuilder.build(), subjectAltNames);
  }

  /**
   * Builder class, used for creating new instances of X509TestContext.
   */
  public static class Builder {

    private final Configuration conf;
    private File tempDir;
    private X509KeyType trustStoreKeyType;
    private char[] trustStorePassword;
    private X509KeyType keyStoreKeyType;
    private char[] keyStorePassword;

    /**
     * Creates an empty builder with the given Configuration.
     */
    public Builder(Configuration conf) {
      this.conf = conf;
      trustStoreKeyType = X509KeyType.EC;
      keyStoreKeyType = X509KeyType.EC;
    }

    /**
     * Builds a new X509TestContext from this builder.
     * @return a new X509TestContext
     */
    public X509TestContext build()
      throws IOException, GeneralSecurityException, OperatorCreationException {
      KeyPair trustStoreKeyPair = X509TestHelpers.generateKeyPair(trustStoreKeyType);
      KeyPair keyStoreKeyPair = X509TestHelpers.generateKeyPair(keyStoreKeyType);
      return new X509TestContext(conf, tempDir, trustStoreKeyPair, trustStorePassword,
        keyStoreKeyPair, keyStorePassword);
    }

    /**
     * Sets the temporary directory. Certificate and private key files will be created in this
     * directory.
     * @param tempDir the temp directory.
     * @return this Builder.
     */
    public Builder setTempDir(File tempDir) {
      this.tempDir = tempDir;
      return this;
    }

    /**
     * Sets the trust store key type. The CA key generated for the test context will be of this
     * type.
     * @param keyType the key type.
     * @return this Builder.
     */
    public Builder setTrustStoreKeyType(X509KeyType keyType) {
      trustStoreKeyType = keyType;
      return this;
    }

    /**
     * Sets the trust store password. Ignored for PEM trust stores, JKS trust stores will be
     * encrypted with this password.
     * @param password the password.
     * @return this Builder.
     */
    public Builder setTrustStorePassword(char[] password) {
      trustStorePassword = password;
      return this;
    }

    /**
     * Sets the key store key type. The private key generated for the test context will be of this
     * type.
     * @param keyType the key type.
     * @return this Builder.
     */
    public Builder setKeyStoreKeyType(X509KeyType keyType) {
      keyStoreKeyType = keyType;
      return this;
    }

    /**
     * Sets the key store password. The private key (PEM, JKS) and certificate (JKS only) will be
     * encrypted with this password.
     * @param password the password.
     * @return this Builder.
     */
    public Builder setKeyStorePassword(char[] password) {
      keyStorePassword = password;
      return this;
    }
  }

  /**
   * Returns a new default-constructed Builder.
   * @return a new Builder.
   */
  public static Builder newBuilder(Configuration conf) {
    return new Builder(conf);
  }
}
