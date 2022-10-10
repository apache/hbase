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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;

/**
 * Base class for instances of {@link KeyStoreLoader} which load the key/trust stores from files on
 * a filesystem using standard {@link KeyStore} types like JKS or PKCS12.
 * <p/>
 * This file has been copied from the Apache ZooKeeper project.
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/c74658d398cdc1d207aa296cb6e20de00faec03e/zookeeper-server/src/main/java/org/apache/zookeeper/common/StandardTypeFileKeyStoreLoader.java">Base
 *      revision</a>
 */
abstract class StandardTypeFileKeyStoreLoader extends FileKeyStoreLoader {
  private static final char[] EMPTY_CHAR_ARRAY = new char[0];

  protected final SupportedStandardKeyFormat format;

  protected enum SupportedStandardKeyFormat {
    JKS,
    PKCS12,
    BCFKS
  }

  StandardTypeFileKeyStoreLoader(String keyStorePath, String trustStorePath,
    char[] keyStorePassword, char[] trustStorePassword, SupportedStandardKeyFormat format) {
    super(keyStorePath, trustStorePath, keyStorePassword, trustStorePassword);
    this.format = format;
  }

  @Override
  public KeyStore loadKeyStore() throws IOException, GeneralSecurityException {
    try (InputStream inputStream = Files.newInputStream(new File(keyStorePath).toPath())) {
      KeyStore ks = keyStoreInstance();
      ks.load(inputStream, passwordStringToCharArray(keyStorePassword));
      return ks;
    }
  }

  @Override
  public KeyStore loadTrustStore() throws IOException, GeneralSecurityException {
    try (InputStream inputStream = Files.newInputStream(new File(trustStorePath).toPath())) {
      KeyStore ts = keyStoreInstance();
      ts.load(inputStream, passwordStringToCharArray(trustStorePassword));
      return ts;
    }
  }

  private KeyStore keyStoreInstance() throws KeyStoreException {
    return KeyStore.getInstance(format.name());
  }

  private static char[] passwordStringToCharArray(char[] password) {
    return password == null ? EMPTY_CHAR_ARRAY : password;
  }
}
