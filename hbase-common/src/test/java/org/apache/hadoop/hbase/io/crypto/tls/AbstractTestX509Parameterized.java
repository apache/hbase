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
import java.security.Security;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.provider.Arguments;

/**
 * Base class for parameterized unit tests that use X509TestContext for testing different X509
 * parameter combinations (CA key type, cert key type, with/without a password, with/without
 * hostname verification, etc).
 * <p/>
 * This base class takes care of setting up / cleaning up the test environment, and caching the
 * X509TestContext objects used by the tests.
 * <p/>
 * This file has been copied from the Apache ZooKeeper project.
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/master/zookeeper-server/src/test/java/org/apache/zookeeper/common/BaseX509ParameterizedTestCase.java">Base
 *      revision</a>
 */
public abstract class AbstractTestX509Parameterized {

  private static final HBaseCommonTestingUtility UTIL = new HBaseCommonTestingUtility();
  private static X509TestContextProvider PROVIDER;

  private X509KeyType caKeyType;
  private X509KeyType certKeyType;
  private char[] keyPassword;

  public AbstractTestX509Parameterized(X509KeyType caKeyType, X509KeyType certKeyType,
    char[] keyPassword) {
    this.caKeyType = caKeyType;
    this.certKeyType = certKeyType;
    this.keyPassword = keyPassword;
  }

  /**
   * Default parameters suitable for most subclasses. See example usage in {@link TestX509Util}.
   * @return a stream of parameter combinations to test with.
   */
  public static Stream<Arguments> parameters() {
    List<Arguments> result = new ArrayList<>();
    for (X509KeyType caKeyType : X509KeyType.values()) {
      for (X509KeyType certKeyType : X509KeyType.values()) {
        for (char[] keyPassword : new char[][] { "".toCharArray(), "pa$$w0rd".toCharArray() }) {
          result.add(Arguments.of(caKeyType, certKeyType, keyPassword));
        }
      }
    }
    return result.stream();
  }

  /**
   * Because key generation and writing / deleting files is kind of expensive, we cache the certs
   * and on-disk files between test cases. None of the test cases modify any of this data so it's
   * safe to reuse between tests. This caching makes all test cases after the first one for a given
   * parameter combination complete almost instantly.
   */
  protected static Configuration conf;

  protected X509TestContext x509TestContext;

  @BeforeAll
  public static void setUpBaseClass() throws Exception {
    Security.addProvider(new BouncyCastleProvider());
    File dir = new File(UTIL.getDataTestDir(TestX509Util.class.getSimpleName()).toString())
      .getCanonicalFile();
    FileUtils.forceMkdir(dir);
    PROVIDER = new X509TestContextProvider(UTIL.getConfiguration(), dir);
  }

  @AfterAll
  public static void cleanUpBaseClass() {
    Security.removeProvider(BouncyCastleProvider.PROVIDER_NAME);
    UTIL.cleanupTestDir();
  }

  @BeforeEach
  public void setUp() throws IOException {
    x509TestContext = PROVIDER.get(caKeyType, certKeyType, keyPassword);
    x509TestContext.setConfigurations(KeyStoreFileType.JKS, KeyStoreFileType.JKS);
    conf = new Configuration(UTIL.getConfiguration());
  }

  @AfterEach
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
}
