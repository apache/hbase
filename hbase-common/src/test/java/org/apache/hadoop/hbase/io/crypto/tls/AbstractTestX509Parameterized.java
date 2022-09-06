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
import java.util.Collection;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized;

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

  @Parameterized.Parameter()
  public X509KeyType caKeyType;

  @Parameterized.Parameter(value = 1)
  public X509KeyType certKeyType;

  @Parameterized.Parameter(value = 2)
  public char[] keyPassword;

  @Parameterized.Parameter(value = 3)
  public Integer paramIndex;

  /**
   * Default parameters suitable for most subclasses. See example usage in {@link TestX509Util}.
   * @return an array of parameter combinations to test with.
   */
  @Parameterized.Parameters(
      name = "{index}: caKeyType={0}, certKeyType={1}, keyPassword={2}, paramIndex={3}")
  public static Collection<Object[]> defaultParams() {
    List<Object[]> result = new ArrayList<>();
    int paramIndex = 0;
    for (X509KeyType caKeyType : X509KeyType.values()) {
      for (X509KeyType certKeyType : X509KeyType.values()) {
        for (char[] keyPassword : new char[][] { "".toCharArray(), "pa$$w0rd".toCharArray() }) {
          result.add(new Object[] { caKeyType, certKeyType, keyPassword, paramIndex++ });
        }
      }
    }
    return result;
  }

  /**
   * Because key generation and writing / deleting files is kind of expensive, we cache the certs
   * and on-disk files between test cases. None of the test cases modify any of this data so it's
   * safe to reuse between tests. This caching makes all test cases after the first one for a given
   * parameter combination complete almost instantly.
   */
  protected static Configuration conf;

  protected X509TestContext x509TestContext;

  @BeforeClass
  public static void setUpBaseClass() throws Exception {
    Security.addProvider(new BouncyCastleProvider());
    File dir = new File(UTIL.getDataTestDir(TestX509Util.class.getSimpleName()).toString())
      .getCanonicalFile();
    FileUtils.forceMkdir(dir);
    PROVIDER = new X509TestContextProvider(UTIL.getConfiguration(), dir);
  }

  @AfterClass
  public static void cleanUpBaseClass() {
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
}
