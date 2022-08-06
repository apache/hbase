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
import java.nio.file.Files;
import java.security.Security;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.commons.io.FileUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Base class for parameterized unit tests that use X509TestContext for testing different X509
 * parameter combinations (CA key type, cert key type, with/without a password, with/without
 * hostname verification, etc). This base class takes care of setting up / cleaning up the test
 * environment, and caching the X509TestContext objects used by the tests.
 * <p/>
 * This file has been copied from the Apache ZooKeeper project.
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/c74658d398cdc1d207aa296cb6e20de00faec03e/zookeeper-server/src/test/java/org/apache/zookeeper/common/BaseX509ParameterizedTestCase.java">Base
 *      revision</a>
 */
public abstract class BaseX509ParameterizedTestCase {
  protected static final String KEY_NON_EMPTY_PASSWORD = "pa$$w0rd";
  protected static final String KEY_EMPTY_PASSWORD = "";

  /**
   * Because key generation and writing / deleting files is kind of expensive, we cache the certs
   * and on-disk files between test cases. None of the test cases modify any of this data so it's
   * safe to reuse between tests. This caching makes all test cases after the first one for a given
   * parameter combination complete almost instantly.
   */
  protected static Map<Integer, X509TestContext> cachedTestContexts;
  protected static File tempDir;

  protected X509TestContext x509TestContext;

  @BeforeClass
  public static void setUpBaseClass() throws Exception {
    Security.addProvider(new BouncyCastleProvider());
    cachedTestContexts = new HashMap<>();
    tempDir = Files.createTempDirectory("x509Tests").toFile();
  }

  @AfterClass
  public static void cleanUpBaseClass() {
    Security.removeProvider(BouncyCastleProvider.PROVIDER_NAME);
    cachedTestContexts.clear();
    cachedTestContexts = null;
    try {
      FileUtils.deleteDirectory(tempDir);
    } catch (IOException e) {
      // ignore
    }
  }

  /**
   * Init method. See example usage in {@link TestX509Util}.
   * @param paramIndex      the index under which the X509TestContext should be cached.
   * @param contextSupplier a function that creates and returns the X509TestContext for the current
   *                        index if one is not already cached.
   */
  protected void init(Integer paramIndex, Supplier<X509TestContext> contextSupplier) {
    if (cachedTestContexts.containsKey(paramIndex)) {
      x509TestContext = cachedTestContexts.get(paramIndex);
    } else {
      x509TestContext = contextSupplier.get();
      cachedTestContexts.put(paramIndex, x509TestContext);
    }
  }

  protected void init(final X509KeyType caKeyType, final X509KeyType certKeyType,
    final String keyPassword, final Integer paramIndex) throws Exception {
    init(paramIndex, () -> {
      try {
        return X509TestContext.newBuilder().setTempDir(tempDir).setKeyStorePassword(keyPassword)
          .setKeyStoreKeyType(certKeyType).setTrustStorePassword(keyPassword)
          .setTrustStoreKeyType(caKeyType).build();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }
}
