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
package org.apache.hadoop.hbase.client;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

/**
 * Test basic read write operation with different {@link ConnectionRegistry} implementations when
 * security is enabled.
 */
@Tag(MediumTests.TAG)
@Tag(ClientTests.TAG)
public class TestSecureBasicReadWriteWithDifferentConnectionRegistries
  extends BasicReadWriteWithDifferentConnectionRegistriesTestBase {

  protected static final File KEYTAB_FILE =
    new File(UTIL.getDataTestDir("keytab").toUri().getPath());

  private static MiniKdc KDC;
  private static String HOST = "localhost";
  private static String PRINCIPAL;
  private static String HTTP_PRINCIPAL;

  protected static void stopKDC() {
    if (KDC != null) {
      KDC.stop();
    }
  }

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    KDC = UTIL.setupMiniKdc(KEYTAB_FILE);
    PRINCIPAL = "hbase/" + HOST;
    HTTP_PRINCIPAL = "HTTP/" + HOST;
    KDC.createPrincipal(KEYTAB_FILE, PRINCIPAL, HTTP_PRINCIPAL);
    // set a smaller timeout and retry to speed up tests
    UTIL.getConfiguration().setInt(RpcClient.SOCKET_TIMEOUT_READ, 2000);
    UTIL.getConfiguration().setInt("hbase.security.relogin.maxretries", 1);
    UTIL.getConfiguration().setInt("hbase.security.relogin.maxbackoff", 100);
    UTIL.startSecureMiniCluster(KDC, PRINCIPAL, HTTP_PRINCIPAL);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  // for connecting to secure hbase cluster, we need to get some information from Configuration, so
  // here we need to use UTIL.getConfiguration to get the security related information
  @Override
  protected Configuration getConf() {
    return new Configuration(UTIL.getConfiguration());
  }

  @Override
  protected Connection createConn(URI uri) throws IOException {
    return ConnectionFactory.createConnection(uri, UTIL.getConfiguration());
  }
}
