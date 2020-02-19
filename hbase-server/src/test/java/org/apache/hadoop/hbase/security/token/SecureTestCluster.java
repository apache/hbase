/**
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

package org.apache.hadoop.hbase.security.token;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;

/**
 * The class for set up a security cluster with kerberos, hdfs, hbase.
 */
public class SecureTestCluster {
  protected static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected static String USERNAME;

  private static LocalHBaseCluster CLUSTER;

  private static final File KEYTAB_FILE = new File(TEST_UTIL.getDataTestDir("keytab").toUri()
      .getPath());
  private static MiniKdc KDC;

  private static String HOST = "localhost";

  private static String PRINCIPAL;

  private static String HTTP_PRINCIPAL;

  /**
   * Setup and start kerberos, hbase
   */
  @BeforeClass
  public static void setUp() throws Exception {
    KDC = TEST_UTIL.setupMiniKdc(KEYTAB_FILE);
    USERNAME = UserGroupInformation.getLoginUser().getShortUserName();
    PRINCIPAL = USERNAME + "/" + HOST;
    HTTP_PRINCIPAL = "HTTP/" + HOST;
    KDC.createPrincipal(KEYTAB_FILE, PRINCIPAL, HTTP_PRINCIPAL);
    TEST_UTIL.startMiniZKCluster();

    HBaseKerberosUtils.setSecuredConfiguration(TEST_UTIL.getConfiguration(),
        PRINCIPAL + "@" + KDC.getRealm(), HTTP_PRINCIPAL + "@" + KDC.getRealm());
    HBaseKerberosUtils.setSSLConfiguration(TEST_UTIL, SecureTestCluster.class);

    TEST_UTIL.getConfiguration().setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        TokenProvider.class.getName());
    TEST_UTIL.startMiniDFSCluster(1);
    Path rootdir = TEST_UTIL.getDataTestDirOnTestFS("TestGenerateDelegationToken");
    FSUtils.setRootDir(TEST_UTIL.getConfiguration(), rootdir);
    CLUSTER = new LocalHBaseCluster(TEST_UTIL.getConfiguration(), 1);
    CLUSTER.startup();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (CLUSTER != null) {
      CLUSTER.shutdown();
      CLUSTER.join();
    }
    if (KDC != null) {
      KDC.stop();
    }
    TEST_UTIL.shutdownMiniCluster();
  }
}
