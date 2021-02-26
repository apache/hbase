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

import java.io.File;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;

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

  //When extending SecureTestCluster on downstream projects that refer SecureTestCluster via
  //hbase-server jar, we need to provide a way for the implementation to refer to its own class
  //definition, so that KeyStoreTestUtil.getClasspathDir can resolve a valid path in the local FS
  //to place required SSL config files.
  private static Class testRunnerClass = SecureTestCluster.class;

  /**
   * SecureTestCluster extending classes can set their own <code>Class</code> reference type
   * to be used as the target resource to be looked for on the class loader by
   * <code>KeyStoreTestUtil</code>, when deciding where to place ssl related config files.
   * @param testRunnerClass a <code>Class</code> reference from the
   *                        <code>SecureTestCluster</code> extender.
   */
  protected static void setTestRunner(Class testRunnerClass){
    SecureTestCluster.testRunnerClass = testRunnerClass;
  }

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
    HBaseKerberosUtils.setSSLConfiguration(TEST_UTIL, testRunnerClass);

    TEST_UTIL.getConfiguration().setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        TokenProvider.class.getName());
    TEST_UTIL.startMiniDFSCluster(1);
    Path rootdir = TEST_UTIL.getDataTestDirOnTestFS("TestGenerateDelegationToken");
    CommonFSUtils.setRootDir(TEST_UTIL.getConfiguration(), rootdir);
    CLUSTER = new LocalHBaseCluster(TEST_UTIL.getConfiguration(), 1);
    CLUSTER.startup();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try {
      if (CLUSTER != null) {
        CLUSTER.shutdown();
      }
      CLUSTER.join();
      if (KDC != null) {
        KDC.stop();
      }
      TEST_UTIL.shutdownMiniCluster();
    } finally {
      setTestRunner(SecureTestCluster.class);
    }
  }
}
