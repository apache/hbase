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
package org.apache.hadoop.hbase.io.asyncfs;

import static org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputSaslHelper.AES_CTR_NOPADDING;
import static org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputSaslHelper.DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY;

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.http.ssl.KeyStoreTestUtil;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.security.token.TestGenerateDelegationToken;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({ MiscTests.class, MediumTests.class })
public class TestSaslFanOutOneBlockAsyncDFSOutput {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static DistributedFileSystem FS;

  private static EventLoopGroup EVENT_LOOP_GROUP;

  private static int READ_TIMEOUT_MS = 200000;

  private static final File KEYTAB_FILE = new File(
      TEST_UTIL.getDataTestDir("keytab").toUri().getPath());

  private static MiniKdc KDC;

  private static String HOST = "localhost";

  private static String USERNAME;

  private static String PRINCIPAL;

  private static String HTTP_PRINCIPAL;
  @Rule
  public TestName name = new TestName();

  @Parameter(0)
  public String protection;

  @Parameter(1)
  public String encryptionAlgorithm;

  @Parameter(2)
  public String cipherSuite;

  @Parameters(name = "{index}: protection={0}, encryption={1}, cipherSuite={2}")
  public static Iterable<Object[]> data() {
    List<Object[]> params = new ArrayList<>();
    for (String protection : Arrays.asList("authentication", "integrity", "privacy")) {
      for (String encryptionAlgorithm : Arrays.asList("", "3des", "rc4")) {
        for (String cipherSuite : Arrays.asList("", AES_CTR_NOPADDING)) {
          params.add(new Object[] { protection, encryptionAlgorithm, cipherSuite });
        }
      }
    }
    return params;
  }

  private static void setHdfsSecuredConfiguration(Configuration conf) throws Exception {
    // change XXX_USER_NAME_KEY to XXX_KERBEROS_PRINCIPAL_KEY after we drop support for hadoop-2.4.1
    conf.set(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY, PRINCIPAL + "@" + KDC.getRealm());
    conf.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, KEYTAB_FILE.getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY, PRINCIPAL + "@" + KDC.getRealm());
    conf.set(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY, KEYTAB_FILE.getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY,
      HTTP_PRINCIPAL + "@" + KDC.getRealm());
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");

    File keystoresDir = new File(TEST_UTIL.getDataTestDir("keystore").toUri().getPath());
    keystoresDir.mkdirs();
    String sslConfDir = KeyStoreTestUtil.getClasspathDir(TestGenerateDelegationToken.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir.getAbsolutePath(), sslConfDir, conf, false);

    conf.setBoolean("ignore.secure.ports.for.testing", true);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    EVENT_LOOP_GROUP = new NioEventLoopGroup();
    TEST_UTIL.getConfiguration().setInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY, READ_TIMEOUT_MS);
    Properties conf = MiniKdc.createConf();
    conf.put(MiniKdc.DEBUG, true);
    KDC = new MiniKdc(conf, new File(TEST_UTIL.getDataTestDir("kdc").toUri().getPath()));
    KDC.start();
    USERNAME = UserGroupInformation.getLoginUser().getShortUserName();
    PRINCIPAL = USERNAME + "/" + HOST;
    HTTP_PRINCIPAL = "HTTP/" + HOST;
    KDC.createPrincipal(KEYTAB_FILE, PRINCIPAL, HTTP_PRINCIPAL);
    setHdfsSecuredConfiguration(TEST_UTIL.getConfiguration());
    HBaseKerberosUtils.setKeytabFileForTesting(KEYTAB_FILE.getAbsolutePath());
    HBaseKerberosUtils.setPrincipalForTesting(PRINCIPAL + "@" + KDC.getRealm());
    HBaseKerberosUtils.setSecuredConfiguration(TEST_UTIL.getConfiguration());
    UserGroupInformation.setConfiguration(TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDownAfterClass() throws IOException, InterruptedException {
    if (EVENT_LOOP_GROUP != null) {
      EVENT_LOOP_GROUP.shutdownGracefully().sync();
    }
    if (KDC != null) {
      KDC.stop();
    }
  }

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().set("dfs.data.transfer.protection", protection);
    if (StringUtils.isBlank(encryptionAlgorithm) && StringUtils.isBlank(cipherSuite)) {
      TEST_UTIL.getConfiguration().setBoolean(DFS_ENCRYPT_DATA_TRANSFER_KEY, false);
    } else {
      TEST_UTIL.getConfiguration().setBoolean(DFS_ENCRYPT_DATA_TRANSFER_KEY, true);
    }
    if (StringUtils.isBlank(encryptionAlgorithm)) {
      TEST_UTIL.getConfiguration().unset(DFS_DATA_ENCRYPTION_ALGORITHM_KEY);
    } else {
      TEST_UTIL.getConfiguration().set(DFS_DATA_ENCRYPTION_ALGORITHM_KEY, encryptionAlgorithm);
    }
    if (StringUtils.isBlank(cipherSuite)) {
      TEST_UTIL.getConfiguration().unset(DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY);
    } else {
      TEST_UTIL.getConfiguration().set(DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY, cipherSuite);
    }

    TEST_UTIL.startMiniDFSCluster(3);
    FS = TEST_UTIL.getDFSCluster().getFileSystem();
  }

  @After
  public void tearDown() throws IOException {
    TEST_UTIL.shutdownMiniDFSCluster();
  }

  private Path getTestFile() {
    return new Path("/" + name.getMethodName().replaceAll("[^0-9a-zA-Z]", "_"));
  }

  @Test
  public void test() throws IOException, InterruptedException, ExecutionException {
    Path f = getTestFile();
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();
    final FanOutOneBlockAsyncDFSOutput out = FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, f,
      true, false, (short) 1, FS.getDefaultBlockSize(), eventLoop);
    TestFanOutOneBlockAsyncDFSOutput.writeAndVerify(eventLoop, FS, f, out);
  }
}
