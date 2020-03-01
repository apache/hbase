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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoop;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

@RunWith(Parameterized.class)
@Category({ MiscTests.class, LargeTests.class })
public class TestSaslFanOutOneBlockAsyncDFSOutput {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSaslFanOutOneBlockAsyncDFSOutput.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static DistributedFileSystem FS;

  private static EventLoopGroup EVENT_LOOP_GROUP;

  private static Class<? extends Channel> CHANNEL_CLASS;

  private static int READ_TIMEOUT_MS = 200000;

  private static final File KEYTAB_FILE =
    new File(TEST_UTIL.getDataTestDir("keytab").toUri().getPath());

  private static MiniKdc KDC;

  private static String HOST = "localhost";

  private static String USERNAME;

  private static String PRINCIPAL;

  private static String HTTP_PRINCIPAL;

  private static String TEST_KEY_NAME = "test_key";

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
        for (String cipherSuite : Arrays.asList("", CipherSuite.AES_CTR_NOPADDING.getName())) {
          params.add(new Object[] { protection, encryptionAlgorithm, cipherSuite });
        }
      }
    }
    return params;
  }

  private static void setUpKeyProvider(Configuration conf) throws Exception {
    URI keyProviderUri =
      new URI("jceks://file" + TEST_UTIL.getDataTestDir("test.jks").toUri().toString());
    conf.set("dfs.encryption.key.provider.uri", keyProviderUri.toString());
    KeyProvider keyProvider = KeyProviderFactory.get(keyProviderUri, conf);
    keyProvider.createKey(TEST_KEY_NAME, KeyProvider.options(conf));
    keyProvider.flush();
    keyProvider.close();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    EVENT_LOOP_GROUP = new NioEventLoopGroup();
    CHANNEL_CLASS = NioSocketChannel.class;
    TEST_UTIL.getConfiguration().setInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY, READ_TIMEOUT_MS);
    KDC = TEST_UTIL.setupMiniKdc(KEYTAB_FILE);
    USERNAME = UserGroupInformation.getLoginUser().getShortUserName();
    PRINCIPAL = USERNAME + "/" + HOST;
    HTTP_PRINCIPAL = "HTTP/" + HOST;
    KDC.createPrincipal(KEYTAB_FILE, PRINCIPAL, HTTP_PRINCIPAL);

    setUpKeyProvider(TEST_UTIL.getConfiguration());
    HBaseKerberosUtils.setSecuredConfiguration(TEST_UTIL.getConfiguration(),
        PRINCIPAL + "@" + KDC.getRealm(), HTTP_PRINCIPAL + "@" + KDC.getRealm());
    HBaseKerberosUtils.setSSLConfiguration(TEST_UTIL, TestSaslFanOutOneBlockAsyncDFSOutput.class);
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

  private Path testDirOnTestFs;

  private Path entryptionTestDirOnTestFs;

  private void createEncryptionZone() throws Exception {
    Method method =
      DistributedFileSystem.class.getMethod("createEncryptionZone", Path.class, String.class);
    method.invoke(FS, entryptionTestDirOnTestFs, TEST_KEY_NAME);
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
    testDirOnTestFs = new Path("/" + name.getMethodName().replaceAll("[^0-9a-zA-Z]", "_"));
    FS.mkdirs(testDirOnTestFs);
    entryptionTestDirOnTestFs = new Path("/" + testDirOnTestFs.getName() + "_enc");
    FS.mkdirs(entryptionTestDirOnTestFs);
    createEncryptionZone();
  }

  @After
  public void tearDown() throws IOException {
    TEST_UTIL.shutdownMiniDFSCluster();
  }

  private Path getTestFile() {
    return new Path(testDirOnTestFs, "test");
  }

  private Path getEncryptionTestFile() {
    return new Path(entryptionTestDirOnTestFs, "test");
  }

  private void test(Path file) throws IOException, InterruptedException, ExecutionException {
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();
    FanOutOneBlockAsyncDFSOutput out = FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, file,
      true, false, (short) 3, FS.getDefaultBlockSize(), eventLoop, CHANNEL_CLASS);
    TestFanOutOneBlockAsyncDFSOutput.writeAndVerify(FS, file, out);
  }

  @Test
  public void test() throws IOException, InterruptedException, ExecutionException {
    test(getTestFile());
    test(getEncryptionTestFile());
  }
}
