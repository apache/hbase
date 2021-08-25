/**
 *
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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertEquals;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.hbase.security.token.TokenProvider;
import org.apache.hadoop.hbase.security.visibility.VisibilityTestUtil;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@Category({ ReplicationTests.class, LargeTests.class })
@RunWith(Parameterized.class)
public class TestVerifyReplicationSecureClusterCredentials {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestVerifyReplicationSecureClusterCredentials.class);

  private static MiniKdc KDC;
  private static final HBaseTestingUtility UTIL1 = new HBaseTestingUtility();
  private static final HBaseTestingUtility UTIL2 = new HBaseTestingUtility();

  private static final File KEYTAB_FILE =
    new File(UTIL1.getDataTestDir("keytab").toUri().getPath());

  private static final String LOCALHOST = "localhost";
  private static String CLUSTER_PRINCIPAL;
  private static String FULL_USER_PRINCIPAL;
  private static String HTTP_PRINCIPAL;

  private static void setUpKdcServer() throws Exception {
    KDC = UTIL1.setupMiniKdc(KEYTAB_FILE);
    String username = UserGroupInformation.getLoginUser().getShortUserName();
    String userPrincipal = username + '/' + LOCALHOST;
    CLUSTER_PRINCIPAL = userPrincipal;
    FULL_USER_PRINCIPAL = userPrincipal + '@' + KDC.getRealm();
    HTTP_PRINCIPAL = "HTTP/" + LOCALHOST;
    KDC.createPrincipal(KEYTAB_FILE, CLUSTER_PRINCIPAL, HTTP_PRINCIPAL);
  }

  private static void setupCluster(HBaseTestingUtility util) throws Exception {
    Configuration conf = util.getConfiguration();

    SecureTestUtil.enableSecurity(conf);
    VisibilityTestUtil.enableVisiblityLabels(conf);
    SecureTestUtil.verifyConfiguration(conf);

    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      AccessController.class.getName() + ',' + TokenProvider.class.getName());

    HBaseKerberosUtils.setSecuredConfiguration(conf,
      CLUSTER_PRINCIPAL + '@' + KDC.getRealm(), HTTP_PRINCIPAL + '@' + KDC.getRealm());

    util.startMiniCluster();
  }

  /**
   * Sets the security firstly for getting the correct default realm.
   */
  @BeforeClass
  public static void beforeClass() throws Exception {
    setUpKdcServer();
    setupCluster(UTIL1);
    setupCluster(UTIL2);

    try (Admin admin = UTIL1.getAdmin()) {
      admin.addReplicationPeer("1", ReplicationPeerConfig.newBuilder()
        .setClusterKey(ZKConfig.getZooKeeperClusterKey(UTIL2.getConfiguration()))
        .putConfiguration(HBaseKerberosUtils.KRB_PRINCIPAL,
          UTIL2.getConfiguration().get(HBaseKerberosUtils.KRB_PRINCIPAL))
        .putConfiguration(HBaseKerberosUtils.MASTER_KRB_PRINCIPAL,
          UTIL2.getConfiguration().get(HBaseKerberosUtils.MASTER_KRB_PRINCIPAL))
        .build());
    }
  }

  @AfterClass
  public static void cleanup() throws IOException {
    UTIL1.shutdownMiniCluster();
    UTIL2.shutdownMiniCluster();
  }

  @Parameters
  public static Collection<Supplier<String>> peer() {
    return Arrays.asList(
      () -> "1",
      () -> ZKConfig.getZooKeeperClusterKey(UTIL2.getConfiguration())
    );
  }

  @Parameter
  public Supplier<String> peer;

  @Test
  @SuppressWarnings("unchecked")
  public void testJobCredentials() throws Exception {
    Job job = new VerifyReplication().createSubmittableJob(
      new Configuration(UTIL1.getConfiguration()),
      new String[] {
        peer.get(),
        "table"
      });

    Credentials credentials = job.getCredentials();
    Collection<Token<? extends TokenIdentifier>> tokens = credentials.getAllTokens();
    assertEquals(2, tokens.size());

    String clusterId1 = ZKClusterId.readClusterIdZNode(UTIL1.getZooKeeperWatcher());
    Token<AuthenticationTokenIdentifier> tokenForCluster1 =
      (Token<AuthenticationTokenIdentifier>) credentials.getToken(new Text(clusterId1));
    assertEquals(FULL_USER_PRINCIPAL, tokenForCluster1.decodeIdentifier().getUsername());

    String clusterId2 = ZKClusterId.readClusterIdZNode(UTIL2.getZooKeeperWatcher());
    Token<AuthenticationTokenIdentifier> tokenForCluster2 =
      (Token<AuthenticationTokenIdentifier>) credentials.getToken(new Text(clusterId2));
    assertEquals(FULL_USER_PRINCIPAL, tokenForCluster2.decodeIdentifier().getUsername());
  }
}
