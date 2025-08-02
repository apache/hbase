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
package org.apache.hadoop.hbase.mapreduce;

import static org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytab;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.access.PermissionStorage;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.security.token.TokenProvider;
import org.apache.hadoop.hbase.security.visibility.VisibilityTestUtil;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowMapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for {@link HFileOutputFormat2} with secure mode.
 */
@Category({ VerySlowMapReduceTests.class, LargeTests.class })
public class TestHFileOutputFormat2WithSecurity extends HFileOutputFormat2TestBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHFileOutputFormat2WithSecurity.class);

  private static final byte[] FAMILIES = Bytes.toBytes("test_cf");

  @Test
  public void testMRIncrementalLoadWithMultiSecurityCluster() throws Exception {
    // Start cluster A
    HBaseTestingUtil localCluster = new HBaseTestingUtil();
    Configuration confA = localCluster.getConfiguration();

    // Prepare security cluster.
    File keytab = new File(localCluster.getDataTestDir("keytab").toUri().getPath());
    MiniKdc kdc = localCluster.setupMiniKdc(keytab);
    String username = UserGroupInformation.getLoginUser().getShortUserName();
    String userPrincipal = username + "/localhost";
    kdc.createPrincipal(keytab, userPrincipal, "HTTP/localhost");
    loginUserFromKeytab(userPrincipal + '@' + kdc.getRealm(), keytab.getAbsolutePath());

    startSecureMiniCluster(localCluster, kdc, userPrincipal);

    // Start remote cluster
    HBaseTestingUtil remoteCluster = new HBaseTestingUtil();
    startSecureMiniCluster(remoteCluster, kdc, userPrincipal);

    TableName tableName = TableName.valueOf("testMRIncrementalLoadWithMultiSecurityClusters");

    // Create table in remote cluster
    try (Table table = remoteCluster.createTable(tableName, FAMILIES);
      RegionLocator r = remoteCluster.getConnection().getRegionLocator(tableName)) {
      Job job = Job.getInstance(confA, "testMRIncrementalLoadWithMultiSecurityClusters");
      job.setWorkingDirectory(
        localCluster.getDataTestDirOnTestFS("testMRIncrementalLoadWithMultiSecurityClusters"));
      setupRandomGeneratorMapper(job, false);
      HFileOutputFormat2.configureIncrementalLoad(job, table, r);

      Map<Text, Token<? extends TokenIdentifier>> tokenMap = job.getCredentials().getTokenMap();
      assertEquals(2, tokenMap.size());

      String remoteClusterId =
        remoteCluster.getHBaseClusterInterface().getClusterMetrics().getClusterId();
      assertTrue(tokenMap.containsKey(new Text(remoteClusterId)));
    } finally {
      remoteCluster.deleteTable(tableName);
      localCluster.shutdownMiniCluster();
      remoteCluster.shutdownMiniCluster();
      kdc.stop();
    }
  }

  private static void startSecureMiniCluster(HBaseTestingUtil util, MiniKdc kdc, String principal)
    throws Exception {
    Configuration conf = util.getConfiguration();

    SecureTestUtil.enableSecurity(conf);
    VisibilityTestUtil.enableVisiblityLabels(conf);
    SecureTestUtil.verifyConfiguration(conf);

    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      AccessController.class.getName() + ',' + TokenProvider.class.getName());

    HBaseKerberosUtils.setSecuredConfiguration(conf, principal + '@' + kdc.getRealm(),
      "HTTP/localhost" + '@' + kdc.getRealm());

    util.startMiniCluster();
    try {
      util.waitUntilAllRegionsAssigned(PermissionStorage.ACL_TABLE_NAME);
    } catch (Exception e) {
      util.shutdownMiniCluster();
      throw e;
    }
  }
}
