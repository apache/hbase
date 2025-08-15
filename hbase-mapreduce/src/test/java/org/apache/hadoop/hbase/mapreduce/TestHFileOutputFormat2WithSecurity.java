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

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowMapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for {@link HFileOutputFormat2} with secure mode.
 */
@Category({ VerySlowMapReduceTests.class, LargeTests.class })
public class TestHFileOutputFormat2WithSecurity {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHFileOutputFormat2WithSecurity.class);

  private static final byte[] FAMILIES = Bytes.toBytes("test_cf");

  private static final String HTTP_PRINCIPAL = "HTTP/localhost";

  private HBaseTestingUtility utilA;

  private Configuration confA;

  private HBaseTestingUtility utilB;

  private MiniKdc kdc;

  private List<Closeable> clusters = new ArrayList<>();

  @Before
  public void setupSecurityClusters() throws Exception {
    utilA = new HBaseTestingUtility();
    confA = utilA.getConfiguration();

    utilB = new HBaseTestingUtility();

    // Prepare security configs.
    File keytab = new File(utilA.getDataTestDir("keytab").toUri().getPath());
    kdc = utilA.setupMiniKdc(keytab);
    String username = UserGroupInformation.getLoginUser().getShortUserName();
    String userPrincipal = username + "/localhost";
    kdc.createPrincipal(keytab, userPrincipal, HTTP_PRINCIPAL);
    loginUserFromKeytab(userPrincipal + '@' + kdc.getRealm(), keytab.getAbsolutePath());

    // Start security clusterA
    clusters.add(utilA.startSecureMiniCluster(kdc, userPrincipal, HTTP_PRINCIPAL));

    // Start security clusterB
    clusters.add(utilB.startSecureMiniCluster(kdc, userPrincipal, HTTP_PRINCIPAL));
  }

  @After
  public void teardownSecurityClusters() {
    IOUtils.closeQuietly(clusters);
    clusters.clear();
    if (kdc != null) {
      kdc.stop();
    }
  }

  @Test
  public void testIncrementalLoadInMultiClusterWithSecurity() throws Exception {
    TableName tableName = TableName.valueOf("testIncrementalLoadInMultiClusterWithSecurity");

    // Create table in clusterB
    try (Table table = utilB.createTable(tableName, FAMILIES);
      RegionLocator r = utilB.getConnection().getRegionLocator(tableName)) {

      // Create job in clusterA
      Job job = Job.getInstance(confA, "testIncrementalLoadInMultiClusterWithSecurity");
      job.setWorkingDirectory(
        utilA.getDataTestDirOnTestFS("testIncrementalLoadInMultiClusterWithSecurity"));
      job.setInputFormatClass(NMapInputFormat.class);
      job.setMapperClass(TestHFileOutputFormat2.RandomKVGeneratingMapper.class);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(KeyValue.class);
      HFileOutputFormat2.configureIncrementalLoad(job, table, r);

      assertEquals(2, job.getCredentials().getAllTokens().size());

      String remoteClusterId = utilB.getHBaseClusterInterface().getClusterMetrics().getClusterId();
      assertTrue(job.getCredentials().getToken(new Text(remoteClusterId)) != null);
    } finally {
      if (utilB.getAdmin().tableExists(tableName)) {
        utilB.deleteTable(tableName);
      }
    }
  }
}
