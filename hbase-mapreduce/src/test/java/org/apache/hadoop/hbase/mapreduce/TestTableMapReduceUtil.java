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
package org.apache.hadoop.hbase.mapreduce;

import static org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytab;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.Closeable;
import java.io.File;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.access.PermissionStorage;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProviders;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.hbase.security.token.TokenProvider;
import org.apache.hadoop.hbase.security.visibility.VisibilityTestUtil;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test different variants of initTableMapperJob method
 */
@Category({MapReduceTests.class, MediumTests.class})
public class TestTableMapReduceUtil {
  private static final String HTTP_PRINCIPAL = "HTTP/localhost";

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTableMapReduceUtil.class);

  @After
  public void after() {
    SaslClientAuthenticationProviders.reset();
  }

  /*
   * initTableSnapshotMapperJob is tested in {@link TestTableSnapshotInputFormat} because
   * the method depends on an online cluster.
   */

  @Test
  public void testInitTableMapperJob1() throws Exception {
    Configuration configuration = new Configuration();
    Job job = Job.getInstance(configuration, "tableName");
    // test
    TableMapReduceUtil.initTableMapperJob(
      "Table", new Scan(), Import.Importer.class, Text.class, Text.class, job,
      false, WALInputFormat.class);
    assertEquals(WALInputFormat.class, job.getInputFormatClass());
    assertEquals(Import.Importer.class, job.getMapperClass());
    assertEquals(LongWritable.class, job.getOutputKeyClass());
    assertEquals(Text.class, job.getOutputValueClass());
    assertNull(job.getCombinerClass());
    assertEquals("Table", job.getConfiguration().get(TableInputFormat.INPUT_TABLE));
  }

  @Test
  public void testInitTableMapperJob2() throws Exception {
    Configuration configuration = new Configuration();
    Job job = Job.getInstance(configuration, "tableName");
    TableMapReduceUtil.initTableMapperJob(
      Bytes.toBytes("Table"), new Scan(), Import.Importer.class, Text.class,
      Text.class, job, false, WALInputFormat.class);
    assertEquals(WALInputFormat.class, job.getInputFormatClass());
    assertEquals(Import.Importer.class, job.getMapperClass());
    assertEquals(LongWritable.class, job.getOutputKeyClass());
    assertEquals(Text.class, job.getOutputValueClass());
    assertNull(job.getCombinerClass());
    assertEquals("Table", job.getConfiguration().get(TableInputFormat.INPUT_TABLE));
  }

  @Test
  public void testInitTableMapperJob3() throws Exception {
    Configuration configuration = new Configuration();
    Job job = Job.getInstance(configuration, "tableName");
    TableMapReduceUtil.initTableMapperJob(
      Bytes.toBytes("Table"), new Scan(), Import.Importer.class, Text.class,
      Text.class, job);
    assertEquals(TableInputFormat.class, job.getInputFormatClass());
    assertEquals(Import.Importer.class, job.getMapperClass());
    assertEquals(LongWritable.class, job.getOutputKeyClass());
    assertEquals(Text.class, job.getOutputValueClass());
    assertNull(job.getCombinerClass());
    assertEquals("Table", job.getConfiguration().get(TableInputFormat.INPUT_TABLE));
  }

  @Test
  public void testInitTableMapperJob4() throws Exception {
    Configuration configuration = new Configuration();
    Job job = Job.getInstance(configuration, "tableName");
    TableMapReduceUtil.initTableMapperJob(
      Bytes.toBytes("Table"), new Scan(), Import.Importer.class, Text.class,
      Text.class, job, false);
    assertEquals(TableInputFormat.class, job.getInputFormatClass());
    assertEquals(Import.Importer.class, job.getMapperClass());
    assertEquals(LongWritable.class, job.getOutputKeyClass());
    assertEquals(Text.class, job.getOutputValueClass());
    assertNull(job.getCombinerClass());
    assertEquals("Table", job.getConfiguration().get(TableInputFormat.INPUT_TABLE));
  }

  private static Closeable startSecureMiniCluster(
    HBaseTestingUtility util, MiniKdc kdc, String principal) throws Exception {
    Configuration conf = util.getConfiguration();

    SecureTestUtil.enableSecurity(conf);
    VisibilityTestUtil.enableVisiblityLabels(conf);
    SecureTestUtil.verifyConfiguration(conf);

    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      AccessController.class.getName() + ',' + TokenProvider.class.getName());

    HBaseKerberosUtils.setSecuredConfiguration(conf,
      principal + '@' + kdc.getRealm(), HTTP_PRINCIPAL + '@' + kdc.getRealm());

    KerberosName.resetDefaultRealm();

    util.startMiniCluster();
    try {
      util.waitUntilAllRegionsAssigned(PermissionStorage.ACL_TABLE_NAME);
    } catch (Exception e) {
      util.shutdownMiniCluster();
      throw e;
    }

    return util::shutdownMiniCluster;
  }

  @Test
  public void testInitCredentialsForCluster1() throws Exception {
    HBaseTestingUtility util1 = new HBaseTestingUtility();
    HBaseTestingUtility util2 = new HBaseTestingUtility();

    util1.startMiniCluster();
    try {
      util2.startMiniCluster();
      try {
        Configuration conf1 = util1.getConfiguration();
        Job job = Job.getInstance(conf1);

        TableMapReduceUtil.initCredentialsForCluster(job, util2.getConfiguration());

        Credentials credentials = job.getCredentials();
        Collection<Token<? extends TokenIdentifier>> tokens = credentials.getAllTokens();
        assertTrue(tokens.isEmpty());
      } finally {
        util2.shutdownMiniCluster();
      }
    } finally {
      util1.shutdownMiniCluster();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testInitCredentialsForCluster2()
    throws Exception {
    HBaseTestingUtility util1 = new HBaseTestingUtility();
    HBaseTestingUtility util2 = new HBaseTestingUtility();

    File keytab = new File(util1.getDataTestDir("keytab").toUri().getPath());
    MiniKdc kdc = util1.setupMiniKdc(keytab);
    String username = UserGroupInformation.getLoginUser().getShortUserName();
    String userPrincipal = username + "/localhost";
    kdc.createPrincipal(keytab, userPrincipal, HTTP_PRINCIPAL);
    loginUserFromKeytab(userPrincipal + '@' + kdc.getRealm(), keytab.getAbsolutePath());

    try (Closeable util1Closeable = startSecureMiniCluster(util1, kdc, userPrincipal);
      Closeable util2Closeable = startSecureMiniCluster(util2, kdc, userPrincipal)) {
      try {
        Configuration conf1 = util1.getConfiguration();
        Job job = Job.getInstance(conf1);

        TableMapReduceUtil.initCredentialsForCluster(job, util2.getConfiguration());

        Credentials credentials = job.getCredentials();
        Collection<Token<? extends TokenIdentifier>> tokens = credentials.getAllTokens();
        assertEquals(1, tokens.size());

        String clusterId = ZKClusterId.readClusterIdZNode(util2.getZooKeeperWatcher());
        Token<AuthenticationTokenIdentifier> tokenForCluster =
          (Token<AuthenticationTokenIdentifier>) credentials.getToken(new Text(clusterId));
        assertEquals(userPrincipal + '@' + kdc.getRealm(),
          tokenForCluster.decodeIdentifier().getUsername());
      } finally {
        kdc.stop();
      }
    }
  }

  @Test
  public void testInitCredentialsForCluster3() throws Exception {
    HBaseTestingUtility util1 = new HBaseTestingUtility();

    File keytab = new File(util1.getDataTestDir("keytab").toUri().getPath());
    MiniKdc kdc = util1.setupMiniKdc(keytab);
    String username = UserGroupInformation.getLoginUser().getShortUserName();
    String userPrincipal = username + "/localhost";
    kdc.createPrincipal(keytab, userPrincipal, HTTP_PRINCIPAL);
    loginUserFromKeytab(userPrincipal + '@' + kdc.getRealm(), keytab.getAbsolutePath());

    try (Closeable util1Closeable = startSecureMiniCluster(util1, kdc, userPrincipal)) {
      try {
        HBaseTestingUtility util2 = new HBaseTestingUtility();
        // Assume util2 is insecure cluster
        // Do not start util2 because cannot boot secured mini cluster and insecure mini cluster at
        // once

        Configuration conf1 = util1.getConfiguration();
        Job job = Job.getInstance(conf1);

        TableMapReduceUtil.initCredentialsForCluster(job, util2.getConfiguration());

        Credentials credentials = job.getCredentials();
        Collection<Token<? extends TokenIdentifier>> tokens = credentials.getAllTokens();
        assertTrue(tokens.isEmpty());
      } finally {
        kdc.stop();
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testInitCredentialsForCluster4()
    throws Exception {
    HBaseTestingUtility util1 = new HBaseTestingUtility();
    // Assume util1 is insecure cluster
    // Do not start util1 because cannot boot secured mini cluster and insecure mini cluster at once

    HBaseTestingUtility util2 = new HBaseTestingUtility();
    File keytab = new File(util2.getDataTestDir("keytab").toUri().getPath());
    MiniKdc kdc = util2.setupMiniKdc(keytab);
    String username = UserGroupInformation.getLoginUser().getShortUserName();
    String userPrincipal = username + "/localhost";
    kdc.createPrincipal(keytab, userPrincipal, HTTP_PRINCIPAL);
    loginUserFromKeytab(userPrincipal + '@' + kdc.getRealm(), keytab.getAbsolutePath());

    try (Closeable util2Closeable = startSecureMiniCluster(util2, kdc, userPrincipal)) {
      try {
        Configuration conf1 = util1.getConfiguration();
        Job job = Job.getInstance(conf1);

        TableMapReduceUtil.initCredentialsForCluster(job, util2.getConfiguration());

        Credentials credentials = job.getCredentials();
        Collection<Token<? extends TokenIdentifier>> tokens = credentials.getAllTokens();
        assertEquals(1, tokens.size());

        String clusterId = ZKClusterId.readClusterIdZNode(util2.getZooKeeperWatcher());
        Token<AuthenticationTokenIdentifier> tokenForCluster =
          (Token<AuthenticationTokenIdentifier>) credentials.getToken(new Text(clusterId));
        assertEquals(userPrincipal + '@' + kdc.getRealm(),
          tokenForCluster.decodeIdentifier().getUsername());
      } finally {
        kdc.stop();
      }
    }
  }
}
