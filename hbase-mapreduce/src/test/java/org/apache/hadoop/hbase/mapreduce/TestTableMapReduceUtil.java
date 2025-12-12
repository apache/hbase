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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProviders;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
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
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test different variants of initTableMapperJob method
 */
@Category({ MapReduceTests.class, MediumTests.class })
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
   * initTableSnapshotMapperJob is tested in {@link TestTableSnapshotInputFormat} because the method
   * depends on an online cluster.
   */

  @Test
  public void testInitTableMapperJob1() throws Exception {
    Configuration configuration = new Configuration();
    Job job = Job.getInstance(configuration, "tableName");
    // test
    TableMapReduceUtil.initTableMapperJob("Table", new Scan(), Import.Importer.class, Text.class,
      Text.class, job, false, WALInputFormat.class);
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
    TableMapReduceUtil.initTableMapperJob(Bytes.toBytes("Table"), new Scan(), Import.Importer.class,
      Text.class, Text.class, job, false, WALInputFormat.class);
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
    TableMapReduceUtil.initTableMapperJob(Bytes.toBytes("Table"), new Scan(), Import.Importer.class,
      Text.class, Text.class, job);
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
    TableMapReduceUtil.initTableMapperJob(Bytes.toBytes("Table"), new Scan(), Import.Importer.class,
      Text.class, Text.class, job, false);
    assertEquals(TableInputFormat.class, job.getInputFormatClass());
    assertEquals(Import.Importer.class, job.getMapperClass());
    assertEquals(LongWritable.class, job.getOutputKeyClass());
    assertEquals(Text.class, job.getOutputValueClass());
    assertNull(job.getCombinerClass());
    assertEquals("Table", job.getConfiguration().get(TableInputFormat.INPUT_TABLE));
  }

  @Test
  public void testInitCredentialsForCluster1() throws Exception {
    HBaseTestingUtil util1 = new HBaseTestingUtil();
    HBaseTestingUtil util2 = new HBaseTestingUtil();

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
  public void testInitCredentialsForCluster2() throws Exception {
    HBaseTestingUtil util1 = new HBaseTestingUtil();
    HBaseTestingUtil util2 = new HBaseTestingUtil();

    File keytab = new File(util1.getDataTestDir("keytab").toUri().getPath());
    MiniKdc kdc = util1.setupMiniKdc(keytab);
    try {
      String username = UserGroupInformation.getLoginUser().getShortUserName();
      String userPrincipal = username + "/localhost";
      kdc.createPrincipal(keytab, userPrincipal, HTTP_PRINCIPAL);
      loginUserFromKeytab(userPrincipal + '@' + kdc.getRealm(), keytab.getAbsolutePath());

      try (Closeable ignored1 = util1.startSecureMiniCluster(kdc, userPrincipal, HTTP_PRINCIPAL);
        Closeable ignored2 = util2.startSecureMiniCluster(kdc, userPrincipal, HTTP_PRINCIPAL)) {
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
      }
    } finally {
      kdc.stop();
    }
  }

  @Test
  public void testInitCredentialsForCluster3() throws Exception {
    HBaseTestingUtil util1 = new HBaseTestingUtil();

    File keytab = new File(util1.getDataTestDir("keytab").toUri().getPath());
    MiniKdc kdc = util1.setupMiniKdc(keytab);
    try {
      String username = UserGroupInformation.getLoginUser().getShortUserName();
      String userPrincipal = username + "/localhost";
      kdc.createPrincipal(keytab, userPrincipal, HTTP_PRINCIPAL);
      loginUserFromKeytab(userPrincipal + '@' + kdc.getRealm(), keytab.getAbsolutePath());

      try (Closeable ignored1 = util1.startSecureMiniCluster(kdc, userPrincipal, HTTP_PRINCIPAL)) {
        HBaseTestingUtil util2 = new HBaseTestingUtil();
        // Assume util2 is insecure cluster
        // Do not start util2 because cannot boot secured mini cluster and insecure mini cluster at
        // once

        Configuration conf1 = util1.getConfiguration();
        Job job = Job.getInstance(conf1);

        TableMapReduceUtil.initCredentialsForCluster(job, util2.getConfiguration());

        Credentials credentials = job.getCredentials();
        Collection<Token<? extends TokenIdentifier>> tokens = credentials.getAllTokens();
        assertTrue(tokens.isEmpty());
      }
    } finally {
      kdc.stop();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testInitCredentialsForCluster4() throws Exception {
    HBaseTestingUtil util1 = new HBaseTestingUtil();
    // Assume util1 is insecure cluster
    // Do not start util1 because cannot boot secured mini cluster and insecure mini cluster at once

    HBaseTestingUtil util2 = new HBaseTestingUtil();
    File keytab = new File(util2.getDataTestDir("keytab").toUri().getPath());
    MiniKdc kdc = util2.setupMiniKdc(keytab);
    try {
      String username = UserGroupInformation.getLoginUser().getShortUserName();
      String userPrincipal = username + "/localhost";
      kdc.createPrincipal(keytab, userPrincipal, HTTP_PRINCIPAL);
      loginUserFromKeytab(userPrincipal + '@' + kdc.getRealm(), keytab.getAbsolutePath());

      try (Closeable ignored2 = util2.startSecureMiniCluster(kdc, userPrincipal, HTTP_PRINCIPAL)) {
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
      }
    } finally {
      kdc.stop();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testInitCredentialsForClusterUri() throws Exception {
    HBaseTestingUtil util1 = new HBaseTestingUtil();
    HBaseTestingUtil util2 = new HBaseTestingUtil();

    File keytab = new File(util1.getDataTestDir("keytab").toUri().getPath());
    MiniKdc kdc = util1.setupMiniKdc(keytab);
    try {
      String username = UserGroupInformation.getLoginUser().getShortUserName();
      String userPrincipal = username + "/localhost";
      kdc.createPrincipal(keytab, userPrincipal, HTTP_PRINCIPAL);
      loginUserFromKeytab(userPrincipal + '@' + kdc.getRealm(), keytab.getAbsolutePath());

      try (Closeable ignored1 = util1.startSecureMiniCluster(kdc, userPrincipal, HTTP_PRINCIPAL);
        Closeable ignored2 = util2.startSecureMiniCluster(kdc, userPrincipal, HTTP_PRINCIPAL)) {
        Configuration conf1 = util1.getConfiguration();
        Job job = Job.getInstance(conf1);

        // use Configuration from util1 and URI from util2, to make sure that we use the URI instead
        // of rely on the Configuration
        TableMapReduceUtil.initCredentialsForCluster(job, util1.getConfiguration(),
          new URI(util2.getRpcConnnectionURI()));

        Credentials credentials = job.getCredentials();
        Collection<Token<? extends TokenIdentifier>> tokens = credentials.getAllTokens();
        assertEquals(1, tokens.size());

        String clusterId = ZKClusterId.readClusterIdZNode(util2.getZooKeeperWatcher());
        Token<AuthenticationTokenIdentifier> tokenForCluster =
          (Token<AuthenticationTokenIdentifier>) credentials.getToken(new Text(clusterId));
        assertEquals(userPrincipal + '@' + kdc.getRealm(),
          tokenForCluster.decodeIdentifier().getUsername());
      }
    } finally {
      kdc.stop();
    }
  }

  @Test
  public void testScanSerialization() throws IOException {
    final byte[] cf = "cf".getBytes();
    final Scan scan = new Scan();
    scan.setLimit(1);
    scan.setBatch(1);
    scan.setMaxResultSize(1);
    scan.setAllowPartialResults(true);
    scan.setLoadColumnFamiliesOnDemand(true);
    scan.readVersions(1);
    scan.setColumnFamilyTimeRange(cf, 0, 1);
    scan.setTimeRange(0, 1);
    scan.setAttribute("cf", cf);
    scan.withStartRow("0".getBytes(), false);
    scan.withStopRow("1".getBytes(), true);
    scan.setFilter(new KeyOnlyFilter());
    scan.addColumn(cf, cf);
    scan.setMaxResultsPerColumnFamily(1);
    scan.setRowOffsetPerColumnFamily(1);
    scan.setReversed(true);
    scan.setConsistency(Consistency.TIMELINE);
    scan.setCaching(1);
    scan.setReadType(Scan.ReadType.STREAM);
    scan.setNeedCursorResult(true);
    scan.setQueryMetricsEnabled(true);

    final String serialized = TableMapReduceUtil.convertScanToString(scan);
    final Scan deserialized = TableMapReduceUtil.convertStringToScan(serialized);
    final String reserialized = TableMapReduceUtil.convertScanToString(deserialized);

    // Verify that serialization is symmetric
    assertEquals(serialized, reserialized);

    // Verify individual fields to catch potential omissions
    assertEquals(scan.getLimit(), deserialized.getLimit());
    assertEquals(scan.getBatch(), deserialized.getBatch());
    assertEquals(scan.getMaxResultSize(), deserialized.getMaxResultSize());
    assertEquals(scan.getAllowPartialResults(), deserialized.getAllowPartialResults());
    assertEquals(scan.getLoadColumnFamiliesOnDemandValue(),
      deserialized.getLoadColumnFamiliesOnDemandValue());
    assertEquals(scan.getMaxVersions(), deserialized.getMaxVersions());
    assertEquals(scan.getColumnFamilyTimeRange().get(cf).toString(),
      deserialized.getColumnFamilyTimeRange().get(cf).toString());
    assertEquals(scan.getTimeRange().toString(), deserialized.getTimeRange().toString());
    assertEquals(Bytes.toString(scan.getAttribute("cf")),
      Bytes.toString(deserialized.getAttribute("cf")));
    assertEquals(0, Bytes.compareTo(scan.getStartRow(), deserialized.getStartRow()));
    assertEquals(scan.includeStartRow(), deserialized.includeStartRow());
    assertEquals(0, Bytes.compareTo(scan.getStopRow(), deserialized.getStopRow()));
    assertEquals(scan.includeStopRow(), deserialized.includeStopRow());
    assertEquals(scan.getFilter().getClass().getName(),
      deserialized.getFilter().getClass().getName());
    assertEquals(scan.getFamilyMap().size(), deserialized.getFamilyMap().size());
    assertEquals(scan.getMaxResultsPerColumnFamily(), deserialized.getMaxResultsPerColumnFamily());
    assertEquals(scan.getRowOffsetPerColumnFamily(), deserialized.getRowOffsetPerColumnFamily());
    assertEquals(scan.isReversed(), deserialized.isReversed());
    assertEquals(scan.getConsistency(), deserialized.getConsistency());
    assertEquals(scan.getCaching(), deserialized.getCaching());
    assertEquals(scan.getReadType(), deserialized.getReadType());
    assertEquals(scan.isNeedCursorResult(), deserialized.isNeedCursorResult());
    assertEquals(scan.isQueryMetricsEnabled(), deserialized.isQueryMetricsEnabled());
  }
}
