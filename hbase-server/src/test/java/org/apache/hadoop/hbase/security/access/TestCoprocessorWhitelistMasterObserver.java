/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.security.access;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.junit.rules.TestRule;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

import java.io.IOException;

/**
 * Performs coprocessor loads for variuos paths and malformed strings
 */
@Category({SecurityTests.class, MediumTests.class})
public class TestCoprocessorWhitelistMasterObserver extends SecureTestUtil {
  private static final Log LOG = LogFactory.getLog(TestCoprocessorWhitelistMasterObserver.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static Configuration conf;
  private static final TableName TEST_TABLE = TableName.valueOf("testTable");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("fam1");

  @After
  public void tearDownTestCoprocessorWhitelistMasterObserver() throws Exception {
    Admin admin = UTIL.getHBaseAdmin();
    try {
      try {
        admin.disableTable(TEST_TABLE);
      } catch (TableNotEnabledException ex) {
        // Table was left disabled by test
        LOG.info("Table was left disabled by test");
      }
      admin.deleteTable(TEST_TABLE);
    } catch (TableNotFoundException ex) {
      // Table was not created for some reason?
      LOG.info("Table was not created for some reason");
    }
    UTIL.shutdownMiniCluster();
  }

  @ClassRule
  public static TestRule timeout =
      CategoryBasedTimeout.forClass(TestCoprocessorWhitelistMasterObserver.class);

  /**
   * Test a table modification adding a coprocessor path
   * which is not whitelisted
   * @result An IOException should be thrown and caught
   *         to show coprocessor is working as desired
   * @param whitelistedPaths A String array of paths to add in
   *         for the whitelisting configuration
   * @param coprocessorPath A String to use as the
   *         path for a mock coprocessor
   */
  private static void positiveTestCase(String[] whitelistedPaths,
      String coprocessorPath) throws Exception {
    Configuration conf = UTIL.getConfiguration();
    // load coprocessor under test
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        CoprocessorWhitelistMasterObserver.class.getName());
    conf.setStrings(
        CoprocessorWhitelistMasterObserver.CP_COPROCESSOR_WHITELIST_PATHS_KEY,
        whitelistedPaths);
    // set retries low to raise exception quickly
    conf.setInt("hbase.client.retries.number", 1);
    UTIL.startMiniCluster();
    Table table = UTIL.createTable(TEST_TABLE,
        new byte[][] { TEST_FAMILY });
    UTIL.waitUntilAllRegionsAssigned(TEST_TABLE);
    Connection connection = ConnectionFactory.createConnection(conf);
    Table t = connection.getTable(TEST_TABLE);
    HTableDescriptor htd = t.getTableDescriptor();
    htd.addCoprocessor("net.clayb.hbase.coprocessor.NotWhitelisted",
      new Path(coprocessorPath),
      Coprocessor.PRIORITY_USER, null);
    LOG.info("Modifying Table");
    try {
      connection.getAdmin().modifyTable(TEST_TABLE, htd);
      fail("Expected coprocessor to raise IOException");
    } catch (IOException e) {
      // swallow exception from coprocessor
    }
    LOG.info("Done Modifying Table");
    assertEquals(0, t.getTableDescriptor().getCoprocessors().size());
  }

  /**
   * Test a table modification adding a coprocessor path
   * which is whitelisted
   * @result The coprocessor should be added to the table
   *         descriptor successfully
   * @param whitelistedPaths A String array of paths to add in
   *         for the whitelisting configuration
   * @param coprocessorPath A String to use as the
   *         path for a mock coprocessor
   */
  private static void negativeTestCase(String[] whitelistedPaths,
      String coprocessorPath) throws Exception {
    Configuration conf = UTIL.getConfiguration();
    // load coprocessor under test
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        CoprocessorWhitelistMasterObserver.class.getName());
    // set retries low to raise exception quickly
    conf.setInt("hbase.client.retries.number", 1);
    // set a coprocessor whitelist path for test
    conf.setStrings(
        CoprocessorWhitelistMasterObserver.CP_COPROCESSOR_WHITELIST_PATHS_KEY,
        whitelistedPaths);
    UTIL.startMiniCluster();
    Table table = UTIL.createTable(TEST_TABLE,
        new byte[][] { TEST_FAMILY });
    UTIL.waitUntilAllRegionsAssigned(TEST_TABLE);
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();
    // disable table so we do not actually try loading non-existant
    // coprocessor file
    admin.disableTable(TEST_TABLE);
    Table t = connection.getTable(TEST_TABLE);
    HTableDescriptor htd = t.getTableDescriptor();
    htd.addCoprocessor("net.clayb.hbase.coprocessor.Whitelisted",
      new Path(coprocessorPath),
      Coprocessor.PRIORITY_USER, null);
    LOG.info("Modifying Table");
    admin.modifyTable(TEST_TABLE, htd);
    assertEquals(1, t.getTableDescriptor().getCoprocessors().size());
    LOG.info("Done Modifying Table");
  }

  /**
   * Test a table modification adding a coprocessor path
   * which is not whitelisted
   * @result An IOException should be thrown and caught
   *         to show coprocessor is working as desired
   */
  @Test
  @Category(MediumTests.class)
  public void testSubstringNonWhitelisted() throws Exception {
    positiveTestCase(new String[]{"/permitted/*"},
        "file:///notpermitted/couldnotpossiblyexist.jar");
  }

  /**
   * Test a table creation including a coprocessor path
   * which is not whitelisted
   * @result Coprocessor should be added to table descriptor
   *         Table is disabled to avoid an IOException due to
   *         the added coprocessor not actually existing on disk
   */
  @Test
  @Category(MediumTests.class)
  public void testDifferentFileSystemNonWhitelisted() throws Exception {
    positiveTestCase(new String[]{"hdfs://foo/bar"},
        "file:///notpermitted/couldnotpossiblyexist.jar");
  }

  /**
   * Test a table modification adding a coprocessor path
   * which is whitelisted
   * @result Coprocessor should be added to table descriptor
   *         Table is disabled to avoid an IOException due to
   *         the added coprocessor not actually existing on disk
   */
  @Test
  @Category(MediumTests.class)
  public void testSchemeAndDirectorywhitelisted() throws Exception {
    negativeTestCase(new String[]{"/tmp","file:///permitted/*"},
        "file:///permitted/couldnotpossiblyexist.jar");
  }

  /**
   * Test a table modification adding a coprocessor path
   * which is whitelisted
   * @result Coprocessor should be added to table descriptor
   *         Table is disabled to avoid an IOException due to
   *         the added coprocessor not actually existing on disk
   */
  @Test
  @Category(MediumTests.class)
  public void testSchemeWhitelisted() throws Exception {
    negativeTestCase(new String[]{"file:///"},
        "file:///permitted/couldnotpossiblyexist.jar");
  }

  /**
   * Test a table modification adding a coprocessor path
   * which is whitelisted
   * @result Coprocessor should be added to table descriptor
   *         Table is disabled to avoid an IOException due to
   *         the added coprocessor not actually existing on disk
   */
  @Test
  @Category(MediumTests.class)
  public void testDFSNameWhitelistedWorks() throws Exception {
    negativeTestCase(new String[]{"hdfs://Your-FileSystem"},
        "hdfs://Your-FileSystem/permitted/couldnotpossiblyexist.jar");
  }

  /**
   * Test a table modification adding a coprocessor path
   * which is whitelisted
   * @result Coprocessor should be added to table descriptor
   *         Table is disabled to avoid an IOException due to
   *         the added coprocessor not actually existing on disk
   */
  @Test
  @Category(MediumTests.class)
  public void testDFSNameNotWhitelistedFails() throws Exception {
    positiveTestCase(new String[]{"hdfs://Your-FileSystem"},
        "hdfs://My-FileSystem/permitted/couldnotpossiblyexist.jar");
  }

  /**
   * Test a table modification adding a coprocessor path
   * which is whitelisted
   * @result Coprocessor should be added to table descriptor
   *         Table is disabled to avoid an IOException due to
   *         the added coprocessor not actually existing on disk
   */
  @Test
  @Category(MediumTests.class)
  public void testBlanketWhitelist() throws Exception {
    negativeTestCase(new String[]{"*"},
        "hdfs:///permitted/couldnotpossiblyexist.jar");
  }

  /**
   * Test a table creation including a coprocessor path
   * which is not whitelisted
   * @result Table will not be created due to the offending coprocessor
   */
  @Test
  @Category(MediumTests.class)
  public void testCreationNonWhitelistedCoprocessorPath() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    // load coprocessor under test
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        CoprocessorWhitelistMasterObserver.class.getName());
    conf.setStrings(CoprocessorWhitelistMasterObserver.CP_COPROCESSOR_WHITELIST_PATHS_KEY,
        new String[]{});
    // set retries low to raise exception quickly
    conf.setInt("hbase.client.retries.number", 1);
    UTIL.startMiniCluster();
    HTableDescriptor htd = new HTableDescriptor(TEST_TABLE);
    HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAMILY);
    htd.addFamily(hcd);
    htd.addCoprocessor("net.clayb.hbase.coprocessor.NotWhitelisted",
      new Path("file:///notpermitted/couldnotpossiblyexist.jar"),
      Coprocessor.PRIORITY_USER, null);
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();
    LOG.info("Creating Table");
    try {
      admin.createTable(htd);
      fail("Expected coprocessor to raise IOException");
    } catch (IOException e) {
      // swallow exception from coprocessor
    }
    LOG.info("Done Creating Table");
    // ensure table was not created
    assertEquals(new HTableDescriptor[0],
      admin.listTables("^" + TEST_TABLE.getNameAsString() + "$"));
  }

  /**
   * Test a table creation including a coprocessor path
   * which is on the classpath
   * @result Table will be created with the coprocessor
   */
  @Test
  @Category(MediumTests.class)
  public void testCreationClasspathCoprocessor() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    // load coprocessor under test
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        CoprocessorWhitelistMasterObserver.class.getName());
    conf.setStrings(CoprocessorWhitelistMasterObserver.CP_COPROCESSOR_WHITELIST_PATHS_KEY,
        new String[]{});
    // set retries low to raise exception quickly
    conf.setInt("hbase.client.retries.number", 1);
    UTIL.startMiniCluster();
    HTableDescriptor htd = new HTableDescriptor(TEST_TABLE);
    HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAMILY);
    htd.addFamily(hcd);
    htd.addCoprocessor("org.apache.hadoop.hbase.coprocessor.BaseRegionObserver");
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();
    LOG.info("Creating Table");
    admin.createTable(htd);
    // ensure table was created and coprocessor is added to table
    LOG.info("Done Creating Table");
    Table t = connection.getTable(TEST_TABLE);
    assertEquals(1, t.getTableDescriptor().getCoprocessors().size());
  }
}
