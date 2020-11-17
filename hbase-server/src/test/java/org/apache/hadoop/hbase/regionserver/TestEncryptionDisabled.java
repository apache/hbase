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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.TableDescriptorChecker;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

@Category({MasterTests.class, MediumTests.class})
public class TestEncryptionDisabled {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestEncryptionDisabled.class);

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf = TEST_UTIL.getConfiguration();
  private static TableDescriptorBuilder tdb;


  @BeforeClass
  public static void setUp() throws Exception {
    conf.setInt("hfile.format.version", 3);
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
    conf.set(Encryption.CRYPTO_ENABLED_CONF_KEY, "false");
    conf.set(TableDescriptorChecker.TABLE_SANITY_CHECKS, "true");

    // Start the minicluster
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testEncryptedTableShouldNotBeCreatedWhenEncryptionDisabled() throws Exception {
    // Create the table schema
    // Specify an encryption algorithm without a key (normally HBase would generate a random key)
    tdb = TableDescriptorBuilder.newBuilder(TableName.valueOf("default",
      "TestEncryptionDisabledFail"));
    ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf"));
    String algorithm = conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    columnFamilyDescriptorBuilder.setEncryptionType(algorithm);
    tdb.setColumnFamily(columnFamilyDescriptorBuilder.build());

    // Create the test table, we expect to get back an exception
    exception.expect(DoNotRetryIOException.class);
    exception.expectMessage("encryption is disabled on the cluster");
    TEST_UTIL.getAdmin().createTable(tdb.build());
  }

  @Test
  public void testNonEncryptedTableShouldBeCreatedWhenEncryptionDisabled() throws Exception {
    // Create the table schema
    tdb = TableDescriptorBuilder.newBuilder(TableName.valueOf("default",
      "TestEncryptionDisabledSuccess"));
    ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf"));
    tdb.setColumnFamily(columnFamilyDescriptorBuilder.build());

    // Create the test table, this should succeed, as we don't use encryption
    TEST_UTIL.getAdmin().createTable(tdb.build());
    TEST_UTIL.waitTableAvailable(tdb.build().getTableName(), 5000);
  }

}
