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
package org.apache.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;
import org.apache.hadoop.hbase.regionserver.wal.SecureProtobufLogReader;
import org.apache.hadoop.hbase.regionserver.wal.SecureProtobufLogWriter;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.EncryptionTest;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IntegrationTests.class)
public class IntegrationTestIngestWithEncryption extends IntegrationTestIngest {
  private final static Logger LOG =
      LoggerFactory.getLogger(IntegrationTestIngestWithEncryption.class);

  boolean initialized = false;

  @Override
  public void setUpCluster() throws Exception {
    util = getTestingUtil(null);
    Configuration conf = util.getConfiguration();
    if (!util.isDistributedCluster()) {
      // Inject required configuration if we are not running in distributed mode
      conf.setInt(HFile.FORMAT_VERSION_KEY, 3);
      conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
      conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
      conf.setClass("hbase.regionserver.hlog.reader.impl", SecureProtobufLogReader.class,
        Reader.class);
      conf.setClass("hbase.regionserver.hlog.writer.impl", SecureProtobufLogWriter.class,
        Writer.class);
      conf.setBoolean(HConstants.ENABLE_WAL_ENCRYPTION, true);
    }
    // Check if the cluster configuration can support this test
    try {
      EncryptionTest.testEncryption(conf, "AES", null);
    } catch (Exception e) {
      LOG.warn("Encryption configuration test did not pass, skipping test", e);
      return;
    }
    super.setUpCluster();
    initialized = true;
  }

  @Before
  @Override
  public void setUp() throws Exception {
    // Initialize the cluster. This invokes LoadTestTool -init_only, which
    // will create the test table, appropriately pre-split
    super.setUp();

    if (!initialized) {
      return;
    }

    // Update the test table schema so HFiles from this point will be written with
    // encryption features enabled.
    final Admin admin = util.getAdmin();
    TableDescriptor tableDescriptor = admin.getDescriptor(getTablename());
    for (ColumnFamilyDescriptor columnDescriptor : tableDescriptor.getColumnFamilies()) {
      ColumnFamilyDescriptor updatedColumn = ColumnFamilyDescriptorBuilder
          .newBuilder(columnDescriptor).setEncryptionType("AES").build();
      LOG.info(
        "Updating CF schema for " + getTablename() + "." + columnDescriptor.getNameAsString());
      admin.disableTable(getTablename());
      admin.modifyColumnFamily(getTablename(), updatedColumn);
      admin.enableTable(getTablename());
      util.waitFor(30000, 1000, true, new Predicate<IOException>() {
        @Override
        public boolean evaluate() throws IOException {
          return admin.isTableAvailable(getTablename());
        }
      });
    }
  }

  @Override
  public int runTestFromCommandLine() throws Exception {
    if (!initialized) {
      return 0;
    }
    return super.runTestFromCommandLine();
  }

  @Override
  public void cleanUp() throws Exception {
    if (!initialized) {
      return;
    }
    super.cleanUp();
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf, new IntegrationTestIngestWithEncryption(), args);
    System.exit(ret);
  }
}
