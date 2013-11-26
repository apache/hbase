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
import java.security.Key;
import java.security.SecureRandom;

import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.io.crypto.aes.AES;
import org.apache.hadoop.hbase.io.hfile.HFileReaderV3;
import org.apache.hadoop.hbase.io.hfile.HFileWriterV3;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.SecureProtobufLogReader;
import org.apache.hadoop.hbase.regionserver.wal.SecureProtobufLogWriter;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.junit.Before;
import org.junit.experimental.categories.Category;

@Category(IntegrationTests.class)
public class IntegrationTestIngestWithEncryption extends IntegrationTestIngest {

  private static final Log LOG;
  static {
    Logger.getLogger(HFileReaderV3.class).setLevel(Level.TRACE);
    Logger.getLogger(HFileWriterV3.class).setLevel(Level.TRACE);
    Logger.getLogger(SecureProtobufLogReader.class).setLevel(Level.TRACE);
    Logger.getLogger(SecureProtobufLogWriter.class).setLevel(Level.TRACE);
    LOG = LogFactory.getLog(IntegrationTestIngestWithEncryption.class);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    // Inject test key provider
    // Set up configuration
    IntegrationTestingUtility testUtil = getTestingUtil(conf);
    testUtil.getConfiguration().setInt("hfile.format.version", 3);
    testUtil.getConfiguration().set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY,
      KeyProviderForTesting.class.getName());
    testUtil.getConfiguration().set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
    testUtil.getConfiguration().setClass("hbase.regionserver.hlog.reader.impl",
      SecureProtobufLogReader.class, HLog.Reader.class);
    testUtil.getConfiguration().setClass("hbase.regionserver.hlog.writer.impl",
      SecureProtobufLogWriter.class, HLog.Writer.class);
    testUtil.getConfiguration().setBoolean(HConstants.ENABLE_WAL_ENCRYPTION, true);
    // Flush frequently
    testUtil.getConfiguration().setInt(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, 120000);

    // Initialize the cluster. This invokes LoadTestTool -init_only, which
    // will create the test table, appropriately pre-split
    super.setUp();

    // Create the test encryption key
    SecureRandom rng = new SecureRandom();
    byte[] keyBytes = new byte[AES.KEY_LENGTH];
    rng.nextBytes(keyBytes);
    Key cfKey = new SecretKeySpec(keyBytes, "AES");

    // Update the test table schema so HFiles from this point will be written with
    // encryption features enabled.
    final HBaseAdmin admin = testUtil.getHBaseAdmin();
    HTableDescriptor tableDescriptor =
        new HTableDescriptor(admin.getTableDescriptor(Bytes.toBytes(getTablename())));
    for (HColumnDescriptor columnDescriptor: tableDescriptor.getColumnFamilies()) {
      columnDescriptor.setEncryptionType("AES");
      columnDescriptor.setEncryptionKey(EncryptionUtil.wrapKey(testUtil.getConfiguration(),
        "hbase", cfKey));
      LOG.info("Updating CF schema for " + getTablename() + "." +
        columnDescriptor.getNameAsString());
      admin.disableTable(getTablename());
      admin.modifyColumn(getTablename(), columnDescriptor);
      admin.enableTable(getTablename());
      testUtil.waitFor(10000, 1000, true, new Predicate<IOException>() {
        @Override
        public boolean evaluate() throws IOException {
          return admin.isTableAvailable(getTablename());
        }
      });
    }

  }

}
