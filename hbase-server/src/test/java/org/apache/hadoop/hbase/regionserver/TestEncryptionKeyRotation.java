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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.*;

import java.security.Key;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.io.crypto.aes.AES;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, MediumTests.class})
public class TestEncryptionKeyRotation {
  private static final Log LOG = LogFactory.getLog(TestEncryptionKeyRotation.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Configuration conf = TEST_UTIL.getConfiguration();
  private static final Key initialCFKey;
  private static final Key secondCFKey;
  static {
    // Create the test encryption keys
    SecureRandom rng = new SecureRandom();
    byte[] keyBytes = new byte[AES.KEY_LENGTH];
    rng.nextBytes(keyBytes);
    String algorithm =
        conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    initialCFKey = new SecretKeySpec(keyBytes, algorithm);
    rng.nextBytes(keyBytes);
    secondCFKey = new SecretKeySpec(keyBytes, algorithm);
  }

  @BeforeClass
  public static void setUp() throws Exception {
    conf.setInt("hfile.format.version", 3);
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
    // Enable online schema updates
    conf.setBoolean("hbase.online.schema.update.enable", true);

    // Start the minicluster
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCFKeyRotation() throws Exception {
    // Create the table schema
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("default",
      "testCFKeyRotation"));
    HColumnDescriptor hcd = new HColumnDescriptor("cf");
    String algorithm =
        conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    hcd.setEncryptionType(algorithm);
    hcd.setEncryptionKey(EncryptionUtil.wrapKey(conf, "hbase", initialCFKey));
    htd.addFamily(hcd);

    // Create the table and some on disk files
    createTableAndFlush(htd);

    // Verify we have store file(s) with the initial key
    final List<Path> initialPaths = findStorefilePaths(htd.getTableName());
    assertTrue(initialPaths.size() > 0);
    for (Path path: initialPaths) {
      assertTrue("Store file " + path + " has incorrect key",
        Bytes.equals(initialCFKey.getEncoded(), extractHFileKey(path)));
    }

    // Update the schema with a new encryption key
    hcd = htd.getFamily(Bytes.toBytes("cf"));
    hcd.setEncryptionKey(EncryptionUtil.wrapKey(conf,
      conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, User.getCurrent().getShortName()),
      secondCFKey));
    TEST_UTIL.getHBaseAdmin().modifyColumn(htd.getTableName(), hcd);
    Thread.sleep(5000); // Need a predicate for online schema change

    // And major compact
    TEST_UTIL.getHBaseAdmin().majorCompact(htd.getTableName());
    TEST_UTIL.waitFor(30000, 1000, true, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        // When compaction has finished, all of the original files will be
        // gone
        boolean found = false;
        for (Path path: initialPaths) {
          found = TEST_UTIL.getTestFileSystem().exists(path);
          if (found) {
            LOG.info("Found " + path);
            break;
          }
        }
        return !found;
      }
    });

    // Verify we have store file(s) with only the new key
    List<Path> pathsAfterCompaction = findStorefilePaths(htd.getTableName());
    assertTrue(pathsAfterCompaction.size() > 0);
    for (Path path: pathsAfterCompaction) {
      assertFalse("Store file " + path + " retains initial key",
        Bytes.equals(initialCFKey.getEncoded(), extractHFileKey(path)));
      assertTrue("Store file " + path + " has incorrect key",
        Bytes.equals(secondCFKey.getEncoded(), extractHFileKey(path)));
    }
  }

  @Test
  public void testMasterKeyRotation() throws Exception {
    // Create the table schema
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("default",
      "testMasterKeyRotation"));
    HColumnDescriptor hcd = new HColumnDescriptor("cf");
    String algorithm =
        conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    hcd.setEncryptionType(algorithm);
    hcd.setEncryptionKey(EncryptionUtil.wrapKey(conf, "hbase", initialCFKey));
    htd.addFamily(hcd);

    // Create the table and some on disk files
    createTableAndFlush(htd);

    // Verify we have store file(s) with the initial key
    List<Path> storeFilePaths = findStorefilePaths(htd.getTableName());
    assertTrue(storeFilePaths.size() > 0);
    for (Path path: storeFilePaths) {
      assertTrue("Store file " + path + " has incorrect key",
        Bytes.equals(initialCFKey.getEncoded(), extractHFileKey(path)));
    }

    // Now shut down the HBase cluster
    TEST_UTIL.shutdownMiniHBaseCluster();

    // "Rotate" the master key
    conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "other");
    conf.set(HConstants.CRYPTO_MASTERKEY_ALTERNATE_NAME_CONF_KEY, "hbase");

    // Start the cluster back up
    TEST_UTIL.startMiniHBaseCluster(1, 1);
    // Verify the table can still be loaded
    TEST_UTIL.waitTableAvailable(htd.getName(), 5000);
    // Double check that the store file keys can be unwrapped
    storeFilePaths = findStorefilePaths(htd.getTableName());
    assertTrue(storeFilePaths.size() > 0);
    for (Path path: storeFilePaths) {
      assertTrue("Store file " + path + " has incorrect key",
        Bytes.equals(initialCFKey.getEncoded(), extractHFileKey(path)));
    }
  }

  private static List<Path> findStorefilePaths(TableName tableName) throws Exception {
    List<Path> paths = new ArrayList<Path>();
    for (HRegion region:
        TEST_UTIL.getRSForFirstRegionInTable(tableName).getOnlineRegions(tableName)) {
      for (Store store: region.getStores().values()) {
        for (StoreFile storefile: store.getStorefiles()) {
          paths.add(storefile.getPath());
        }
      }
    }
    return paths;
  }

  private void createTableAndFlush(HTableDescriptor htd) throws Exception {
    HColumnDescriptor hcd = htd.getFamilies().iterator().next();
    // Create the test table
    TEST_UTIL.getHBaseAdmin().createTable(htd);
    TEST_UTIL.waitTableAvailable(htd.getName(), 5000);
    // Create a store file
    Table table = TEST_UTIL.getConnection().getTable(htd.getTableName());
    try {
      table.put(new Put(Bytes.toBytes("testrow"))
        .add(hcd.getName(), Bytes.toBytes("q"), Bytes.toBytes("value")));
    } finally {
      table.close();
    }
    TEST_UTIL.getHBaseAdmin().flush(htd.getTableName());
  }

  private static byte[] extractHFileKey(Path path) throws Exception {
    HFile.Reader reader = HFile.createReader(TEST_UTIL.getTestFileSystem(), path,
      new CacheConfig(conf), conf);
    try {
      reader.loadFileInfo();
      Encryption.Context cryptoContext = reader.getFileContext().getEncryptionContext();
      assertNotNull("Reader has a null crypto context", cryptoContext);
      Key key = cryptoContext.getKey();
      assertNotNull("Crypto context has no key", key);
      return key.getEncoded();
    } finally {
      reader.close();
    }
  }

}
