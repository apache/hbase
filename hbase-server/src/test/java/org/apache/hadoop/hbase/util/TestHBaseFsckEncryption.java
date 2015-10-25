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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.security.Key;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.io.crypto.aes.AES;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.hbck.HFileCorruptionChecker;
import org.apache.hadoop.hbase.util.hbck.HbckTestingUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, LargeTests.class})
public class TestHBaseFsckEncryption {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private Configuration conf;
  private HTableDescriptor htd;
  private Key cfKey;

  @Before
  public void setUp() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.setInt("hfile.format.version", 3);
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");

    // Create the test encryption key
    SecureRandom rng = new SecureRandom();
    byte[] keyBytes = new byte[AES.KEY_LENGTH];
    rng.nextBytes(keyBytes);
    String algorithm =
        conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    cfKey = new SecretKeySpec(keyBytes,algorithm);

    // Start the minicluster
    TEST_UTIL.startMiniCluster(3);

    // Create the table
    htd = new HTableDescriptor(TableName.valueOf("default", "TestHBaseFsckEncryption"));
    HColumnDescriptor hcd = new HColumnDescriptor("cf");
    hcd.setEncryptionType(algorithm);
    hcd.setEncryptionKey(EncryptionUtil.wrapKey(conf,
      conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, User.getCurrent().getShortName()),
      cfKey));
    htd.addFamily(hcd);
    TEST_UTIL.getHBaseAdmin().createTable(htd);
    TEST_UTIL.waitTableAvailable(htd.getName(), 5000);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testFsckWithEncryption() throws Exception {
    // Populate the table with some data
    Table table = TEST_UTIL.getConnection().getTable(htd.getTableName());
    try {
      byte[] values = { 'A', 'B', 'C', 'D' };
      for (int i = 0; i < values.length; i++) {
        for (int j = 0; j < values.length; j++) {
          Put put = new Put(new byte[] { values[i], values[j] });
          put.addColumn(Bytes.toBytes("cf"), new byte[]{}, new byte[]{values[i],
                  values[j]});
          table.put(put);
        }
      }
    } finally {
      table.close();
    }
    // Flush it
    TEST_UTIL.getHBaseAdmin().flush(htd.getTableName());

    // Verify we have encrypted store files on disk
    final List<Path> paths = findStorefilePaths(htd.getTableName());
    assertTrue(paths.size() > 0);
    for (Path path: paths) {
      assertTrue("Store file " + path + " has incorrect key",
        Bytes.equals(cfKey.getEncoded(), extractHFileKey(path)));
    }

    // Insure HBck doesn't consider them corrupt
    HBaseFsck res = HbckTestingUtil.doHFileQuarantine(conf, htd.getTableName());
    assertEquals(res.getRetCode(), 0);
    HFileCorruptionChecker hfcc = res.getHFilecorruptionChecker();
    assertEquals(hfcc.getCorrupted().size(), 0);
    assertEquals(hfcc.getFailures().size(), 0);
    assertEquals(hfcc.getQuarantined().size(), 0);
    assertEquals(hfcc.getMissing().size(), 0);
  }

  private List<Path> findStorefilePaths(TableName tableName) throws Exception {
    List<Path> paths = new ArrayList<Path>();
    for (Region region:
        TEST_UTIL.getRSForFirstRegionInTable(tableName).getOnlineRegions(htd.getTableName())) {
      for (Store store: region.getStores()) {
        for (StoreFile storefile: store.getStorefiles()) {
          paths.add(storefile.getPath());
        }
      }
    }
    return paths;
  }

  private byte[] extractHFileKey(Path path) throws Exception {
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