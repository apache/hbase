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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.security.Key;
import java.util.ArrayList;
import java.util.List;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.io.crypto.aes.AES;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.hbck.HFileCorruptionChecker;
import org.apache.hadoop.hbase.util.hbck.HbckTestingUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, MediumTests.class})
public class TestHBaseFsckEncryption {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseFsckEncryption.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private Configuration conf;
  private TableDescriptor tableDescriptor;
  private Key cfKey;

  @Before
  public void setUp() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.setInt("hfile.format.version", 3);
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");

    // Create the test encryption key
    byte[] keyBytes = new byte[AES.KEY_LENGTH];
    Bytes.secureRandom(keyBytes);
    String algorithm =
        conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    cfKey = new SecretKeySpec(keyBytes,algorithm);

    // Start the minicluster
    TEST_UTIL.startMiniCluster(3);

    // Create the table
    TableDescriptorBuilder tableDescriptorBuilder =
      TableDescriptorBuilder.newBuilder(TableName.valueOf("default", "TestHBaseFsckEncryption"));
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder
        .newBuilder(Bytes.toBytes("cf"))
        .setEncryptionType(algorithm)
        .setEncryptionKey(EncryptionUtil.wrapKey(conf,
          conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, User.getCurrent().getShortName()),
          cfKey)).build();
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
    tableDescriptor = tableDescriptorBuilder.build();
    TEST_UTIL.getAdmin().createTable(tableDescriptor);
    TEST_UTIL.waitTableAvailable(tableDescriptor.getTableName(), 5000);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testFsckWithEncryption() throws Exception {
    // Populate the table with some data
    Table table = TEST_UTIL.getConnection().getTable(tableDescriptor.getTableName());
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
    TEST_UTIL.getAdmin().flush(tableDescriptor.getTableName());

    // Verify we have encrypted store files on disk
    final List<Path> paths = findStorefilePaths(tableDescriptor.getTableName());
    assertTrue(paths.size() > 0);
    for (Path path: paths) {
      assertTrue("Store file " + path + " has incorrect key",
        Bytes.equals(cfKey.getEncoded(), extractHFileKey(path)));
    }

    // Insure HBck doesn't consider them corrupt
    HBaseFsck res = HbckTestingUtil.doHFileQuarantine(conf, tableDescriptor.getTableName());
    assertEquals(0, res.getRetCode());
    HFileCorruptionChecker hfcc = res.getHFilecorruptionChecker();
    assertEquals(0, hfcc.getCorrupted().size());
    assertEquals(0, hfcc.getFailures().size());
    assertEquals(0, hfcc.getQuarantined().size());
    assertEquals(0, hfcc.getMissing().size());
  }

  private List<Path> findStorefilePaths(TableName tableName) throws Exception {
    List<Path> paths = new ArrayList<>();
    for (Region region : TEST_UTIL.getRSForFirstRegionInTable(tableName)
        .getRegions(tableDescriptor.getTableName())) {
      for (HStore store : ((HRegion) region).getStores()) {
        for (HStoreFile storefile : store.getStorefiles()) {
          paths.add(storefile.getPath());
        }
      }
    }
    return paths;
  }

  private byte[] extractHFileKey(Path path) throws Exception {
    HFile.Reader reader = HFile.createReader(TEST_UTIL.getTestFileSystem(), path,
      new CacheConfig(conf), true, conf);
    try {
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
