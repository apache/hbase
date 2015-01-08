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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, MediumTests.class})
public class TestEncryptionRandomKeying {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf = TEST_UTIL.getConfiguration();
  private static HTableDescriptor htd;

  private static List<Path> findStorefilePaths(TableName tableName) throws Exception {
    List<Path> paths = new ArrayList<Path>();
    for (HRegion region:
        TEST_UTIL.getRSForFirstRegionInTable(tableName).getOnlineRegions(htd.getTableName())) {
      for (Store store: region.getStores().values()) {
        for (StoreFile storefile: store.getStorefiles()) {
          paths.add(storefile.getPath());
        }
      }
    }
    return paths;
  }

  private static byte[] extractHFileKey(Path path) throws Exception {
    HFile.Reader reader = HFile.createReader(TEST_UTIL.getTestFileSystem(), path,
      new CacheConfig(conf), conf);
    try {
      reader.loadFileInfo();
      Encryption.Context cryptoContext = reader.getFileContext().getEncryptionContext();
      assertNotNull("Reader has a null crypto context", cryptoContext);
      Key key = cryptoContext.getKey();
      if (key == null) {
        return null;
      }
      return key.getEncoded();
    } finally {
      reader.close();
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    conf.setInt("hfile.format.version", 3);
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");

    // Create the table schema
    // Specify an encryption algorithm without a key
    htd = new HTableDescriptor(TableName.valueOf("default", "TestEncryptionRandomKeying"));
    HColumnDescriptor hcd = new HColumnDescriptor("cf");
    hcd.setEncryptionType("AES");
    htd.addFamily(hcd);

    // Start the minicluster
    TEST_UTIL.startMiniCluster(1);

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

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRandomKeying() throws Exception {
    // Verify we have store file(s) with a random key
    final List<Path> initialPaths = findStorefilePaths(htd.getTableName());
    assertTrue(initialPaths.size() > 0);
    for (Path path: initialPaths) {
      assertNotNull("Store file " + path + " is not encrypted", extractHFileKey(path));
    }
  }

}
