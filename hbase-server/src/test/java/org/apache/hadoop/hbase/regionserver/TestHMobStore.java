/**
 *
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.io.crypto.aes.AES;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.security.Key;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;

@Category(MediumTests.class)
public class TestHMobStore {
  public static final Log LOG = LogFactory.getLog(TestHMobStore.class);
  @Rule public TestName name = new TestName();

  private HMobStore store;
  private HRegion region;
  private HColumnDescriptor hcd;
  private FileSystem fs;
  private byte [] table = Bytes.toBytes("table");
  private byte [] family = Bytes.toBytes("family");
  private byte [] row = Bytes.toBytes("row");
  private byte [] row2 = Bytes.toBytes("row2");
  private byte [] qf1 = Bytes.toBytes("qf1");
  private byte [] qf2 = Bytes.toBytes("qf2");
  private byte [] qf3 = Bytes.toBytes("qf3");
  private byte [] qf4 = Bytes.toBytes("qf4");
  private byte [] qf5 = Bytes.toBytes("qf5");
  private byte [] qf6 = Bytes.toBytes("qf6");
  private byte[] value = Bytes.toBytes("value");
  private byte[] value2 = Bytes.toBytes("value2");
  private Path mobFilePath;
  private Date currentDate = new Date();
  private KeyValue seekKey1;
  private KeyValue seekKey2;
  private KeyValue seekKey3;
  private NavigableSet<byte[]> qualifiers =
    new ConcurrentSkipListSet<byte[]>(Bytes.BYTES_COMPARATOR);
  private List<Cell> expected = new ArrayList<Cell>();
  private long id = System.currentTimeMillis();
  private Get get = new Get(row);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final String DIR = TEST_UTIL.getDataTestDir("TestHMobStore").toString();

  /**
   * Setup
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception {
    qualifiers.add(qf1);
    qualifiers.add(qf3);
    qualifiers.add(qf5);

    Iterator<byte[]> iter = qualifiers.iterator();
    while(iter.hasNext()){
      byte [] next = iter.next();
      expected.add(new KeyValue(row, family, next, 1, value));
      get.addColumn(family, next);
      get.setMaxVersions(); // all versions.
    }
  }

  private void init(String methodName, Configuration conf, boolean testStore)
  throws IOException {
    hcd = new HColumnDescriptor(family);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(3L);
    hcd.setMaxVersions(4);
    init(methodName, conf, hcd, testStore);
  }

  private void init(String methodName, Configuration conf,
      HColumnDescriptor hcd, boolean testStore) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(table));
    init(methodName, conf, htd, hcd, testStore);
  }

  private void init(String methodName, Configuration conf, HTableDescriptor htd,
      HColumnDescriptor hcd, boolean testStore) throws IOException {
    //Setting up tje Region and Store
    Path basedir = new Path(DIR+methodName);
    Path tableDir = FSUtils.getTableDir(basedir, htd.getTableName());
    String logName = "logs";
    Path logdir = new Path(basedir, logName);
    FileSystem fs = FileSystem.get(conf);
    fs.delete(logdir, true);

    htd.addFamily(hcd);
    HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);

    final Configuration walConf = new Configuration(conf);
    FSUtils.setRootDir(walConf, basedir);
    final WALFactory wals = new WALFactory(walConf, null, methodName);
    region = new HRegion(tableDir, wals.getWAL(info.getEncodedNameAsBytes(),
            info.getTable().getNamespace()), fs, conf, info, htd, null);
    store = new HMobStore(region, hcd, conf);
    if(testStore) {
      init(conf, hcd);
    }
  }

  private void init(Configuration conf, HColumnDescriptor hcd)
      throws IOException {
    Path basedir = FSUtils.getRootDir(conf);
    fs = FileSystem.get(conf);
    Path homePath = new Path(basedir, Bytes.toString(family) + Path.SEPARATOR
        + Bytes.toString(family));
    fs.mkdirs(homePath);

    KeyValue key1 = new KeyValue(row, family, qf1, 1, value);
    KeyValue key2 = new KeyValue(row, family, qf2, 1, value);
    KeyValue key3 = new KeyValue(row2, family, qf3, 1, value2);
    KeyValue[] keys = new KeyValue[] { key1, key2, key3 };
    int maxKeyCount = keys.length;
    StoreFileWriter mobWriter = store.createWriterInTmp(currentDate, maxKeyCount,
        hcd.getCompactionCompression(), region.getRegionInfo().getStartKey());
    mobFilePath = mobWriter.getPath();

    mobWriter.append(key1);
    mobWriter.append(key2);
    mobWriter.append(key3);
    mobWriter.close();

    String targetPathName = MobUtils.formatDate(currentDate);
    byte[] referenceValue = Bytes.toBytes(targetPathName + Path.SEPARATOR + mobFilePath.getName());
    Tag tableNameTag = new ArrayBackedTag(TagType.MOB_TABLE_NAME_TAG_TYPE,
        store.getTableName().getName());
    KeyValue kv1 = new KeyValue(row, family, qf1, Long.MAX_VALUE, referenceValue);
    KeyValue kv2 = new KeyValue(row, family, qf2, Long.MAX_VALUE, referenceValue);
    KeyValue kv3 = new KeyValue(row2, family, qf3, Long.MAX_VALUE, referenceValue);
    seekKey1 = MobUtils.createMobRefKeyValue(kv1, referenceValue, tableNameTag);
    seekKey2 = MobUtils.createMobRefKeyValue(kv2, referenceValue, tableNameTag);
    seekKey3 = MobUtils.createMobRefKeyValue(kv3, referenceValue, tableNameTag);
  }

  /**
   * Getting data from memstore
   * @throws IOException
   */
  @Test
  public void testGetFromMemStore() throws IOException {
    final Configuration conf = HBaseConfiguration.create();
    init(name.getMethodName(), conf, false);

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, 1, value));
    this.store.add(new KeyValue(row, family, qf2, 1, value));
    this.store.add(new KeyValue(row, family, qf3, 1, value));
    this.store.add(new KeyValue(row, family, qf4, 1, value));
    this.store.add(new KeyValue(row, family, qf5, 1, value));
    this.store.add(new KeyValue(row, family, qf6, 1, value));

    Scan scan = new Scan(get);
    InternalScanner scanner = (InternalScanner) store.getScanner(scan,
        scan.getFamilyMap().get(store.getFamily().getName()),
        0);

    List<Cell> results = new ArrayList<Cell>();
    scanner.next(results);
    Collections.sort(results, KeyValue.COMPARATOR);
    scanner.close();

    //Compare
    Assert.assertEquals(expected.size(), results.size());
    for(int i=0; i<results.size(); i++) {
      // Verify the values
      Assert.assertEquals(expected.get(i), results.get(i));
    }
  }

  /**
   * Getting MOB data from files
   * @throws IOException
   */
  @Test
  public void testGetFromFiles() throws IOException {
    final Configuration conf = TEST_UTIL.getConfiguration();
    init(name.getMethodName(), conf, false);

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, 1, value));
    this.store.add(new KeyValue(row, family, qf2, 1, value));
    //flush
    flush(1);

    //Add more data
    this.store.add(new KeyValue(row, family, qf3, 1, value));
    this.store.add(new KeyValue(row, family, qf4, 1, value));
    //flush
    flush(2);

    //Add more data
    this.store.add(new KeyValue(row, family, qf5, 1, value));
    this.store.add(new KeyValue(row, family, qf6, 1, value));
    //flush
    flush(3);

    Scan scan = new Scan(get);
    InternalScanner scanner = (InternalScanner) store.getScanner(scan,
        scan.getFamilyMap().get(store.getFamily().getName()),
        0);

    List<Cell> results = new ArrayList<Cell>();
    scanner.next(results);
    Collections.sort(results, KeyValue.COMPARATOR);
    scanner.close();

    //Compare
    Assert.assertEquals(expected.size(), results.size());
    for(int i=0; i<results.size(); i++) {
      Assert.assertEquals(expected.get(i), results.get(i));
    }
  }

  /**
   * Getting the reference data from files
   * @throws IOException
   */
  @Test
  public void testGetReferencesFromFiles() throws IOException {
    final Configuration conf = HBaseConfiguration.create();
    init(name.getMethodName(), conf, false);

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, 1, value));
    this.store.add(new KeyValue(row, family, qf2, 1, value));
    //flush
    flush(1);

    //Add more data
    this.store.add(new KeyValue(row, family, qf3, 1, value));
    this.store.add(new KeyValue(row, family, qf4, 1, value));
    //flush
    flush(2);

    //Add more data
    this.store.add(new KeyValue(row, family, qf5, 1, value));
    this.store.add(new KeyValue(row, family, qf6, 1, value));
    //flush
    flush(3);

    Scan scan = new Scan(get);
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    InternalScanner scanner = (InternalScanner) store.getScanner(scan,
      scan.getFamilyMap().get(store.getFamily().getName()),
      0);

    List<Cell> results = new ArrayList<Cell>();
    scanner.next(results);
    Collections.sort(results, KeyValue.COMPARATOR);
    scanner.close();

    //Compare
    Assert.assertEquals(expected.size(), results.size());
    for(int i=0; i<results.size(); i++) {
      Cell cell = results.get(i);
      Assert.assertTrue(MobUtils.isMobReferenceCell(cell));
    }
  }

  /**
   * Getting data from memstore and files
   * @throws IOException
   */
  @Test
  public void testGetFromMemStoreAndFiles() throws IOException {

    final Configuration conf = HBaseConfiguration.create();

    init(name.getMethodName(), conf, false);

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, 1, value));
    this.store.add(new KeyValue(row, family, qf2, 1, value));
    //flush
    flush(1);

    //Add more data
    this.store.add(new KeyValue(row, family, qf3, 1, value));
    this.store.add(new KeyValue(row, family, qf4, 1, value));
    //flush
    flush(2);

    //Add more data
    this.store.add(new KeyValue(row, family, qf5, 1, value));
    this.store.add(new KeyValue(row, family, qf6, 1, value));

    Scan scan = new Scan(get);
    InternalScanner scanner = (InternalScanner) store.getScanner(scan,
        scan.getFamilyMap().get(store.getFamily().getName()),
        0);

    List<Cell> results = new ArrayList<Cell>();
    scanner.next(results);
    Collections.sort(results, KeyValue.COMPARATOR);
    scanner.close();

    //Compare
    Assert.assertEquals(expected.size(), results.size());
    for(int i=0; i<results.size(); i++) {
      Assert.assertEquals(expected.get(i), results.get(i));
    }
  }

  /**
   * Getting data from memstore and files
   * @throws IOException
   */
  @Test
  public void testMobCellSizeThreshold() throws IOException {

    final Configuration conf = HBaseConfiguration.create();

    HColumnDescriptor hcd;
    hcd = new HColumnDescriptor(family);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(100);
    hcd.setMaxVersions(4);
    init(name.getMethodName(), conf, hcd, false);

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, 1, value));
    this.store.add(new KeyValue(row, family, qf2, 1, value));
    //flush
    flush(1);

    //Add more data
    this.store.add(new KeyValue(row, family, qf3, 1, value));
    this.store.add(new KeyValue(row, family, qf4, 1, value));
    //flush
    flush(2);

    //Add more data
    this.store.add(new KeyValue(row, family, qf5, 1, value));
    this.store.add(new KeyValue(row, family, qf6, 1, value));
    //flush
    flush(3);

    Scan scan = new Scan(get);
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    InternalScanner scanner = (InternalScanner) store.getScanner(scan,
      scan.getFamilyMap().get(store.getFamily().getName()),
      0);

    List<Cell> results = new ArrayList<Cell>();
    scanner.next(results);
    Collections.sort(results, KeyValue.COMPARATOR);
    scanner.close();

    //Compare
    Assert.assertEquals(expected.size(), results.size());
    for(int i=0; i<results.size(); i++) {
      Cell cell = results.get(i);
      //this is not mob reference cell.
      Assert.assertFalse(MobUtils.isMobReferenceCell(cell));
      Assert.assertEquals(expected.get(i), results.get(i));
      Assert.assertEquals(100, store.getFamily().getMobThreshold());
    }
  }

  @Test
  public void testCommitFile() throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    init(name.getMethodName(), conf, true);
    String targetPathName = MobUtils.formatDate(new Date());
    Path targetPath = new Path(store.getPath(), (targetPathName
        + Path.SEPARATOR + mobFilePath.getName()));
    fs.delete(targetPath, true);
    Assert.assertFalse(fs.exists(targetPath));
    //commit file
    store.commitFile(mobFilePath, targetPath);
    Assert.assertTrue(fs.exists(targetPath));
  }

  @Test
  public void testResolve() throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    init(name.getMethodName(), conf, true);
    String targetPathName = MobUtils.formatDate(currentDate);
    Path targetPath = new Path(store.getPath(), targetPathName);
    store.commitFile(mobFilePath, targetPath);
    //resolve
    Cell resultCell1 = store.resolve(seekKey1, false);
    Cell resultCell2 = store.resolve(seekKey2, false);
    Cell resultCell3 = store.resolve(seekKey3, false);
    //compare
    Assert.assertEquals(Bytes.toString(value),
        Bytes.toString(CellUtil.cloneValue(resultCell1)));
    Assert.assertEquals(Bytes.toString(value),
        Bytes.toString(CellUtil.cloneValue(resultCell2)));
    Assert.assertEquals(Bytes.toString(value2),
        Bytes.toString(CellUtil.cloneValue(resultCell3)));
  }

  /**
   * Flush the memstore
   * @param storeFilesSize
   * @throws IOException
   */
  private void flush(int storeFilesSize) throws IOException{
    this.store.snapshot();
    flushStore(store, id++);
    Assert.assertEquals(storeFilesSize, this.store.getStorefiles().size());
    Assert.assertEquals(0, ((AbstractMemStore)this.store.memstore).getActive().getCellsCount());
  }

  /**
   * Flush the memstore
   * @param store
   * @param id
   * @throws IOException
   */
  private static void flushStore(HMobStore store, long id) throws IOException {
    StoreFlushContext storeFlushCtx = store.createFlushContext(id);
    storeFlushCtx.prepare();
    storeFlushCtx.flushCache(Mockito.mock(MonitoredTask.class));
    storeFlushCtx.commit(Mockito.mock(MonitoredTask.class));
  }

  @Test
  public void testMOBStoreEncryption() throws Exception {
    final Configuration conf = TEST_UTIL.getConfiguration();

    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
    SecureRandom rng = new SecureRandom();
    byte[] keyBytes = new byte[AES.KEY_LENGTH];
    rng.nextBytes(keyBytes);
    String algorithm = conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    Key cfKey = new SecretKeySpec(keyBytes, algorithm);

    HColumnDescriptor hcd = new HColumnDescriptor(family);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(100);
    hcd.setMaxVersions(4);
    hcd.setEncryptionType(algorithm);
    hcd.setEncryptionKey(EncryptionUtil.wrapKey(conf,
      conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, User.getCurrent().getShortName()),cfKey));

    init(name.getMethodName(), conf, hcd, false);

    this.store.add(new KeyValue(row, family, qf1, 1, value));
    this.store.add(new KeyValue(row, family, qf2, 1, value));
    this.store.add(new KeyValue(row, family, qf3, 1, value));
    flush(1);

    this.store.add(new KeyValue(row, family, qf4, 1, value));
    this.store.add(new KeyValue(row, family, qf5, 1, value));
    this.store.add(new KeyValue(row, family, qf6, 1, value));
    flush(2);

    Collection<StoreFile> storefiles = this.store.getStorefiles();
    checkMobHFileEncrytption(storefiles);

    // Scan the values
    Scan scan = new Scan(get);
    InternalScanner scanner = (InternalScanner) store.getScanner(scan,
        scan.getFamilyMap().get(store.getFamily().getName()),
        0);

    List<Cell> results = new ArrayList<Cell>();
    scanner.next(results);
    Collections.sort(results, KeyValue.COMPARATOR);
    scanner.close();
    Assert.assertEquals(expected.size(), results.size());
    for(int i=0; i<results.size(); i++) {
      Assert.assertEquals(expected.get(i), results.get(i));
    }

    // Trigger major compaction
    this.store.triggerMajorCompaction();
    CompactionContext requestCompaction = this.store.requestCompaction(1, null);
    this.store.compact(requestCompaction, NoLimitThroughputController.INSTANCE);
    Assert.assertEquals(1, this.store.getStorefiles().size());

    //Check encryption after compaction
    checkMobHFileEncrytption(this.store.getStorefiles());
  }

  private void checkMobHFileEncrytption(Collection<StoreFile> storefiles) {
    StoreFile storeFile = storefiles.iterator().next();
    HFile.Reader reader = storeFile.getReader().getHFileReader();
    byte[] encryptionKey = reader.getTrailer().getEncryptionKey();
    Assert.assertTrue(null != encryptionKey);
    Assert.assertTrue(reader.getFileContext().getEncryptionContext().getCipher().getName()
        .equals(HConstants.CIPHER_AES));
  }

}
