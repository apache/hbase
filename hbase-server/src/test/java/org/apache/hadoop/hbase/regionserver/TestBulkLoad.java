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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.StoreDescriptor;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This class attempts to unit test bulk HLog loading.
 */
@Category(SmallTests.class)
public class TestBulkLoad {

  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();
  @Rule
  public final JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};
  private final WAL log = context.mock(WAL.class);
  private final Configuration conf = HBaseConfiguration.create();
  private final Random random = new Random();
  private final byte[] randomBytes = new byte[100];
  private final byte[] family1 = Bytes.toBytes("family1");
  private final byte[] family2 = Bytes.toBytes("family2");
  private final Expectations callOnce;
  @Rule
  public TestName name = new TestName();
  
  public TestBulkLoad() throws IOException {
    callOnce = new Expectations() {
      {
        oneOf(log).append(with(any(HTableDescriptor.class)), with(any(HRegionInfo.class)),
                with(any(WALKey.class)), with(bulkLogWalEditType(WALEdit.BULK_LOAD)),
                with(any(AtomicLong.class)), with(any(boolean.class)), with(any(List.class)));
        will(returnValue(0l));
        oneOf(log).sync(with(any(long.class)));
      }
    };
  }

  @Before
  public void before() throws IOException {
    random.nextBytes(randomBytes);
  }

  @Test
  public void verifyBulkLoadEvent() throws IOException {
    TableName tableName = TableName.valueOf("test", "test");
    List<Pair<byte[], String>> familyPaths = withFamilyPathsFor(family1);
    byte[] familyName = familyPaths.get(0).getFirst();
    String storeFileName = familyPaths.get(0).getSecond();
    storeFileName = (new Path(storeFileName)).getName();
    List<String> storeFileNames = new ArrayList<String>();
    storeFileNames.add(storeFileName);
    final Matcher<WALEdit> bulkEventMatcher = bulkLogWalEdit(WALEdit.BULK_LOAD,
      tableName.toBytes(), familyName, storeFileNames);
    Expectations expection = new Expectations() {
      {
        oneOf(log).append(with(any(HTableDescriptor.class)), with(any(HRegionInfo.class)),
                with(any(WALKey.class)), with(bulkEventMatcher),
                with(any(AtomicLong.class)), with(any(boolean.class)), with(any(List.class)));
        will(returnValue(0l));
        oneOf(log).sync(with(any(long.class)));
      }
    };
    context.checking(expection);
    testRegionWithFamiliesAndSpecifiedTableName(tableName, family1)
        .bulkLoadHFiles(familyPaths, false);
  }

  @Test
  public void bulkHLogShouldThrowNoErrorAndWriteMarkerWithBlankInput() throws IOException {
    testRegionWithFamilies(family1).bulkLoadHFiles(new ArrayList<Pair<byte[], String>>(), false);
  }

  @Test
  public void shouldBulkLoadSingleFamilyHLog() throws IOException {
    context.checking(callOnce);
    testRegionWithFamilies(family1).bulkLoadHFiles(withFamilyPathsFor(family1), false);
  }

  @Test
  public void shouldBulkLoadManyFamilyHLog() throws IOException {
    context.checking(callOnce);
    testRegionWithFamilies(family1, family2).bulkLoadHFiles(withFamilyPathsFor(family1, family2),
        false);
  }

  @Test
  public void shouldBulkLoadManyFamilyHLogEvenWhenTableNameNamespaceSpecified() throws IOException {
    context.checking(callOnce);
    TableName tableName = TableName.valueOf("test", "test");
    testRegionWithFamiliesAndSpecifiedTableName(tableName, family1, family2)
        .bulkLoadHFiles(withFamilyPathsFor(family1, family2), false);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void shouldCrashIfBulkLoadFamiliesNotInTable() throws IOException {
    testRegionWithFamilies(family1).bulkLoadHFiles(withFamilyPathsFor(family1, family2), false);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void bulkHLogShouldThrowErrorWhenFamilySpecifiedAndHFileExistsButNotInTableDescriptor()
      throws IOException {
    testRegionWithFamilies().bulkLoadHFiles(withFamilyPathsFor(family1), false);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void shouldThrowErrorIfBadFamilySpecifiedAsFamilyPath() throws IOException {
    testRegionWithFamilies()
        .bulkLoadHFiles(asList(withInvalidColumnFamilyButProperHFileLocation(family1)),
            false);
  }

  @Test(expected = FileNotFoundException.class)
  public void shouldThrowErrorIfHFileDoesNotExist() throws IOException {
    List<Pair<byte[], String>> list = asList(withMissingHFileForFamily(family1));
    testRegionWithFamilies(family1).bulkLoadHFiles(list, false);
  }

  private Pair<byte[], String> withMissingHFileForFamily(byte[] family) {
    return new Pair<byte[], String>(family, "/tmp/does_not_exist");
  }

  private Pair<byte[], String> withInvalidColumnFamilyButProperHFileLocation(byte[] family)
      throws IOException {
    createHFileForFamilies(family);
    return new Pair<byte[], String>(new byte[]{0x00, 0x01, 0x02}, "/tmp/does_not_exist");
  }


  private HRegion testRegionWithFamiliesAndSpecifiedTableName(TableName tableName,
                                                              byte[]... families)
  throws IOException {
    HRegionInfo hRegionInfo = new HRegionInfo(tableName);
    HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
    for (byte[] family : families) {
      hTableDescriptor.addFamily(new HColumnDescriptor(family));
    }

    // TODO We need a way to do this without creating files
    return HRegion.createHRegion(hRegionInfo,
        new Path(testFolder.newFolder().toURI()),
        conf,
        hTableDescriptor,
        log);

  }

  private HRegion testRegionWithFamilies(byte[]... families) throws IOException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    return testRegionWithFamiliesAndSpecifiedTableName(tableName, families);
  }

  private List<Pair<byte[], String>> getBlankFamilyPaths(){
    return new ArrayList<Pair<byte[], String>>();
  }

  private List<Pair<byte[], String>> withFamilyPathsFor(byte[]... families) throws IOException {
    List<Pair<byte[], String>> familyPaths = getBlankFamilyPaths();
    for (byte[] family : families) {
      familyPaths.add(new Pair<byte[], String>(family, createHFileForFamilies(family)));
    }
    return familyPaths;
  }

  private String createHFileForFamilies(byte[] family) throws IOException {
    HFile.WriterFactory hFileFactory = HFile.getWriterFactoryNoCache(conf);
    // TODO We need a way to do this without creating files
    File hFileLocation = testFolder.newFile();
    hFileFactory.withOutputStream(new FSDataOutputStream(new FileOutputStream(hFileLocation)));
    hFileFactory.withFileContext(new HFileContext());
    HFile.Writer writer = hFileFactory.create();

    writer.append(new KeyValue(CellUtil.createCell(randomBytes,
        family,
        randomBytes,
        0l,
        KeyValue.Type.Put.getCode(),
        randomBytes)));
    writer.close();
    return hFileLocation.getAbsoluteFile().getAbsolutePath();
  }

  private static Matcher<WALEdit> bulkLogWalEditType(byte[] typeBytes) {
    return new WalMatcher(typeBytes);
  }

  private static Matcher<WALEdit> bulkLogWalEdit(byte[] typeBytes, byte[] tableName,
      byte[] familyName, List<String> storeFileNames) {
    return new WalMatcher(typeBytes, tableName, familyName, storeFileNames);
  }

  private static class WalMatcher extends TypeSafeMatcher<WALEdit> {
    private final byte[] typeBytes;
    private final byte[] tableName;
    private final byte[] familyName;
    private final List<String> storeFileNames;

    public WalMatcher(byte[] typeBytes) {
      this(typeBytes, null, null, null);
    }

    public WalMatcher(byte[] typeBytes, byte[] tableName, byte[] familyName,
        List<String> storeFileNames) {
      this.typeBytes = typeBytes;
      this.tableName = tableName;
      this.familyName = familyName;
      this.storeFileNames = storeFileNames;
    }

    @Override
    protected boolean matchesSafely(WALEdit item) {
      assertTrue(Arrays.equals(item.getCells().get(0).getQualifier(), typeBytes));
      BulkLoadDescriptor desc;
      try {
        desc = WALEdit.getBulkLoadDescriptor(item.getCells().get(0));
      } catch (IOException e) {
        return false;
      }
      assertNotNull(desc);

      if (tableName != null) {
        assertTrue(Bytes.equals(ProtobufUtil.toTableName(desc.getTableName()).getName(), 
          tableName));
      }

      if(storeFileNames != null) {
        int index=0;
        StoreDescriptor store = desc.getStores(0);
        assertTrue(Bytes.equals(store.getFamilyName().toByteArray(), familyName));
        assertTrue(Bytes.equals(Bytes.toBytes(store.getStoreHomeDir()), familyName));
        assertEquals(storeFileNames.size(), store.getStoreFileCount());
        for (String storeFile : store.getStoreFileList()) {
          assertTrue(storeFile.equals(storeFileNames.get(index++)));
        }
      }
      
      return true;
    }

    @Override
    public void describeTo(Description description) {

    }
  }
}
