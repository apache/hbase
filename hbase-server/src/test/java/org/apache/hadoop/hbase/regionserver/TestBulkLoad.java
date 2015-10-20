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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.StoreDescriptor;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * This class attempts to unit test bulk HLog loading.
 */
@Category(SmallTests.class)
public class TestBulkLoad {

  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();
  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final WAL log = mock(WAL.class);
  private final Configuration conf = HBaseConfiguration.create();
  private final Random random = new Random();
  private final byte[] randomBytes = new byte[100];
  private final byte[] family1 = Bytes.toBytes("family1");
  private final byte[] family2 = Bytes.toBytes("family2");

  @Rule
  public TestName name = new TestName();

  @Before
  public void before() throws IOException {
    random.nextBytes(randomBytes);
    // Mockito.when(log.append(htd, info, key, edits, inMemstore));
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
    when(log.append(any(HRegionInfo.class), any(WALKey.class),
            argThat(bulkLogWalEdit(WALEdit.BULK_LOAD, tableName.toBytes(),
                    familyName, storeFileNames)),
            any(boolean.class))).thenAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) {
        WALKey walKey = invocation.getArgumentAt(1, WALKey.class);
        MultiVersionConcurrencyControl mvcc = walKey.getMvcc();
        if (mvcc != null) {
          MultiVersionConcurrencyControl.WriteEntry we = mvcc.begin();
          walKey.setWriteEntry(we);
        }
        return 01L;
      };
    });
    testRegionWithFamiliesAndSpecifiedTableName(tableName, family1)
        .bulkLoadHFiles(familyPaths, false, null);
    verify(log).sync(anyLong());
  }

  @Test
  public void bulkHLogShouldThrowNoErrorAndWriteMarkerWithBlankInput() throws IOException {
    testRegionWithFamilies(family1).bulkLoadHFiles(new ArrayList<Pair<byte[], String>>(),
      false, null);
  }

  @Test
  public void shouldBulkLoadSingleFamilyHLog() throws IOException {
    when(log.append(any(HRegionInfo.class),
            any(WALKey.class), argThat(bulkLogWalEditType(WALEdit.BULK_LOAD)),
            any(boolean.class))).thenAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) {
        WALKey walKey = invocation.getArgumentAt(1, WALKey.class);
        MultiVersionConcurrencyControl mvcc = walKey.getMvcc();
        if (mvcc != null) {
          MultiVersionConcurrencyControl.WriteEntry we = mvcc.begin();
          walKey.setWriteEntry(we);
        }
        return 01L;
      };
    });
    testRegionWithFamilies(family1).bulkLoadHFiles(withFamilyPathsFor(family1), false, null);
    verify(log).sync(anyLong());
  }

  @Test
  public void shouldBulkLoadManyFamilyHLog() throws IOException {
    when(log.append(any(HRegionInfo.class),
            any(WALKey.class), argThat(bulkLogWalEditType(WALEdit.BULK_LOAD)),
            any(boolean.class))).thenAnswer(new Answer() {
              public Object answer(InvocationOnMock invocation) {
                WALKey walKey = invocation.getArgumentAt(1, WALKey.class);
                MultiVersionConcurrencyControl mvcc = walKey.getMvcc();
                if (mvcc != null) {
                  MultiVersionConcurrencyControl.WriteEntry we = mvcc.begin();
                  walKey.setWriteEntry(we);
                }
                return 01L;
              };
            });
    testRegionWithFamilies(family1, family2).bulkLoadHFiles(withFamilyPathsFor(family1, family2),
            false, null);
    verify(log).sync(anyLong());
  }

  @Test
  public void shouldBulkLoadManyFamilyHLogEvenWhenTableNameNamespaceSpecified() throws IOException {
    when(log.append(any(HRegionInfo.class),
            any(WALKey.class), argThat(bulkLogWalEditType(WALEdit.BULK_LOAD)),
            any(boolean.class))).thenAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) {
        WALKey walKey = invocation.getArgumentAt(1, WALKey.class);
        MultiVersionConcurrencyControl mvcc = walKey.getMvcc();
        if (mvcc != null) {
          MultiVersionConcurrencyControl.WriteEntry we = mvcc.begin();
          walKey.setWriteEntry(we);
        }
        return 01L;
      };
    });
    TableName tableName = TableName.valueOf("test", "test");
    testRegionWithFamiliesAndSpecifiedTableName(tableName, family1, family2)
        .bulkLoadHFiles(withFamilyPathsFor(family1, family2), false, null);
    verify(log).sync(anyLong());
  }

  @Test(expected = DoNotRetryIOException.class)
  public void shouldCrashIfBulkLoadFamiliesNotInTable() throws IOException {
    testRegionWithFamilies(family1).bulkLoadHFiles(withFamilyPathsFor(family1, family2), false,
      null);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void bulkHLogShouldThrowErrorWhenFamilySpecifiedAndHFileExistsButNotInTableDescriptor()
      throws IOException {
    testRegionWithFamilies().bulkLoadHFiles(withFamilyPathsFor(family1), false, null);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void shouldThrowErrorIfBadFamilySpecifiedAsFamilyPath() throws IOException {
    testRegionWithFamilies()
        .bulkLoadHFiles(asList(withInvalidColumnFamilyButProperHFileLocation(family1)),
            false, null);
  }

  @Test(expected = FileNotFoundException.class)
  public void shouldThrowErrorIfHFileDoesNotExist() throws IOException {
    List<Pair<byte[], String>> list = asList(withMissingHFileForFamily(family1));
    testRegionWithFamilies(family1).bulkLoadHFiles(list, false, null);
  }

  private Pair<byte[], String> withMissingHFileForFamily(byte[] family) {
    return new Pair<byte[], String>(family, getNotExistFilePath());
  }

  private String getNotExistFilePath() {
    Path path = new Path(TEST_UTIL.getDataTestDir(), "does_not_exist");
    return path.toUri().getPath();
  }

  private Pair<byte[], String> withInvalidColumnFamilyButProperHFileLocation(byte[] family)
      throws IOException {
    createHFileForFamilies(family);
    return new Pair<byte[], String>(new byte[]{0x00, 0x01, 0x02}, getNotExistFilePath());
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
    return HRegion.createHRegion(conf, new Path(testFolder.newFolder().toURI()),
        hTableDescriptor, hRegionInfo, log);
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
    FSDataOutputStream out = new FSDataOutputStream(new FileOutputStream(hFileLocation), null);
    try {
      hFileFactory.withOutputStream(out);
      hFileFactory.withFileContext(new HFileContext());
      HFile.Writer writer = hFileFactory.create();
      try {
        writer.append(new KeyValue(CellUtil.createCell(randomBytes,
            family,
            randomBytes,
            0l,
            KeyValue.Type.Put.getCode(),
            randomBytes)));
      } finally {
        writer.close();
      }
    } finally {
      out.close();
    }
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
      assertTrue(Arrays.equals(CellUtil.cloneQualifier(item.getCells().get(0)), typeBytes));
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
      }

      return true;
    }

    @Override
    public void describeTo(Description description) {

    }
  }
}
