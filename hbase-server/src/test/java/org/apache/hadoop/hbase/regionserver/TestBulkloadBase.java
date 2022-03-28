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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestBulkloadBase {
  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();
  private static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  protected final WAL log = mock(WAL.class);
  protected final Configuration conf = HBaseConfiguration.create();
  private final byte[] randomBytes = new byte[100];
  protected final byte[] family1 = Bytes.toBytes("family1");
  protected final byte[] family2 = Bytes.toBytes("family2");
  protected final byte[] family3 = Bytes.toBytes("family3");

  protected Boolean useFileBasedSFT;

  @Rule
  public TestName name = new TestName();

  public TestBulkloadBase(boolean useFileBasedSFT) {
    this.useFileBasedSFT = useFileBasedSFT;
  }

  @Parameterized.Parameters
  public static Collection<Boolean> data() {
    Boolean[] data = {false, true};
    return Arrays.asList(data);
  }

  @Before
  public void before() throws IOException {
    Bytes.random(randomBytes);
    if(useFileBasedSFT) {
      conf.set(StoreFileTrackerFactory.TRACKER_IMPL,
        "org.apache.hadoop.hbase.regionserver.storefiletracker.FileBasedStoreFileTracker");
    }
    else {
      conf.unset(StoreFileTrackerFactory.TRACKER_IMPL);
    }
  }

  protected Pair<byte[], String> withMissingHFileForFamily(byte[] family) {
    return new Pair<>(family, getNotExistFilePath());
  }

  private String getNotExistFilePath() {
    Path path = new Path(TEST_UTIL.getDataTestDir(), "does_not_exist");
    return path.toUri().getPath();
  }

  protected Pair<byte[], String> withInvalidColumnFamilyButProperHFileLocation(byte[] family)
      throws IOException {
    createHFileForFamilies(family);
    return new Pair<>(new byte[] { 0x00, 0x01, 0x02 }, getNotExistFilePath());
  }

  protected HRegion testRegionWithFamiliesAndSpecifiedTableName(TableName tableName,
      byte[]... families) throws IOException {
    RegionInfo hRegionInfo = RegionInfoBuilder.newBuilder(tableName).build();
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);

    for (byte[] family : families) {
      builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(family));
    }
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0, 0, null,
      MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    // TODO We need a way to do this without creating files
    return HRegion.createHRegion(hRegionInfo, new Path(testFolder.newFolder().toURI()), conf,
      builder.build(), log);

  }

  protected HRegion testRegionWithFamilies(byte[]... families) throws IOException {
    TableName tableName = TableName.valueOf(name.getMethodName().substring(0, name.getMethodName().indexOf("[")));
    return testRegionWithFamiliesAndSpecifiedTableName(tableName, families);
  }

  private List<Pair<byte[], String>> getBlankFamilyPaths() {
    return new ArrayList<>();
  }

  protected List<Pair<byte[], String>> withFamilyPathsFor(byte[]... families) throws IOException {
    List<Pair<byte[], String>> familyPaths = getBlankFamilyPaths();
    for (byte[] family : families) {
      familyPaths.add(new Pair<>(family, createHFileForFamilies(family)));
    }
    return familyPaths;
  }

  private String createHFileForFamilies(byte[] family) throws IOException {
    HFile.WriterFactory hFileFactory = HFile.getWriterFactoryNoCache(conf);
    // TODO We need a way to do this without creating files
    File hFileLocation = testFolder.newFile(generateUniqueName(null));
    FSDataOutputStream out = new FSDataOutputStream(new FileOutputStream(hFileLocation), null);
    try {
      hFileFactory.withOutputStream(out);
      hFileFactory.withFileContext(new HFileContextBuilder().build());
      HFile.Writer writer = hFileFactory.create();
      try {
        writer.append(new KeyValue(ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
            .setRow(randomBytes).setFamily(family).setQualifier(randomBytes).setTimestamp(0L)
            .setType(KeyValue.Type.Put.getCode()).setValue(randomBytes).build()));
      } finally {
        writer.close();
      }
    } finally {
      out.close();
    }
    return hFileLocation.getAbsoluteFile().getAbsolutePath();
  }

  private static String generateUniqueName(final String suffix) {
    String name = UUID.randomUUID().toString().replaceAll("-", "");
    if (suffix != null) name += suffix;
    return name;
  }

  protected static Matcher<WALEdit> bulkLogWalEditType(byte[] typeBytes) {
    return new WalMatcher(typeBytes);
  }

  protected static Matcher<WALEdit> bulkLogWalEdit(byte[] typeBytes, byte[] tableName,
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
      WALProtos.BulkLoadDescriptor desc;
      try {
        desc = WALEdit.getBulkLoadDescriptor(item.getCells().get(0));
      } catch (IOException e) {
        return false;
      }
      assertNotNull(desc);

      if (tableName != null) {
        assertTrue(
          Bytes.equals(ProtobufUtil.toTableName(desc.getTableName()).getName(), tableName));
      }

      if (storeFileNames != null) {
        int index = 0;
        WALProtos.StoreDescriptor store = desc.getStores(0);
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
