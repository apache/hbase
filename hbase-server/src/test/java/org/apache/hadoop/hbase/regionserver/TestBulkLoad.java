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

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


/**
 * This class attempts to unit test bulk HLog loading.
 */
@Category(SmallTests.class)
public class TestBulkLoad extends TestBulkloadBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBulkLoad.class);

  public TestBulkLoad(boolean useFileBasedSFT) {
    super(useFileBasedSFT);
  }

  @Test
  public void verifyBulkLoadEvent() throws IOException {
    TableName tableName = TableName.valueOf("test", "test");
    List<Pair<byte[], String>> familyPaths = withFamilyPathsFor(family1);
    byte[] familyName = familyPaths.get(0).getFirst();
    String storeFileName = familyPaths.get(0).getSecond();
    storeFileName = (new Path(storeFileName)).getName();
    List<String> storeFileNames = new ArrayList<>();
    storeFileNames.add(storeFileName);
    when(log.appendMarker(any(), any(),
      argThat(bulkLogWalEdit(WALEdit.BULK_LOAD, tableName.toBytes(), familyName, storeFileNames))))
        .thenAnswer(new Answer() {
          @Override
          public Object answer(InvocationOnMock invocation) {
            WALKeyImpl walKey = invocation.getArgument(1);
            MultiVersionConcurrencyControl mvcc = walKey.getMvcc();
            if (mvcc != null) {
              MultiVersionConcurrencyControl.WriteEntry we = mvcc.begin();
              walKey.setWriteEntry(we);
            }
            return 01L;
          }
        });
    testRegionWithFamiliesAndSpecifiedTableName(tableName, family1)
        .bulkLoadHFiles(familyPaths, false, null);
    verify(log).sync(anyLong());
  }

  @Test
  public void bulkHLogShouldThrowNoErrorAndWriteMarkerWithBlankInput() throws IOException {
    testRegionWithFamilies(family1).bulkLoadHFiles(new ArrayList<>(),false, null);
  }

  @Test
  public void shouldBulkLoadSingleFamilyHLog() throws IOException {
    when(log.appendMarker(any(), any(), argThat(bulkLogWalEditType(WALEdit.BULK_LOAD))))
        .thenAnswer(new Answer() {
          @Override
          public Object answer(InvocationOnMock invocation) {
            WALKeyImpl walKey = invocation.getArgument(1);
            MultiVersionConcurrencyControl mvcc = walKey.getMvcc();
            if (mvcc != null) {
              MultiVersionConcurrencyControl.WriteEntry we = mvcc.begin();
              walKey.setWriteEntry(we);
            }
            return 01L;
          }
        });
    testRegionWithFamilies(family1).bulkLoadHFiles(withFamilyPathsFor(family1), false, null);
    verify(log).sync(anyLong());
  }

  @Test
  public void shouldBulkLoadManyFamilyHLog() throws IOException {
    when(log.appendMarker(any(),
            any(), argThat(bulkLogWalEditType(WALEdit.BULK_LOAD)))).thenAnswer(new Answer() {
              @Override
              public Object answer(InvocationOnMock invocation) {
                WALKeyImpl walKey = invocation.getArgument(1);
                MultiVersionConcurrencyControl mvcc = walKey.getMvcc();
                if (mvcc != null) {
                  MultiVersionConcurrencyControl.WriteEntry we = mvcc.begin();
                  walKey.setWriteEntry(we);
                }
                return 01L;
              }
            });
    testRegionWithFamilies(family1, family2).bulkLoadHFiles(withFamilyPathsFor(family1, family2),
            false, null);
    verify(log).sync(anyLong());
  }

  @Test
  public void shouldBulkLoadManyFamilyHLogEvenWhenTableNameNamespaceSpecified() throws IOException {
    when(log.appendMarker(any(), any(), argThat(bulkLogWalEditType(WALEdit.BULK_LOAD))))
        .thenAnswer(new Answer() {
          @Override
          public Object answer(InvocationOnMock invocation) {
            WALKeyImpl walKey = invocation.getArgument(1);
            MultiVersionConcurrencyControl mvcc = walKey.getMvcc();
            if (mvcc != null) {
              MultiVersionConcurrencyControl.WriteEntry we = mvcc.begin();
              walKey.setWriteEntry(we);
            }
            return 01L;
          }
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

  // after HBASE-24021 will throw DoNotRetryIOException, not MultipleIOException
  @Test(expected = DoNotRetryIOException.class)
  public void shouldCrashIfBulkLoadMultiFamiliesNotInTable() throws IOException {
    testRegionWithFamilies(family1).bulkLoadHFiles(withFamilyPathsFor(family1, family2, family3),
      false, null);
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

  // after HBASE-24021 will throw FileNotFoundException, not MultipleIOException
  @Test(expected = FileNotFoundException.class)
  public void shouldThrowErrorIfMultiHFileDoesNotExist() throws IOException {
    List<Pair<byte[], String>> list = new ArrayList<>();
    list.addAll(asList(withMissingHFileForFamily(family1)));
    list.addAll(asList(withMissingHFileForFamily(family2)));
    testRegionWithFamilies(family1, family2).bulkLoadHFiles(list, false, null);
  }
}
