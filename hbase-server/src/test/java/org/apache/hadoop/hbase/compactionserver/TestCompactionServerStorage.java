/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.compactionserver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@Category(SmallTests.class)
public class TestCompactionServerStorage {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionServerStorage.class);
  private static TableName tableName = TableName.valueOf("testCompactingFilesStore");
  private static byte[] family = Bytes.toBytes("A");
  private static CompactionServerStorage storage;
  private static RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName).build();
  private static ColumnFamilyDescriptor cfd =
      ColumnFamilyDescriptorBuilder.newBuilder(family).build();

  private static class TestStorageThread extends Thread {
    List<String> fileNameList;
    boolean flag;

    TestStorageThread(List<String> files, boolean flag) {
      this.fileNameList = files;
      this.flag = flag;
    }

    @Override
    public void run() {
      if (flag) {
        storage.addSelectedFiles(regionInfo, cfd, fileNameList);
      } else {
        storage.removeSelectedFiles(regionInfo, cfd, fileNameList);
      }
    }
  }

  @Before
  public void cleanUpStorage() {
    storage = new CompactionServerStorage();
  }

  @Test
  public void testSelectedFilesStore() {
    List<String> fileNames = Lists.newArrayList("1");
    storage.addSelectedFiles(regionInfo, cfd, fileNames);
    // can not add a selected file again
    Assert.assertFalse(storage.addSelectedFiles(regionInfo, cfd, fileNames));

    List<String> fileNames2 = Lists.newArrayList("2");
    Assert.assertTrue(storage.addSelectedFiles(regionInfo, cfd, fileNames2));

    List<String> fileNames3 = Lists.newArrayList("3", "2");
    Assert.assertFalse(storage.addSelectedFiles(regionInfo, cfd, fileNames3));

    storage.removeSelectedFiles(regionInfo, cfd, fileNames2);
    Assert.assertTrue(storage.addSelectedFiles(regionInfo, cfd, fileNames3));

    Set<String> selectedFiles = storage.getSelectedStoreFiles(regionInfo, cfd);
    Assert.assertEquals(3, selectedFiles.size());
  }

  @Test
  public void testCompactedFilesStore() {
    List<String> fileNames = Lists.newArrayList("1");
    storage.addCompactedFiles(regionInfo, cfd, fileNames);
    // add a compacted file twice, no check(This should not happen)
    Assert.assertTrue(storage.addCompactedFiles(regionInfo, cfd, fileNames));

    List<String> fileNames2 = Lists.newArrayList("3", "2");
    Assert.assertTrue(storage.addCompactedFiles(regionInfo, cfd, fileNames2));

    Set<String> compactedFiles = storage.getCompactedStoreFiles(regionInfo, cfd);
    Assert.assertEquals(3, compactedFiles.size());

    Set<String> storeFileNames = Sets.newHashSet("3");
    storage.cleanupCompactedFiles(regionInfo, cfd, storeFileNames);
    Assert.assertEquals(1, compactedFiles.size());
  }

  @Test
  public void testSelectedFilesStoreMultiThread() {
    int threadNum = 50;
    TestStorageThread[] testStorageThreads = new TestStorageThread[threadNum];
    for (int i = 0; i < threadNum; i++) {
      testStorageThreads[i] =
          new TestStorageThread(new ArrayList<>(Arrays.asList("" + i)), true);
    }
    for (int i = 0; i < threadNum; i++) {
      testStorageThreads[i].start();
    }
    for (int i = 0; i < threadNum; i++) {
      try {
        testStorageThreads[i].join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupted();
      }
    }
    Assert.assertEquals(threadNum, storage.getSelectedStoreFiles(regionInfo, cfd).size());
    for (int i = 0; i < threadNum; i++) {
      testStorageThreads[i] =
          new TestStorageThread(new ArrayList<>(Arrays.asList("" + i)), false);
    }
    for (int i = 0; i < threadNum; i++) {
      testStorageThreads[i].start();
    }
    for (int i = 0; i < threadNum; i++) {
      try {
        testStorageThreads[i].join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupted();
      }
    }
    Assert.assertEquals(0, storage.getSelectedStoreFiles(regionInfo, cfd).size());
  }

}
