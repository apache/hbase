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

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;

/**
 * Test class for the TestPersistedStoreFlushContext
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestPersistedStoreFlushContext {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestPersistedStoreFlushContext.class);

  @Rule
  public TestName name = new TestName();

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String DIR =
    TEST_UTIL.getDataTestDir("TestPersistedStoreFlushContext").toString();

  @Test
  public void testCommit() throws Exception {
    HStore mockStore = mock(HStore.class);
    mockStore.storeSize = new AtomicLong(0);
    mockStore.totalUncompressedBytes = new AtomicLong(0);
    mockStore.flushedCellsCount = new AtomicLong(0);
    mockStore.flushedCellsSize = new AtomicLong(0);
    mockStore.flushedOutputFileSize = new AtomicLong(0);
    Path filePath = new Path(DIR + name.getMethodName());
    ArgumentCaptor<Path> captor = ArgumentCaptor.forClass(Path.class);
    HStoreFile mockStoreFile = mock(HStoreFile.class);
    when(mockStoreFile.getReader()).thenReturn(mock(StoreFileReader.class));
    when(mockStore.createStoreFileAndReader(captor.capture())).thenReturn(mockStoreFile);
    PersistedStoreFlushContext context = new PersistedStoreFlushContext(mockStore,
      0L, FlushLifeCycleTracker.DUMMY);
    context.tempFiles = new ArrayList<>();
    context.tempFiles.add(filePath);
    context.committedFiles = new ArrayList<>();
    context.snapshot = mock(MemStoreSnapshot.class);
    context.commit(mock(MonitoredTask.class));
    //asserts that original file didn't get renamed after the commit operation
    assertEquals(filePath, captor.getValue());
  }

}
