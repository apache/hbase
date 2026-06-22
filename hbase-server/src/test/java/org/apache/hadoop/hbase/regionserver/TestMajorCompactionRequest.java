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

import static org.apache.hadoop.hbase.regionserver.Store.PRIORITY_USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequestImpl;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;

@Tag(RegionServerTests.TAG)
@Tag(LargeTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: compType={0}")
public class TestMajorCompactionRequest extends MajorCompactionTestBase {

  public TestMajorCompactionRequest(String compType) {
    super(compType);
  }

  /**
   * Test for HBASE-5920 - Test user requested major compactions always occurring
   */
  @TestTemplate
  public void testNonUserMajorCompactionRequest() throws Exception {
    HStore store = r.getStore(COLUMN_FAMILY);
    createStoreFile(r);
    for (int i = 0; i < MAX_FILES_TO_COMPACT + 1; i++) {
      createStoreFile(r);
    }
    store.triggerMajorCompaction();

    CompactionRequestImpl request = store.requestCompaction().get().getRequest();
    assertNotNull(request, "Expected to receive a compaction request");
    assertEquals(false, request.isMajor(),
      "System-requested major compaction should not occur if there are too many store files");
  }

  /**
   * Test for HBASE-5920
   */
  @TestTemplate
  public void testUserMajorCompactionRequest() throws IOException {
    HStore store = r.getStore(COLUMN_FAMILY);
    createStoreFile(r);
    for (int i = 0; i < MAX_FILES_TO_COMPACT + 1; i++) {
      createStoreFile(r);
    }
    store.triggerMajorCompaction();
    CompactionRequestImpl request = store
      .requestCompaction(PRIORITY_USER, CompactionLifeCycleTracker.DUMMY, null).get().getRequest();
    assertNotNull(request, "Expected to receive a compaction request");
    assertTrue(request.isMajor(), "User-requested major compaction should always occur, "
      + "even if there are too many store files");
  }
}
