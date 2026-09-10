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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TestRefreshHFilesBase;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.regionserver.CreateStoreFileWriterParams;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(RegionServerTests.TAG)
@Tag(SmallTests.TAG)
public class TestStoreFileTrackerBaseReadOnlyMode extends TestRefreshHFilesBase {
  private DummyStoreFileTrackerForReadOnlyMode tracker;

  TableName tableName = TableName.valueOf("TestStoreFileTrackerBaseReadOnlyMode");

  @BeforeEach
  public void setup() throws Exception {
    // When true is passed only setup for readonly property is done.
    // The initial ReadOnly property will be false for table creation
    baseSetup(true);
  }

  @AfterEach
  public void tearDown() throws Exception {
    baseTearDown();
  }

  private void verifyLoadInReadOnlyMode(boolean readOnlyMode, TableName table,
    boolean expectReadOnly, String msg) throws Exception {
    try {
      setReadOnlyMode(readOnlyMode);
      tracker = new DummyStoreFileTrackerForReadOnlyMode(conf, true, table);
      tracker.load();
      assertEquals(expectReadOnly, tracker.wasReadOnlyLoad(), msg);
    } finally {
      setReadOnlyMode(false);
    }
  }

  @Test
  public void testLoadNonWritableTableWhenGlobalReadOnlyEnabled() throws Exception {
    verifyLoadInReadOnlyMode(true, tableName, true,
      "For non-writable tables, the doLoadStoreFiles() should get called with readOnly=true");
  }

  @Test
  public void testLoadMetaTableWhenGlobalReadOnlyEnabled() throws Exception {
    verifyLoadInReadOnlyMode(true, TableName.META_TABLE_NAME, false,
      "As meta table is always writable, the doLoadStoreFiles should not get called with readOnly=false even if readonly mode is enabled");
  }

  @Test
  public void testLoadMasterStoreTableWhenGlobalReadOnlyEnabled() throws Exception {
    // As master:store table is always writable, the doLoadStoreFiles should not get called with
    // readOnly=true
    verifyLoadInReadOnlyMode(true, MasterRegionFactory.TABLE_NAME, false,
      "As master:store table is always writable, the doLoadStoreFiles should not get called with readOnly=false even if readonly mode is enabled");
  }

  @Test
  public void testLoadWhenGlobalReadOnlyDisabled() throws Exception {
    // When readonly mode is disabled, then it should not interfere with normal functionality
    verifyLoadInReadOnlyMode(false, tableName, false,
      "As readonly mode is not set, the doLoadStoreFiles() should get called with readOnly=false");
  }

  private void verifyReplaceInReadOnlyMode(boolean readOnlyMode, TableName table,
    boolean expectCompactionExecuted, String msg) {
    try {
      setReadOnlyMode(readOnlyMode);
      tracker = new DummyStoreFileTrackerForReadOnlyMode(conf, true, table);
      tracker.replace(Collections.emptyList(), Collections.emptyList());
      assertEquals(expectCompactionExecuted, tracker.wasCompactionExecuted(), msg);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      setReadOnlyMode(false);
    }
  }

  @Test
  public void testReplaceSkippedForNonWritableTableWhenGlobalReadOnlyEnabled() throws Exception {
    verifyReplaceInReadOnlyMode(true, tableName, false,
      "Compaction should not be executed for non-writable table in readonly mode");
  }

  @Test
  public void testReplaceExecutedForMetaTableWhenGlobalReadOnlyEnabled() throws Exception {
    verifyReplaceInReadOnlyMode(true, TableName.META_TABLE_NAME, true,
      "Compaction should be executed for meta table in readonly mode");
  }

  @Test
  public void testReplaceExecutedForMasterStoreTableWhenGlobalReadOnlyEnabled() throws Exception {
    verifyReplaceInReadOnlyMode(true, MasterRegionFactory.TABLE_NAME, true,
      "Compaction should be executed for master:store table in readonly mode");
  }

  @Test
  public void testReplaceExecutedWhenGlobalReadOnlyDisabled() throws Exception {
    verifyReplaceInReadOnlyMode(false, tableName, true,
      "Compaction should be executed for any table when readonly mode is disabled");
  }

  private void verifyAddInReadOnlyMode(boolean readOnlyMode, TableName table,
    boolean expectAddExecuted, String msg) {
    try {
      setReadOnlyMode(readOnlyMode);
      tracker = new DummyStoreFileTrackerForReadOnlyMode(conf, true, table);
      tracker.add(Collections.emptyList());
      assertEquals(expectAddExecuted, tracker.wasAddExecuted(), msg);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      setReadOnlyMode(false);
    }
  }

  @Test
  public void testAddSkippedForNonWritableTableWhenGlobalReadOnlyEnabled() throws Exception {
    verifyAddInReadOnlyMode(true, tableName, false,
      "Add should not be executed for non-writable table in readonly mode");
  }

  @Test
  public void testAddExecutedForMetaTableWhenGlobalReadOnlyEnabled() throws Exception {
    verifyAddInReadOnlyMode(true, TableName.META_TABLE_NAME, true,
      "Add should be executed for meta table in readonly mode");
  }

  @Test
  public void testAddExecutedForMasterStoreTableWhenGlobalReadOnlyEnabled() throws Exception {
    verifyAddInReadOnlyMode(true, MasterRegionFactory.TABLE_NAME, true,
      "Add should be executed for master:store table in readonly mode");
  }

  @Test
  public void testAddExecutedWhenGlobalReadOnlyDisabled() throws Exception {
    verifyAddInReadOnlyMode(false, tableName, true,
      "Add should be executed for any table when readonly mode is disabled");
  }

  private void verifySetInReadOnlyMode(boolean readOnlyMode, TableName table,
    boolean expectSetExecuted, String msg) {
    try {
      setReadOnlyMode(readOnlyMode);
      tracker = new DummyStoreFileTrackerForReadOnlyMode(conf, true, table);
      tracker.set(Collections.emptyList());
      assertEquals(expectSetExecuted, tracker.wasSetExecuted(), msg);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      setReadOnlyMode(false);
    }
  }

  @Test
  public void testSetSkippedForNonWritableTableWhenGlobalReadOnlyEnabled() throws Exception {
    verifySetInReadOnlyMode(true, tableName, false,
      "Set should not be executed for non-writable table in readonly mode");
  }

  @Test
  public void testSetExecutedForMetaTableWhenGlobalReadOnlyEnabled() throws Exception {
    verifySetInReadOnlyMode(true, TableName.META_TABLE_NAME, true,
      "Set should be executed for meta table in readonly mode");
  }

  @Test
  public void testSetExecutedForMasterStoreTableWhenGlobalReadOnlyEnabled() throws Exception {
    verifySetInReadOnlyMode(true, MasterRegionFactory.TABLE_NAME, true,
      "Set should be executed for master:store table in readonly mode");
  }

  @Test
  public void testSetExecutedWhenGlobalReadOnlyDisabled() throws Exception {
    verifySetInReadOnlyMode(false, tableName, true,
      "Set should be executed for any table when readonly mode is disabled");
  }

  private CreateStoreFileWriterParams createParams() {
    return CreateStoreFileWriterParams.create().maxKeyCount(4).isCompaction(false)
      .includeMVCCReadpoint(true).includesTag(false).shouldDropBehind(false);
  }

  private void assertIllegalStateThrown(TableName tableName) {
    try {
      setReadOnlyMode(true);
      tracker = new DummyStoreFileTrackerForReadOnlyMode(conf, true, tableName);
      assertThrows(IllegalStateException.class, () -> tracker.createWriter(createParams()),
        "Expected IllegalStateException");
    } finally {
      setReadOnlyMode(false);
    }
  }

  private void assertNoIllegalStateThrown(TableName tableName) {
    try {
      setReadOnlyMode(true);
      tracker = new DummyStoreFileTrackerForReadOnlyMode(conf, true, tableName);
      try {
        tracker.createWriter(createParams());
      } catch (IllegalStateException e) {
        fail("Should not throw IllegalStateException for table " + tableName);
      } catch (Exception e) {
        // Ignore other exceptions as they are not the focus of this test
      }
    } finally {
      setReadOnlyMode(false);
    }
  }

  @Test
  public void testCreateWriterThrowExceptionWhenGlobalReadOnlyEnabled() throws Exception {
    assertIllegalStateThrown(tableName);
  }

  @Test
  public void testCreateWriterNoExceptionMetaTableWhenGlobalReadOnlyEnabled() throws Exception {
    assertNoIllegalStateThrown(TableName.META_TABLE_NAME);
  }

  @Test
  public void testCreateWriterNoExceptionMasterStoreTableWhenGlobalReadOnlyEnabled()
    throws Exception {
    assertNoIllegalStateThrown(MasterRegionFactory.TABLE_NAME);
  }
}
