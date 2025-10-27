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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TestRefreshHFilesBase;
import org.apache.hadoop.hbase.master.procedure.TestRefreshHFilesProcedureWithReadOnlyConf;
import org.apache.hadoop.hbase.regionserver.CreateStoreFileWriterParams;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestStoreFileTrackerBaseReadOnlyMode extends TestRefreshHFilesBase {
  private DummyStoreFileTrackerForReadOnlyMode tracker;

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRefreshHFilesProcedureWithReadOnlyConf.class);

  @Before
  public void setup() throws Exception {
    // When true is passed only setup for readonly property is done.
    // The initial ReadOnly property will be false for table creation
    baseSetup(true);
  }

  @After
  public void tearDown() throws Exception {
    baseTearDown();
  }

  @Test
  public void testLoadReadOnlyWhenGlobalReadOnlyEnabled() throws Exception {
    try {
      setReadOnlyMode(true);
      tracker = new DummyStoreFileTrackerForReadOnlyMode(conf, true);
      tracker.load();
      assertTrue("Tracker should be in read-only mode", tracker.wasReadOnlyLoad());
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      setReadOnlyMode(false);
    }
  }

  @Test
  public void testReplaceSkippedWhenGlobalReadOnlyEnabled() throws Exception {
    try {
      setReadOnlyMode(true);
      tracker = new DummyStoreFileTrackerForReadOnlyMode(conf, true);
      tracker.replace(Collections.emptyList(), Collections.emptyList());
      assertFalse("Compaction should not be executed in readonly mode",
        tracker.wasCompactionExecuted());
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      setReadOnlyMode(false);
    }
  }

  @Test
  public void testReplaceExecutedWhenWritable() throws Exception {
    tracker = new DummyStoreFileTrackerForReadOnlyMode(conf, true);
    tracker.replace(Collections.emptyList(), Collections.emptyList());
    assertTrue("Compaction should run when not readonly", tracker.wasCompactionExecuted());
  }

  @Test
  public void testAddSkippedWhenGlobalReadOnlyEnabled() throws Exception {
    try {
      setReadOnlyMode(true);
      tracker = new DummyStoreFileTrackerForReadOnlyMode(conf, true);
      tracker.add(Collections.emptyList());
      assertFalse("Add should not be executed in readonly mode", tracker.wasAddExecuted());
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      setReadOnlyMode(false);
    }
  }

  @Test
  public void testAddExecutedWhenWritable() throws Exception {
    tracker = new DummyStoreFileTrackerForReadOnlyMode(conf, true);
    tracker.add(Collections.emptyList());
    assertTrue("Add should run when not readonly", tracker.wasAddExecuted());
  }

  @Test
  public void testSetSkippedWhenGlobalReadOnlyEnabled() throws Exception {
    try {
      setReadOnlyMode(true);
      tracker = new DummyStoreFileTrackerForReadOnlyMode(conf, true);
      tracker.set(Collections.emptyList());
      assertFalse("Set should not be executed in readonly mode", tracker.wasSetExecuted());
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      setReadOnlyMode(false);
    }
  }

  @Test
  public void testSetExecutedWhenWritable() throws Exception {
    tracker = new DummyStoreFileTrackerForReadOnlyMode(conf, true);
    tracker.set(Collections.emptyList());
    assertTrue("Set should run when not readonly", tracker.wasSetExecuted());
  }

  @Test(expected = IllegalStateException.class)
  public void testCreateWriterThrowExceptionWhenGlobalReadOnlyEnabled() throws Exception {
    try {
      setReadOnlyMode(true);
      tracker = new DummyStoreFileTrackerForReadOnlyMode(conf, true);
      CreateStoreFileWriterParams params = CreateStoreFileWriterParams.create().maxKeyCount(4)
        .isCompaction(false).includeMVCCReadpoint(true).includesTag(false).shouldDropBehind(false);
      tracker.createWriter(params);
    } finally {
      setReadOnlyMode(false);
    }
  }
}
