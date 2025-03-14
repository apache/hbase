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
package org.apache.hadoop.hbase.wal;

import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.io.InterruptedIOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestRecoveredEditsOutputSink {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRecoveredEditsOutputSink.class);

  private static WALFactory wals;
  private static FileSystem fs;
  private static Path rootDir;
  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static RecoveredEditsOutputSink outputSink;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(WALFactory.WAL_PROVIDER, "filesystem");
    rootDir = TEST_UTIL.createRootDir();
    fs = CommonFSUtils.getRootDirFileSystem(conf);
    wals = new WALFactory(conf, "testRecoveredEditsOutputSinkWALFactory");
    WALSplitter splitter = new WALSplitter(wals, conf, rootDir, fs, rootDir, fs);
    WALSplitter.PipelineController pipelineController = new WALSplitter.PipelineController();
    EntryBuffers sink = new EntryBuffers(pipelineController, 1024 * 1024);
    outputSink = new RecoveredEditsOutputSink(splitter, pipelineController, sink, 3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    wals.close();
    fs.delete(rootDir, true);
  }

  @Test
  public void testClose() throws IOException {
    RecoveredEditsOutputSink spyOutputSink = Mockito.spy(outputSink);

    spyOutputSink.close();
    Mockito.verify(spyOutputSink, Mockito.times(1)).finishWriterThreads();
    Mockito.verify(spyOutputSink, Mockito.times(1)).closeWriters();
  }

  /**
   * When a WAL split is interrupted (ex. by a RegionServer abort), the thread join in
   * finishWriterThreads() will get interrupted, rethrowing the exception without stopping the
   * writer threads. Test to ensure that when this happens, RecoveredEditsOutputSink.close() does
   * not execute closeWriters() as this can cause corruption. Please see HBASE-28569
   */
  @Test
  public void testCloseWALSplitInterrupted() throws IOException {
    RecoveredEditsOutputSink spyOutputSink = Mockito.spy(outputSink);

    // The race condition will lead to an InterruptedException to be caught by finishWriterThreads()
    // which is then rethrown as an InterruptedIOException and should bubble up from
    // RecoveredEditsOutputSink.close()
    Mockito.doThrow(new InterruptedIOException()).when(spyOutputSink).finishWriterThreads();
    assertThrows(InterruptedIOException.class, spyOutputSink::close);
    Mockito.verify(spyOutputSink, Mockito.times(1)).finishWriterThreads();
    Mockito.verify(spyOutputSink, Mockito.never()).closeWriters();
  }
}
