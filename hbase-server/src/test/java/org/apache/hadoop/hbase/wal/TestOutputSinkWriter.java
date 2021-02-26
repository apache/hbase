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
package org.apache.hadoop.hbase.wal;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestOutputSinkWriter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(
          TestOutputSinkWriter.class);

  @Test
  public void testExeptionHandling() throws IOException, InterruptedException {
    WALSplitter.PipelineController controller = new WALSplitter.PipelineController();
    BrokenEntryBuffers entryBuffers = new BrokenEntryBuffers(controller, 2000);
    OutputSink sink = new OutputSink(controller, entryBuffers, 1) {

      @Override protected int getNumOpenWriters() {
        return 0;
      }

      @Override protected void append(EntryBuffers.RegionEntryBuffer buffer) throws IOException {

      }

      @Override protected List<Path> close() throws IOException {
        return null;
      }

      @Override public Map<String,Long> getOutputCounts() {
        return null;
      }

      @Override public int getNumberOfRecoveredRegions() {
        return 0;
      }

      @Override public boolean keepRegionEvent(WAL.Entry entry) {
        return false;
      }
    };

    //start the Writer thread and give it time trow the exception
    sink.startWriterThreads();
    Thread.sleep(1000L);

    //make sure the exception is stored
    try {
      controller.checkForErrors();
      Assert.fail();
    }
    catch (RuntimeException re){
      Assert.assertTrue(true);
    }

    sink.restartWriterThreadsIfNeeded();

    //after the check the stored exception should be gone
    try {
      controller.checkForErrors();
    }
    catch (RuntimeException re){
      Assert.fail();
    }

    //prep another exception and wait for it to be thrown
    entryBuffers.setThrowError(true);
    Thread.sleep(1000L);

    //make sure the exception is stored
    try {
      controller.checkForErrors();
      Assert.fail();
    }
    catch (RuntimeException re){
      Assert.assertTrue(true);
    }
  }

  static class BrokenEntryBuffers extends EntryBuffers{
    boolean throwError = true;

    public BrokenEntryBuffers(WALSplitter.PipelineController controller, long maxHeapUsage) {
      super(controller, maxHeapUsage);
    }

    @Override
    synchronized EntryBuffers.RegionEntryBuffer getChunkToWrite() {
      //This just emulates something going wrong with in the Writer
      if(throwError){
        throwError = false;
        throw new RuntimeException("testing");
      }
      return null;
    }

    public void setThrowError(boolean newValue){
      throwError = newValue;
    }
  };
}
