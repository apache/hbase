package org.apache.hadoop.hbase.wal;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestOutputSinkWriter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(
          TestOutputSinkWriter.class);

  @Test
  public void testExeptionHandling() throws IOException, InterruptedException {
    WALSplitter.PipelineController controller = new WALSplitter.PipelineController();
    BrokenEntryBuffers entryBuffers = new BrokenEntryBuffers(controller, 2000);
    OutputSink sink =
        new OutputSink(controller,
            entryBuffers,
            1){

          @Override public List<Path> finishWritingAndClose() throws IOException {
            return null;
          }

          @Override public Map<byte[],Long> getOutputCounts() {
            return null;
          }

          @Override public int getNumberOfRecoveredRegions() {
            return 0;
          }

          @Override public void append(WALSplitter.RegionEntryBuffer buffer) throws IOException {

          }

          @Override public boolean keepRegionEvent(WAL.Entry entry) {
            return false;
          }
        };

    //start the Writer thread and give it time trow the exception
    sink.startWriterThreads();
    Thread.sleep(1000l);

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
    Thread.sleep(1000l);

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
    synchronized WALSplitter.RegionEntryBuffer getChunkToWrite() {
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
