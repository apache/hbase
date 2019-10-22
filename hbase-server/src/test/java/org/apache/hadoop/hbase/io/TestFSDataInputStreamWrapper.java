package org.apache.hadoop.hbase.io;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.HasEnhancedByteBufferAccess;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.io.ByteBufferPool;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestFSDataInputStreamWrapper {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFSDataInputStreamWrapper.class);

  @Test
  public void testIsImplementsCanUnbuffer() throws Exception {
    InputStream pc = new ParentClass();
    FSDataInputStream is = new FSDataInputStream(pc);
    FSDataInputStreamWrapper fsdisw = new FSDataInputStreamWrapper(is);
    //parent class should be true for CanUnbuffer
    assertEquals(true, fsdisw.isImplementsCanUnbuffer(ParentClass.class));
    //child1 class should be true for CanUnbuffer
    assertEquals(true, fsdisw.isImplementsCanUnbuffer(ChildClass1.class));
    //child2 class not implements  CanUnbuffer should be false
    assertEquals(false, fsdisw.isImplementsCanUnbuffer(ChildClass2.class));
    fsdisw.close();
  }
  
  @Test
  public void testUnbuffer() throws Exception {
    InputStream pc = new ParentClass();
    FSDataInputStreamWrapper fsdisw1 =
      new FSDataInputStreamWrapper(new FSDataInputStream(pc));
    fsdisw1.unbuffer();
    // parent class should be true
    assertEquals(true, fsdisw1.instanceOfCanUnbuffer);
    fsdisw1.close();

    InputStream cc1 = new ChildClass1();
    FSDataInputStreamWrapper fsdisw2 =
      new FSDataInputStreamWrapper(new FSDataInputStream(cc1));
    fsdisw2.unbuffer();
    // child1 class should be true
    assertEquals(true, fsdisw2.instanceOfCanUnbuffer);
    fsdisw2.close();
  }

  private class ParentClass extends FSInputStream
      implements ByteBufferReadable, CanSetDropBehind, CanSetReadahead,
                 HasEnhancedByteBufferAccess, CanUnbuffer {

    @Override
    public void unbuffer() {

    }

    @Override
    public int read() throws IOException {
      return 0;
    }

    @Override
    public ByteBuffer read(ByteBufferPool paramByteBufferPool,
        int paramInt, EnumSet<ReadOption> paramEnumSet)
            throws IOException, UnsupportedOperationException {
      return null;
    }

    @Override
    public void releaseBuffer(ByteBuffer paramByteBuffer) {

    }

    @Override
    public void setReadahead(Long paramLong)
        throws IOException, UnsupportedOperationException {

    }

    @Override
    public void setDropBehind(Boolean paramBoolean)
        throws IOException, UnsupportedOperationException {

    }

    @Override
    public int read(ByteBuffer paramByteBuffer) throws IOException {
      return 0;
    }

    @Override
    public void seek(long paramLong) throws IOException {

    }

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public boolean seekToNewSource(long paramLong) throws IOException {
      return false;
    }
  }
  
  private class ChildClass1 extends ParentClass{
    @Override
    public void unbuffer() {

    }
  }
  
  private class ChildClass2 extends InputStream {

    @Override
    public int read() throws IOException {
      return 0;
    }
  }
}
