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
package org.apache.hadoop.hbase.io;

import static org.junit.Assert.assertTrue;

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
  public void testUnbuffer() throws Exception {
    InputStream pc = new ParentClass();
    FSDataInputStreamWrapper fsdisw1 =
      new FSDataInputStreamWrapper(new FSDataInputStream(pc));
    fsdisw1.unbuffer();
    // parent class should be true
    assertTrue(((ParentClass)pc).getIsCallUnbuffer());
    fsdisw1.close();

    InputStream cc1 = new ChildClass1();
    FSDataInputStreamWrapper fsdisw2 =
      new FSDataInputStreamWrapper(new FSDataInputStream(cc1));
    fsdisw2.unbuffer();
    // child1 class should be true
    assertTrue(((ChildClass1)cc1).getIsCallUnbuffer());
    fsdisw2.close();
  }

  private class ParentClass extends FSInputStream
      implements ByteBufferReadable, CanSetDropBehind, CanSetReadahead,
                 HasEnhancedByteBufferAccess, CanUnbuffer {

    public boolean isCallUnbuffer = false;

    public boolean getIsCallUnbuffer(){
      return isCallUnbuffer;
    }

    @Override
    public void unbuffer() {
      isCallUnbuffer =  true;
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
      isCallUnbuffer = true;
    }
  }
}
