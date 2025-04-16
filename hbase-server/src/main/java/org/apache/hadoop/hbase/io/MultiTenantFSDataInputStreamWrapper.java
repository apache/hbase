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
package org.apache.hadoop.hbase.io;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A specialized FSDataInputStreamWrapper for multi-tenant HFile section reading.
 * This wrapper adds offset handling to standard input stream operations.
 */
@InterfaceAudience.Private
public class MultiTenantFSDataInputStreamWrapper extends FSDataInputStreamWrapper {
  private final long sectionOffset;
  private final FSDataInputStreamWrapper parent;
  
  /**
   * Creates a wrapper over an existing input stream with a section offset.
   *
   * @param parent The parent input stream wrapper
   * @param sectionOffset The offset to the start of the section
   */
  public MultiTenantFSDataInputStreamWrapper(FSDataInputStreamWrapper parent, long sectionOffset) {
    super(new ForwardingFSDataInputStream(parent.getStream(false), sectionOffset));
    this.parent = parent;
    this.sectionOffset = sectionOffset;
  }
  
  @Override
  public FSDataInputStream getStream(boolean useHBaseChecksum) {
    // Always delegate to the original stream with offset adjustment
    return new ForwardingFSDataInputStream(parent.getStream(useHBaseChecksum), sectionOffset);
  }

  @Override
  public boolean shouldUseHBaseChecksum() {
    return parent.shouldUseHBaseChecksum();
  }
  
  @Override
  public void checksumOk() {
    parent.checksumOk();
  }
  
  @Override
  public FSDataInputStream fallbackToFsChecksum(int offCount) throws IOException {
    parent.fallbackToFsChecksum(offCount);
    return getStream(false);
  }
  
  @Override
  public void unbuffer() {
    parent.unbuffer();
  }
  
  /**
   * Inner class that forwards input stream operations with offset adjustment.
   */
  private static class ForwardingFSDataInputStream extends FSDataInputStream {
    private final FSDataInputStream delegate;
    private final long offset;
    
    public ForwardingFSDataInputStream(FSDataInputStream delegate, long offset) {
      super(delegate.getWrappedStream());
      this.delegate = delegate;
      this.offset = offset;
    }
    
    @Override
    public void seek(long pos) throws IOException {
      delegate.seek(pos + offset);
    }
    
    @Override
    public long getPos() throws IOException {
      return delegate.getPos() - offset;
    }
    
    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      return delegate.read(position + this.offset, buffer, offset, length);
    }
    
    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      delegate.readFully(position + this.offset, buffer, offset, length);
    }
    
    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      delegate.readFully(position + this.offset, buffer);
    }
  }
} 