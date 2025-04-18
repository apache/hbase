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
import java.io.InputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A specialized FSDataInputStreamWrapper for multi-tenant HFile section reading.
 * This wrapper adds offset handling to standard input stream operations.
 */
@InterfaceAudience.Private
public class MultiTenantFSDataInputStreamWrapper extends FSDataInputStreamWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(MultiTenantFSDataInputStreamWrapper.class);
  
  private final FSDataInputStreamWrapper parent;
  private final long sectionOffset;
  private boolean useRelativeOffsets = false;
  
  /**
   * Creates a wrapper over an existing input stream with a section offset.
   *
   * @param parent The parent input stream wrapper
   * @param sectionOffset The offset to the start of the section
   */
  public MultiTenantFSDataInputStreamWrapper(FSDataInputStreamWrapper parent, long sectionOffset) {
    super(new ForwardingFSDataInputStream(parent.getStream(false), sectionOffset, false));
    this.parent = parent;
    this.sectionOffset = sectionOffset;
  }
  
  /**
   * Set whether this wrapper should use relative offsets.
   * @param value True to use relative offsets, false for absolute offsets
   */
  public void setUsesRelativeOffsets(boolean value) {
    this.useRelativeOffsets = value;
    LOG.debug("Set relative offset mode to {} (base offset={})", value, sectionOffset);
  }
  
  @Override
  public FSDataInputStream getStream(boolean useHBaseChecksum) {
    // Always delegate to the original stream with offset adjustment
    ForwardingFSDataInputStream stream = new ForwardingFSDataInputStream(
        parent.getStream(useHBaseChecksum), sectionOffset, useRelativeOffsets);
    return stream;
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
  
  @Override
  public Path getReaderPath() {
    return parent.getReaderPath();
  }
  
  /**
   * Inner class that forwards input stream operations with offset adjustment.
   */
  private static class ForwardingFSDataInputStream extends FSDataInputStream {
    private final FSDataInputStream delegate;
    private final long offset;
    private boolean useRelativeOffsets;
    
    public ForwardingFSDataInputStream(FSDataInputStream delegate, long offset, boolean useRelativeOffsets) {
      super(delegate.getWrappedStream());
      this.delegate = delegate;
      this.offset = offset;
      this.useRelativeOffsets = useRelativeOffsets;
    }
    
    @Override
    public void seek(long pos) throws IOException {
      if (useRelativeOffsets) {
        // If using relative offsets, translate relative to absolute
        delegate.seek(pos + offset);
      } else {
        // Otherwise, pass through directly
        delegate.seek(pos);
      }
    }
    
    @Override
    public long getPos() throws IOException {
      if (useRelativeOffsets) {
        // If using relative offsets, translate absolute to relative
        return delegate.getPos() - offset;
      } else {
        // Otherwise, return actual position
        return delegate.getPos();
      }
    }
    
    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      if (useRelativeOffsets) {
        // If using relative offsets, translate relative to absolute
        return delegate.read(position + this.offset, buffer, offset, length);
      } else {
        // Otherwise, pass through directly
        return delegate.read(position, buffer, offset, length);
      }
    }
    
    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      if (useRelativeOffsets) {
        // If using relative offsets, translate relative to absolute
        delegate.readFully(position + this.offset, buffer, offset, length);
      } else {
        // Otherwise, pass through directly
        delegate.readFully(position, buffer, offset, length);
      }
    }
    
    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      if (useRelativeOffsets) {
        // If using relative offsets, translate relative to absolute
        delegate.readFully(position + this.offset, buffer);
      } else {
        // Otherwise, pass through directly
        delegate.readFully(position, buffer);
      }
    }
  }
} 