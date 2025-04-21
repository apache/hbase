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
package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.MultiTenantFSDataInputStreamWrapper;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HFile reader for multi-tenant HFiles in STREAM (sequential access) mode.
 * This implementation creates HFileStreamReader instances for each tenant section.
 */
@InterfaceAudience.Private
public class MultiTenantStreamReader extends AbstractMultiTenantReader {
  private static final Logger LOG = LoggerFactory.getLogger(MultiTenantStreamReader.class);

  /**
   * Constructor for multi-tenant stream reader
   *
   * @param context Reader context info
   * @param fileInfo HFile info
   * @param cacheConf Cache configuration values
   * @param conf Configuration
   * @throws IOException If an error occurs during initialization
   */
  public MultiTenantStreamReader(ReaderContext context, HFileInfo fileInfo,
      CacheConfig cacheConf, Configuration conf) throws IOException {
    super(context, fileInfo, cacheConf, conf);
    // Tenant index structure is loaded and logged by the parent class
  }

  @Override
  protected SectionReader createSectionReader(byte[] tenantPrefix, SectionMetadata metadata)
      throws IOException {
    LOG.debug("Creating section reader for tenant: {}, offset: {}, size: {}",
        Bytes.toStringBinary(tenantPrefix), metadata.getOffset(), metadata.getSize());
    return new StreamSectionReader(tenantPrefix, metadata);
  }

  /**
   * Section reader implementation for stream mode that uses HFileStreamReader
   */
  protected class StreamSectionReader extends SectionReader {
    public StreamSectionReader(byte[] tenantPrefix, SectionMetadata metadata) {
      super(tenantPrefix, metadata);
    }

    @Override
    public synchronized HFileReaderImpl getReader() throws IOException {
      if (!initialized) {
        // Create section context with section-specific settings using parent method
        ReaderContext sectionContext = buildSectionContext(
            metadata, ReaderContext.ReaderType.STREAM);

        try {
          // Create a section-specific HFileInfo
          HFileInfo sectionFileInfo = new HFileInfo(sectionContext, getConf());
          
          // Create stream reader for this section with the section-specific fileInfo
          reader = new HFileStreamReader(sectionContext, sectionFileInfo, cacheConf, getConf());
          
          // Initialize section indices using the standard HFileInfo method
          // This method was designed for HFile v3 format, which each section follows
          LOG.debug("Initializing section indices for tenant at offset {}", metadata.getOffset());
          sectionFileInfo.initMetaAndIndex(reader);
          LOG.debug("Successfully initialized indices for section at offset {}", metadata.getOffset());
          
          initialized = true;
          LOG.debug("Initialized HFileStreamReader for tenant prefix: {}",
              org.apache.hadoop.hbase.util.Bytes.toStringBinary(tenantPrefix));
        } catch (IOException e) {
          LOG.error("Failed to initialize section reader", e);
          throw e;
        }
      }
      return reader;
    }

    @Override
    public HFileScanner getScanner(Configuration conf, boolean cacheBlocks, 
        boolean pread, boolean isCompaction) throws IOException {
      return getReader().getScanner(conf, cacheBlocks, pread, isCompaction);
    }

    @Override
    public void close(boolean evictOnClose) throws IOException {
      if (reader != null) {
        reader.close(evictOnClose);
        reader = null;
      }
      initialized = false;
    }
  }

  // Add the close() implementation for completeness
  @Override
  public void close() throws IOException {
    close(false);
  }
  
  // Add the close(boolean) implementation
  @Override
  public void close(boolean evictOnClose) throws IOException {
    // Close all section readers
    for (SectionReader reader : sectionReaders.values()) {
      if (reader != null) {
        reader.close(evictOnClose);
      }
    }
    sectionReaders.clear();
    
    // Close resources in HFileReaderImpl
    if (fsBlockReader != null) {
      fsBlockReader.closeStreams();
    }
  }
} 