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
import org.apache.hadoop.hbase.io.MultiTenantFSDataInputStreamWrapper;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HFile reader for multi-tenant HFiles in PREAD (random access) mode.
 * This implementation creates HFilePreadReader instances for each tenant section.
 */
@InterfaceAudience.Private
public class MultiTenantPreadReader extends AbstractMultiTenantReader {
  private static final Logger LOG = LoggerFactory.getLogger(MultiTenantPreadReader.class);

  /**
   * Constructor for multi-tenant pread reader
   *
   * @param context Reader context info
   * @param fileInfo HFile info
   * @param cacheConf Cache configuration values
   * @param conf Configuration
   * @throws IOException If an error occurs during initialization
   */
  public MultiTenantPreadReader(ReaderContext context, HFileInfo fileInfo,
      CacheConfig cacheConf, Configuration conf) throws IOException {
    super(context, fileInfo, cacheConf, conf);
  }

  @Override
  protected SectionReader createSectionReader(byte[] tenantPrefix, SectionMetadata metadata)
      throws IOException {
    return new PreadSectionReader(tenantPrefix, metadata);
  }

  /**
   * Section reader implementation for pread mode that uses HFilePreadReader
   */
  protected class PreadSectionReader extends SectionReader {
    public PreadSectionReader(byte[] tenantPrefix, SectionMetadata metadata) {
      super(tenantPrefix, metadata);
    }

    @Override
    public synchronized HFileReaderImpl getReader() throws IOException {
      if (!initialized) {
        // Create section context with section-specific settings
        MultiTenantFSDataInputStreamWrapper sectionStream = 
            new MultiTenantFSDataInputStreamWrapper(context.getInputStreamWrapper(), metadata.offset);
            
        ReaderContext sectionContext = ReaderContextBuilder.newBuilder(context)
            .withInputStreamWrapper(sectionStream)
            .withFilePath(context.getFilePath())
            .withReaderType(ReaderContext.ReaderType.PREAD)
            .withFileSystem(context.getFileSystem())
            .withFileSize(metadata.size)
            .build();

        // Create pread reader for this section
        reader = new HFilePreadReader(sectionContext, fileInfo, cacheConf, getConf());
        initialized = true;
        LOG.debug("Initialized HFilePreadReader for tenant prefix: {}",
            org.apache.hadoop.hbase.util.Bytes.toStringBinary(tenantPrefix));
      }
      return reader;
    }

    @Override
    public HFileScanner getScanner(Configuration conf, boolean cacheBlocks, 
        boolean pread, boolean isCompaction) throws IOException {
      return getReader().getScanner(conf, cacheBlocks, pread, isCompaction);
    }

    @Override
    public void close() throws IOException {
      close(false);
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