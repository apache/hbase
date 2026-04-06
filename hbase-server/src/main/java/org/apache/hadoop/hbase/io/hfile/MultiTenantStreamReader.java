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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HFile reader for multi-tenant HFiles in STREAM (sequential access) mode. This implementation
 * creates HFileStreamReader instances for each tenant section.
 */
@InterfaceAudience.Private
public class MultiTenantStreamReader extends AbstractMultiTenantReader {
  private static final Logger LOG = LoggerFactory.getLogger(MultiTenantStreamReader.class);

  /**
   * Constructor for multi-tenant stream reader.
   * @param context   Reader context info
   * @param fileInfo  HFile info
   * @param cacheConf Cache configuration values
   * @param conf      Configuration
   * @throws IOException If an error occurs during initialization
   */
  public MultiTenantStreamReader(ReaderContext context, HFileInfo fileInfo, CacheConfig cacheConf,
    Configuration conf) throws IOException {
    super(context, fileInfo, cacheConf, conf);
    // Tenant index structure is loaded and logged by the parent class
  }

  /**
   * Create a section reader for a specific tenant.
   * <p>
   * Creates a StreamSectionReader that handles sequential access to a specific tenant section
   * within the multi-tenant HFile.
   * @param tenantSectionId The tenant section ID
   * @param metadata        The section metadata containing offset and size
   * @return A section reader for the tenant
   * @throws IOException If an error occurs creating the reader
   */
  @Override
  protected SectionReader createSectionReader(byte[] tenantSectionId, SectionMetadata metadata)
    throws IOException {
    LOG.debug("Creating section reader for tenant section: {}, offset: {}, size: {}",
      Bytes.toStringBinary(tenantSectionId), metadata.getOffset(), metadata.getSize());
    return new StreamSectionReader(tenantSectionId, metadata);
  }

  /**
   * Section reader implementation for stream (sequential access) mode.
   * <p>
   * This implementation creates HFileStreamReader instances for each tenant section, providing
   * efficient sequential access to data within specific tenant boundaries. Stream readers are
   * optimized for sequential scans and compaction operations.
   */
  protected class StreamSectionReader extends SectionReader {

    /**
     * Constructor for StreamSectionReader.
     * @param tenantSectionId The tenant section ID
     * @param metadata        The section metadata
     */
    public StreamSectionReader(byte[] tenantSectionId, SectionMetadata metadata) {
      super(tenantSectionId, metadata);
    }

    @Override
    public HFileReaderImpl getReader() throws IOException {
      HFileReaderImpl local = reader;
      if (local != null) {
        return local;
      }

      synchronized (this) {
        local = reader;
        if (local != null) {
          return local;
        }

        ReaderContext sectionContext =
          buildSectionContext(metadata, ReaderContext.ReaderType.STREAM);
        if (sectionContext == null) {
          throw new IOException(
            "Section too small to read at offset " + metadata.getOffset() + ", size "
              + metadata.getSize() + " for tenant " + Bytes.toStringBinary(tenantSectionId));
        }

        try {
          HFileInfo sectionFileInfo = new HFileInfo(sectionContext, getConf());
          local = new HFileStreamReader(sectionContext, sectionFileInfo, cacheConf, getConf());

          LOG.debug("Initializing section indices for tenant at offset {}", metadata.getOffset());
          sectionFileInfo.initMetaAndIndex(local);
          LOG.debug("Successfully initialized indices for section at offset {}",
            metadata.getOffset());

          reader = local;
          LOG.debug("Initialized HFileStreamReader for tenant section ID: {}",
            Bytes.toStringBinary(tenantSectionId));
        } catch (IOException e) {
          if (local != null) {
            try {
              local.close();
            } catch (Exception closeEx) {
              e.addSuppressed(closeEx);
            }
          }
          LOG.error("Failed to initialize section reader", e);
          throw e;
        }

        return local;
      }
    }

    @Override
    public HFileScanner getScanner(Configuration conf, boolean cacheBlocks, boolean pread,
      boolean isCompaction) throws IOException {
      return getReader().getScanner(conf, cacheBlocks, pread, isCompaction);
    }

    @Override
    public void close(boolean evictOnClose) throws IOException {
      HFileReaderImpl local = reader;
      if (local != null) {
        reader = null;
        // HFileStreamReader.close() does not close fileInfo (by design — V3 readers don't
        // own it). But section readers create their own HFileInfo, so we must close it
        // explicitly to free load-on-open block buffers.
        HFileInfo sectionInfo = local.getHFileInfo();
        local.close(evictOnClose);
        if (sectionInfo != null) {
          sectionInfo.close();
        }
        local.getContext().getInputStreamWrapper().close();
      }
    }
  }

  // No close overrides needed; inherited from AbstractMultiTenantReader
}
