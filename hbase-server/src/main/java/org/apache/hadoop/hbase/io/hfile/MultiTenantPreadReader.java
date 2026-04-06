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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HFile reader for multi-tenant HFiles in PREAD (random access) mode. This implementation creates
 * HFilePreadReader instances for each tenant section.
 */
@InterfaceAudience.Private
public class MultiTenantPreadReader extends AbstractMultiTenantReader {
  private static final Logger LOG = LoggerFactory.getLogger(MultiTenantPreadReader.class);

  /**
   * Constructor for multi-tenant pread reader.
   * @param context   Reader context info
   * @param fileInfo  HFile info
   * @param cacheConf Cache configuration values
   * @param conf      Configuration
   * @throws IOException If an error occurs during initialization
   */
  public MultiTenantPreadReader(ReaderContext context, HFileInfo fileInfo, CacheConfig cacheConf,
    Configuration conf) throws IOException {
    super(context, fileInfo, cacheConf, conf);
    // Tenant index structure is loaded and logged by the parent class
    prefetchBlocksOnOpenIfRequested();
  }

  /**
   * Create a section reader for a specific tenant.
   * <p>
   * Creates a PreadSectionReader that handles positional read access to a specific tenant section
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

    return new PreadSectionReader(tenantSectionId, metadata);
  }

  /**
   * Section reader implementation for pread (positional read) access mode.
   * <p>
   * This implementation creates HFilePreadReader instances for each tenant section, providing
   * efficient random access to data within specific tenant boundaries.
   */
  protected class PreadSectionReader extends SectionReader {

    /**
     * Constructor for PreadSectionReader.
     * @param tenantSectionId The tenant section ID
     * @param metadata        The section metadata
     */
    public PreadSectionReader(byte[] tenantSectionId, SectionMetadata metadata) {
      super(tenantSectionId, metadata);
      LOG.debug("Created PreadSectionReader for tenant section ID: {}",
        Bytes.toStringBinary(this.tenantSectionId));
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

        try {
          ReaderContext sectionContext =
            buildSectionContext(metadata, ReaderContext.ReaderType.PREAD);
          if (sectionContext == null) {
            throw new IOException(
              "Section too small to read at offset " + metadata.getOffset() + ", size "
                + metadata.getSize() + " for tenant " + Bytes.toStringBinary(tenantSectionId));
          }

          Path containerPath = sectionContext.getFilePath();
          String tenantSectionIdStr = Bytes.toStringBinary(tenantSectionId);
          Path perSectionPath = new Path(containerPath.toString() + "#" + tenantSectionIdStr);
          ReaderContext perSectionContext =
            ReaderContextBuilder.newBuilder(sectionContext).withFilePath(perSectionPath).build();

          HFileInfo info = new HFileInfo(perSectionContext, getConf());
          local = new HFilePreadReader(perSectionContext, info, cacheConf, getConf());

          reader = local;
          LOG.debug("Successfully initialized HFilePreadReader for tenant section ID: {}",
            Bytes.toStringBinary(tenantSectionId));

          return local;
        } catch (IOException e) {
          LOG.error("Failed to initialize section reader for tenant section at offset {}: {}",
            metadata.getOffset(), e.getMessage());
          throw e;
        }
      }
    }

    @Override
    public HFileScanner getScanner(Configuration conf, boolean cacheBlocks, boolean pread,
      boolean isCompaction) throws IOException {
      HFileReaderImpl local = getReader();
      HFileScanner scanner = local.getScanner(conf, cacheBlocks, true, isCompaction);
      LOG.debug(
        "PreadSectionReader.getScanner for tenant section ID: {}, reader: {}, " + "scanner: {}",
        Bytes.toStringBinary(tenantSectionId), local, scanner);
      return scanner;
    }

    @Override
    public void close(boolean evictOnClose) throws IOException {
      HFileReaderImpl local = reader;
      if (local != null) {
        reader = null;
        local.close(evictOnClose);
      }
    }
  }
}
