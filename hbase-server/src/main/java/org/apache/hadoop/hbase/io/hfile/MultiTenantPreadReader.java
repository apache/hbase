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
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.fs.Path;

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
    // Tenant index structure is loaded and logged by the parent class
  }

  /**
   * Create a section reader for a specific tenant
   * 
   * @param tenantSectionId The tenant section ID
   * @param metadata The section metadata
   * @return A section reader for the tenant
   * @throws IOException If an error occurs creating the reader
   */
  @Override
  protected SectionReader createSectionReader(byte[] tenantSectionId, SectionMetadata metadata) throws IOException {
    LOG.debug("Creating section reader for tenant section: {}, offset: {}, size: {}",
             Bytes.toStringBinary(tenantSectionId), metadata.getOffset(), metadata.getSize());
            
    // Special handling for non-first sections
    if (metadata.getOffset() > 0) {
      LOG.debug("Non-first section tenant reader: offset={}, size={}, end={}",
               metadata.getOffset(), metadata.getSize(), 
               metadata.getOffset() + metadata.getSize());
               
      // For non-first sections, we need to be especially careful about trailer position
      // Use proper trailer size for HFile v3 (which is 4096 bytes, not 212)
      int trailerSize = FixedFileTrailer.getTrailerSize(3); // HFile v3 trailer size
      long trailerPos = metadata.getOffset() + metadata.getSize() - trailerSize;
      LOG.debug("Trailer should be at absolute position: {}", trailerPos);
    }
    
    return new PreadSectionReader(tenantSectionId, metadata);
  }

  /**
   * Section reader implementation for pread access mode
   */
  protected class PreadSectionReader extends SectionReader {
    private HFileReaderImpl hfileReader;
    
    public PreadSectionReader(byte[] tenantSectionId, SectionMetadata metadata) {
      super(tenantSectionId, metadata);
    }
    
    @Override
    public HFileReaderImpl getReader() throws IOException {
      if (hfileReader != null) {
        return hfileReader;
      }
      synchronized (this) {
        if (hfileReader != null) {
          return hfileReader;
        }
        // Prepare placeholders for contexts for logging in catch
        ReaderContext sectionContext = null;
        ReaderContext perSectionContext = null;
        try {
          // Build section context with offset translation
          LOG.debug("Building section context for tenant at offset {}", metadata.getOffset());
          sectionContext = buildSectionContext(metadata, ReaderContext.ReaderType.PREAD);
          // Override filePath so each tenant section schedules its own prefetch key
          Path containerPath = sectionContext.getFilePath();
          String tenantSectionIdStr = Bytes.toStringBinary(tenantSectionId);
          Path perSectionPath = new Path(containerPath.toString() + "#" + tenantSectionIdStr);
          perSectionContext = ReaderContextBuilder.newBuilder(sectionContext)
              .withFilePath(perSectionPath)
              .build();
          LOG.debug("Created section context (prefetchKey={}) : {}", perSectionPath, perSectionContext);
          
          // Use per-section context for info and reader
          LOG.debug("Creating HFileInfo for tenant section at offset {}", metadata.getOffset());
          HFileInfo info = new HFileInfo(perSectionContext, getConf());
          // Extra debug for non-first sections
          if (metadata.getOffset() > 0) {
            int trailerSize = FixedFileTrailer.getTrailerSize(3); // HFile v3 trailer size
            LOG.debug("Section size: {}, expected trailer at relative offset: {}", metadata.getSize(), metadata.getSize() - trailerSize);
            LOG.debug("Trailer position in absolute coordinates: {}", metadata.getOffset() + metadata.getSize() - trailerSize);
          }
          LOG.debug("Initializing section indices for tenant at offset {}", metadata.getOffset());
          // Instantiate the PreadReader for this section
          LOG.debug("Creating HFilePreadReader for tenant section at offset {}", metadata.getOffset());
          hfileReader = new HFilePreadReader(perSectionContext, info, cacheConf, getConf());
          // Init metadata and indices
          LOG.debug("About to initialize metadata and indices for section at offset {}", metadata.getOffset());
          info.initMetaAndIndex(hfileReader);
          LOG.debug("Successfully initialized indices for section at offset {}", metadata.getOffset());
          LOG.debug("Initialized HFilePreadReader for tenant section ID: {}", Bytes.toStringBinary(tenantSectionId));
          return hfileReader;
        } catch (IOException e) {
          LOG.error("Failed to initialize section reader", e);
          // Log basic diagnostic info (omit context to avoid scope issues)
          if (metadata.getOffset() > 0) {
            LOG.error("Error details for section at offset {}: size={}, endpoint={}",
                metadata.getOffset(), metadata.getSize(), metadata.getOffset() + metadata.getSize());
          }
          throw e;
        }
      }
    }
    
    @Override
    public HFileScanner getScanner(Configuration conf, boolean cacheBlocks, 
        boolean pread, boolean isCompaction) throws IOException {
      return getReader().getScanner(conf, cacheBlocks, true, isCompaction);
    }
    
    @Override
    public void close(boolean evictOnClose) throws IOException {
      if (hfileReader != null) {
        HFileReaderImpl r = hfileReader;
        hfileReader = null;
        try {
          r.close(evictOnClose);
        } finally {
          // Unbuffer section wrapper to free socket/buffer
          r.getContext().getInputStreamWrapper().unbuffer();
        }
      }
    }
  }
} 