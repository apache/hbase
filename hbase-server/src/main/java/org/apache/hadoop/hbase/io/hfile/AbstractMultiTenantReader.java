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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;

/**
 * Abstract base class for multi-tenant HFile readers. This class handles the common
 * functionality for both pread and stream access modes, delegating specific reader
 * creation to subclasses.
 * 
 * The multi-tenant reader acts as a router that:
 * 1. Extracts tenant information from cell keys
 * 2. Locates the appropriate section in the HFile for that tenant
 * 3. Delegates reading operations to a standard v3 reader for that section
 */
@InterfaceAudience.Private
public abstract class AbstractMultiTenantReader extends HFileReaderImpl {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMultiTenantReader.class);
  
  // Reuse constants from writer
  protected final TenantExtractor tenantExtractor;
  protected final Map<ImmutableBytesWritable, SectionReader> sectionReaders;
  protected final HFileBlockIndex.ByteArrayKeyBlockIndexReader sectionIndexReader;
  
  // Private map to store section metadata
  private final Map<ImmutableBytesWritable, SectionMetadata> sectionLocations = new HashMap<>();
  
  /**
   * Constructor for multi-tenant reader
   * 
   * @param context Reader context info
   * @param fileInfo HFile info
   * @param cacheConf Cache configuration values
   * @param conf Configuration
   * @throws IOException If an error occurs during initialization
   */
  public AbstractMultiTenantReader(ReaderContext context, HFileInfo fileInfo,
      CacheConfig cacheConf, Configuration conf) throws IOException {
    super(context, fileInfo, cacheConf, conf);
    
    // Get table properties for tenant configuration
    Map<String, String> tableProperties = getTableProperties();
    
    // Create tenant extractor with consistent configuration
    this.tenantExtractor = TenantExtractorFactory.createTenantExtractor(conf, tableProperties);
    this.sectionReaders = new ConcurrentHashMap<>();
    
    // Initialize section index - this will be used to find tenant sections
    this.sectionIndexReader = new HFileBlockIndex.ByteArrayKeyBlockIndexReader(1);
    
    // Initialize section index using dataBlockIndexReader from parent
    initializeSectionIndex();
    
    LOG.info("Initialized multi-tenant reader for {}", context.getFilePath());
  }
  
  /**
   * Initialize the section index from the file
   * 
   * @throws IOException If an error occurs loading the section index
   */
  protected void initializeSectionIndex() throws IOException {
    // Get the trailer directly
    FixedFileTrailer trailer = fileInfo.getTrailer();
    
    // Access the input stream through the context
    FSDataInputStreamWrapper fsWrapper = context.getInputStreamWrapper();
    FSDataInputStream fsdis = fsWrapper.getStream(fsWrapper.shouldUseHBaseChecksum());
    long originalPosition = fsdis.getPos();
    
    try {
      // Position at the beginning of the load-on-open section
      fsdis.seek(trailer.getLoadOnOpenDataOffset());
      
      // Read the section index
      HFileBlock block = readBlock(trailer.getLoadOnOpenDataOffset(),
                                  trailer.getUncompressedDataIndexSize(), 
                                  true, true, false, true,
                                  BlockType.ROOT_INDEX, null);
      
      // Use the block to initialize our data structure
      initSectionLocations(block);
      
      LOG.debug("Initialized section index with {} entries", getSectionCount());
    } finally {
      // Restore original position
      fsdis.seek(originalPosition);
    }
  }
  
  // Initialize our section location map from the index block
  private void initSectionLocations(HFileBlock indexBlock) {
    ByteBuff buffer = indexBlock.getBufferWithoutHeader();
    
    // First int is the number of entries
    int numEntries = buffer.getInt();
    
    for (int i = 0; i < numEntries; i++) {
      // Each entry has: key length, key bytes, block offset, block on-disk size
      int keyLength = buffer.getInt();
      byte[] key = new byte[keyLength];
      buffer.get(key);
      long offset = buffer.getLong();
      int size = buffer.getInt();
      
      sectionLocations.put(new ImmutableBytesWritable(key), new SectionMetadata(offset, size));
    }
  }
  
  // Get the number of sections
  private int getSectionCount() {
    return sectionLocations.size();
  }
  
  /**
   * Get table properties from the file context if available
   * 
   * @return A map of table properties, or empty map if not available
   */
  protected Map<String, String> getTableProperties() {
    Map<String, String> tableProperties = new HashMap<>();
    
    try {
      // If file context has table name, try to get table properties
      HFileContext fileContext = getFileContext();
      if (fileContext == null || fileContext.getTableName() == null) {
        LOG.debug("Table name not available in HFileContext");
        return tableProperties;
      }
      
      // Get the table descriptor from the Admin API
      TableName tableName = TableName.valueOf(fileContext.getTableName());
      try (Connection conn = ConnectionFactory.createConnection(getConf());
           Admin admin = conn.getAdmin()) {
        TableDescriptor tableDesc = admin.getDescriptor(tableName);
        if (tableDesc != null) {
          // Extract relevant properties for multi-tenant configuration
          tableDesc.getValues().forEach((k, v) -> {
            tableProperties.put(Bytes.toString(k.get()), Bytes.toString(v.get()));
          });
          LOG.debug("Loaded table properties for {}", tableName);
        }
      } catch (Exception e) {
        LOG.warn("Failed to get table descriptor for {}", tableName, e);
      }
    } catch (Exception e) {
      LOG.warn("Error loading table properties", e);
    }
    
    return tableProperties;
  }
  
  /**
   * Metadata for a tenant section within the HFile
   */
  protected static class SectionMetadata {
    final long offset;
    final int size;
    
    SectionMetadata(long offset, int size) {
      this.offset = offset;
      this.size = size;
    }
  }
  
  /**
   * Get metadata for a tenant section
   * 
   * @param tenantPrefix The tenant prefix to look up
   * @return Section metadata or null if not found
   * @throws IOException If an error occurs during lookup
   */
  protected SectionMetadata getSectionMetadata(byte[] tenantPrefix) throws IOException {
    return sectionLocations.get(new ImmutableBytesWritable(tenantPrefix));
  }
  
  /**
   * Get or create a reader for a tenant section
   * 
   * @param tenantPrefix The tenant prefix for the section
   * @return A section reader or null if the section doesn't exist
   * @throws IOException If an error occurs creating the reader
   */
  protected SectionReader getSectionReader(byte[] tenantPrefix) throws IOException {
    ImmutableBytesWritable key = new ImmutableBytesWritable(tenantPrefix);
    
    // Check if we already have a reader for this tenant
    SectionReader reader = sectionReaders.get(key);
    if (reader != null) {
      return reader;
    }
    
    // Create new section reader
    SectionMetadata metadata = getSectionMetadata(tenantPrefix);
    if (metadata == null) {
      LOG.debug("No section found for tenant prefix: {}", Bytes.toStringBinary(tenantPrefix));
      return null;
    }
    
    reader = createSectionReader(tenantPrefix, metadata);
    sectionReaders.put(key, reader);
    LOG.debug("Created section reader for tenant prefix: {}", Bytes.toStringBinary(tenantPrefix));
    return reader;
  }
  
  /**
   * Create appropriate section reader based on type (to be implemented by subclasses)
   * 
   * @param tenantPrefix The tenant prefix
   * @param metadata The section metadata
   * @return A section reader
   * @throws IOException If an error occurs creating the reader
   */
  protected abstract SectionReader createSectionReader(
      byte[] tenantPrefix, SectionMetadata metadata) throws IOException;
  
  /**
   * Get a scanner for this file
   */
  @Override
  public HFileScanner getScanner(Configuration conf, boolean cacheBlocks, 
      boolean pread, boolean isCompaction) {
    return new MultiTenantScanner(conf, cacheBlocks, pread, isCompaction);
  }
  
  /**
   * Simpler scanner method that delegates to the full method
   */
  @Override
  public HFileScanner getScanner(Configuration conf, boolean cacheBlocks, boolean pread) {
    return getScanner(conf, cacheBlocks, pread, false);
  }
  
  /**
   * Abstract base class for section readers
   */
  protected abstract class SectionReader {
    protected final byte[] tenantPrefix;
    protected final SectionMetadata metadata;
    protected HFileReaderImpl reader;
    protected boolean initialized = false;
    
    public SectionReader(byte[] tenantPrefix, SectionMetadata metadata) {
      this.tenantPrefix = tenantPrefix;
      this.metadata = metadata;
    }
    
    /**
     * Get or initialize the underlying reader
     * 
     * @return The underlying HFile reader
     * @throws IOException If an error occurs initializing the reader
     */
    public abstract HFileReaderImpl getReader() throws IOException;
    
    /**
     * Get a scanner for this section
     * 
     * @param conf Configuration to use
     * @param cacheBlocks Whether to cache blocks
     * @param pread Whether to use positional read
     * @param isCompaction Whether this is for a compaction
     * @return A scanner for this section
     * @throws IOException If an error occurs creating the scanner
     */
    public abstract HFileScanner getScanner(Configuration conf, boolean cacheBlocks, 
        boolean pread, boolean isCompaction) throws IOException;
    
    /**
     * Close the section reader
     * 
     * @throws IOException If an error occurs closing the reader
     */
    public void close() throws IOException {
      close(false);
    }
    
    /**
     * Close the section reader
     * 
     * @param evictOnClose whether to evict blocks on close
     * @throws IOException If an error occurs closing the reader
     */
    public abstract void close(boolean evictOnClose) throws IOException;
  }
  
  /**
   * Scanner implementation for multi-tenant HFiles
   */
  protected class MultiTenantScanner implements HFileScanner {
    private final Configuration conf;
    private final boolean cacheBlocks;
    private final boolean pread;
    private final boolean isCompaction;
    
    private byte[] currentTenantPrefix;
    private HFileScanner currentScanner;
    private boolean seeked = false;
    
    public MultiTenantScanner(Configuration conf, boolean cacheBlocks, 
        boolean pread, boolean isCompaction) {
      this.conf = conf;
      this.cacheBlocks = cacheBlocks;
      this.pread = pread;
      this.isCompaction = isCompaction;
    }
    
    @Override
    public boolean isSeeked() {
      return seeked && currentScanner != null && currentScanner.isSeeked();
    }
    
    @Override
    public boolean seekTo() throws IOException {
      // Try default tenant first
      currentTenantPrefix = new byte[0]; // Default tenant prefix
      SectionReader sectionReader = getSectionReader(currentTenantPrefix);
      
      if (sectionReader == null) {
        // Try to find any section if default doesn't exist
        for (ImmutableBytesWritable key : sectionReaders.keySet()) {
          currentTenantPrefix = key.get();
          sectionReader = getSectionReader(currentTenantPrefix);
          if (sectionReader != null) {
            break;
          }
        }
      }
      
      if (sectionReader == null) {
        seeked = false;
        return false;
      }
      
      currentScanner = sectionReader.getScanner(conf, cacheBlocks, pread, isCompaction);
      boolean result = currentScanner.seekTo();
      seeked = result;
      return result;
    }
    
    @Override
    public int seekTo(ExtendedCell key) throws IOException {
      // Extract tenant prefix
      byte[] tenantPrefix = tenantExtractor.extractTenantPrefix(key);
      
      // Get the scanner for this tenant
      SectionReader sectionReader = getSectionReader(tenantPrefix);
      if (sectionReader == null) {
        seeked = false;
        return -1;
      }
      
      // Use the section scanner
      HFileScanner scanner = sectionReader.getScanner(conf, cacheBlocks, pread, isCompaction);
      int result = scanner.seekTo(key);
      if (result != -1) {
        currentTenantPrefix = tenantPrefix;
        currentScanner = scanner;
        seeked = true;
      } else {
        seeked = false;
      }
      
      return result;
    }
    
    @Override
    public int reseekTo(ExtendedCell key) throws IOException {
      assertSeeked();
      
      // Extract tenant prefix
      byte[] tenantPrefix = tenantExtractor.extractTenantPrefix(key);
      
      // If tenant changed, we need to do a full seek
      if (!Bytes.equals(tenantPrefix, currentTenantPrefix)) {
        return seekTo(key);
      }
      
      // Reuse existing scanner for same tenant
      int result = currentScanner.reseekTo(key);
      if (result == -1) {
        seeked = false;
      }
      return result;
    }
    
    @Override
    public boolean seekBefore(ExtendedCell key) throws IOException {
      // Extract tenant prefix
      byte[] tenantPrefix = tenantExtractor.extractTenantPrefix(key);
      
      // Get the scanner for this tenant
      SectionReader sectionReader = getSectionReader(tenantPrefix);
      if (sectionReader == null) {
        seeked = false;
        return false;
      }
      
      // Use the section scanner
      HFileScanner scanner = sectionReader.getScanner(conf, cacheBlocks, pread, isCompaction);
      boolean result = scanner.seekBefore(key);
      if (result) {
        currentTenantPrefix = tenantPrefix;
        currentScanner = scanner;
        seeked = true;
      } else {
        seeked = false;
      }
      
      return result;
    }
    
    @Override
    public ExtendedCell getCell() {
      if (!isSeeked()) {
        return null;
      }
      return currentScanner.getCell();
    }
    
    @Override
    public ExtendedCell getKey() {
      if (!isSeeked()) {
        return null;
      }
      return currentScanner.getKey();
    }
    
    @Override
    public java.nio.ByteBuffer getValue() {
      if (!isSeeked()) {
        return null;
      }
      return currentScanner.getValue();
    }
    
    @Override
    public boolean next() throws IOException {
      assertSeeked();
      
      boolean hasNext = currentScanner.next();
      if (!hasNext) {
        // Try to find the next tenant section
        byte[] nextTenantPrefix = findNextTenantPrefix(currentTenantPrefix);
        if (nextTenantPrefix == null) {
          seeked = false;
          return false;
        }
        
        // Move to the next tenant
        SectionReader nextSectionReader = getSectionReader(nextTenantPrefix);
        if (nextSectionReader == null) {
          seeked = false;
          return false;
        }
        
        currentTenantPrefix = nextTenantPrefix;
        currentScanner = nextSectionReader.getScanner(conf, cacheBlocks, pread, isCompaction);
        boolean result = currentScanner.seekTo();
        seeked = result;
        return result;
      }
      
      return true;
    }
    
    private byte[] findNextTenantPrefix(byte[] currentPrefix) {
      // Simple linear search for the lexicographically next tenant prefix
      byte[] nextPrefix = null;
      
      for (ImmutableBytesWritable key : sectionReaders.keySet()) {
        byte[] candidatePrefix = key.get();
        if (Bytes.compareTo(candidatePrefix, currentPrefix) > 0 && 
            (nextPrefix == null || Bytes.compareTo(candidatePrefix, nextPrefix) < 0)) {
          nextPrefix = candidatePrefix;
        }
      }
      
      return nextPrefix;
    }
    
    private void assertSeeked() {
      if (!isSeeked()) {
        throw new NotSeekedException(getPath());
      }
    }
    
    @Override
    public ExtendedCell getNextIndexedKey() {
      if (!isSeeked()) {
        return null;
      }
      return currentScanner.getNextIndexedKey();
    }
    
    @Override
    public void close() {
      if (currentScanner != null) {
        currentScanner.close();
        currentScanner = null;
      }
      seeked = false;
    }

    @Override
    public void shipped() throws IOException {
      if (currentScanner != null) {
        currentScanner.shipped();
      }
    }

    @Override
    public void recordBlockSize(java.util.function.IntConsumer blockSizeConsumer) {
      if (currentScanner != null) {
        currentScanner.recordBlockSize(blockSizeConsumer);
      }
    }
    
    @Override
    public HFile.Reader getReader() {
      return AbstractMultiTenantReader.this;
    }
  }
  
  /**
   * Close all section readers and release resources
   */
  @Override
  public void close() throws IOException {
    close(false);
  }

  
  /**
   * Get HFile version
   */
  @Override
  public int getMajorVersion() {
    return HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT;
  }
} 