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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.common.cache.Cache;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.common.cache.RemovalListener;
import org.apache.hbase.thirdparty.com.google.common.cache.RemovalNotification;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.MultiTenantFSDataInputStreamWrapper;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
  
  // Static cache for table properties to avoid repeated loading
  private static final Cache<TableName, Map<String, String>> TABLE_PROPERTIES_CACHE = 
      CacheBuilder.newBuilder()
          .maximumSize(100)
          .expireAfterWrite(5, java.util.concurrent.TimeUnit.MINUTES)
          .build();
  
  // Reuse constants from writer
  protected final TenantExtractor tenantExtractor;
  protected final SectionIndexManager.Reader sectionIndexReader;
  
  // Add cache configuration
  private static final String SECTION_READER_CACHE_SIZE = "hbase.multi.tenant.reader.cache.size";
  private static final int DEFAULT_SECTION_READER_CACHE_SIZE = 100;
  
  // Prefetch configuration for sequential access
  private static final String SECTION_PREFETCH_ENABLED = "hbase.multi.tenant.reader.prefetch.enabled";
  private static final boolean DEFAULT_SECTION_PREFETCH_ENABLED = true;
  
  // Cache for section readers with bounded size and eviction
  protected final Cache<ImmutableBytesWritable, SectionReader> sectionReaderCache;
  
  // Private map to store section metadata
  private final Map<ImmutableBytesWritable, SectionMetadata> sectionLocations = new HashMap<>();
  
  // Add sorted list for efficient navigation
  private List<ImmutableBytesWritable> sortedSectionIds;
  
  // Tenant index structure information
  private int tenantIndexLevels = 1;
  private int tenantIndexMaxChunkSize = SectionIndexManager.DEFAULT_MAX_CHUNK_SIZE;
  private final boolean prefetchEnabled;
  
  // Partial key optimization
  private int commonPrefixLength = -1; // -1 means not computed yet
  
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
    
    // Initialize bounded cache with eviction
    int cacheSize = conf.getInt(SECTION_READER_CACHE_SIZE, DEFAULT_SECTION_READER_CACHE_SIZE);
    this.sectionReaderCache = CacheBuilder.newBuilder()
        .maximumSize(cacheSize)
        .recordStats()
        .removalListener(new RemovalListener<ImmutableBytesWritable, SectionReader>() {
          @Override
          public void onRemoval(RemovalNotification<ImmutableBytesWritable, SectionReader> notification) {
            SectionReader reader = notification.getValue();
            if (reader != null) {
              try {
                reader.close();
                LOG.debug("Evicted section reader for tenant: {}", 
                    Bytes.toStringBinary(notification.getKey().get()));
              } catch (IOException e) {
                LOG.warn("Error closing evicted section reader", e);
              }
            }
          }
        })
        .build();
    
    // Initialize section index reader
    this.sectionIndexReader = new SectionIndexManager.Reader();
    
    // Initialize section index using dataBlockIndexReader from parent
    initializeSectionIndex();
    
    // Load tenant index structure information
    loadTenantIndexStructureInfo();

    // Create tenant extractor with consistent configuration
    this.tenantExtractor = TenantExtractorFactory.createFromReader(this);
    
    // Initialize prefetch configuration
    this.prefetchEnabled = conf.getBoolean(SECTION_PREFETCH_ENABLED, DEFAULT_SECTION_PREFETCH_ENABLED);
    
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
      LOG.debug("Seeking to load-on-open section at offset {}", trailer.getLoadOnOpenDataOffset());
      
      // In HFile v4, the tenant index is stored at the load-on-open offset
      HFileBlock rootIndexBlock = getUncachedBlockReader().readBlockData(
          trailer.getLoadOnOpenDataOffset(), -1, true, false, false);
      
      // Validate this is a root index block
      if (rootIndexBlock.getBlockType() != BlockType.ROOT_INDEX) {
        throw new IOException("Expected ROOT_INDEX block for tenant index in HFile v4, found " + 
            rootIndexBlock.getBlockType() + " at offset " + trailer.getLoadOnOpenDataOffset());
      }
      
      // Load the section index from the root block
      sectionIndexReader.loadSectionIndex(rootIndexBlock);
      
      // Copy section info to our internal data structures
      initSectionLocations();
      
      LOG.debug("Initialized tenant section index with {} entries", getSectionCount());
    } catch (IOException e) {
      LOG.error("Failed to load tenant section index", e);
      throw e;
    } finally {
      // Restore original position
      fsdis.seek(originalPosition);
    }
  }
  
  /**
   * Load information about the tenant index structure from file info
   */
  private void loadTenantIndexStructureInfo() {
    // Get tenant index level information
    byte[] tenantIndexLevelsBytes = fileInfo.get(Bytes.toBytes("TENANT_INDEX_LEVELS"));
    if (tenantIndexLevelsBytes != null) {
      tenantIndexLevels = Bytes.toInt(tenantIndexLevelsBytes);
    }
    
    // Get chunk size for multi-level indices
    if (tenantIndexLevels > 1) {
      byte[] chunkSizeBytes = fileInfo.get(Bytes.toBytes("TENANT_INDEX_MAX_CHUNK"));
      if (chunkSizeBytes != null) {
        tenantIndexMaxChunkSize = Bytes.toInt(chunkSizeBytes);
      }
    }
    
    // Log tenant index structure information
    int numSections = getSectionCount();
    if (tenantIndexLevels > 1) {
      LOG.info("Multi-tenant HFile loaded with {} sections using {}-level tenant index " +
               "(maxChunkSize={})", 
               numSections, tenantIndexLevels, tenantIndexMaxChunkSize);
    } else {
      LOG.info("Multi-tenant HFile loaded with {} sections using single-level tenant index",
               numSections);
    }
    
    LOG.debug("Tenant index details: levels={}, chunkSize={}, sections={}",
              tenantIndexLevels, tenantIndexMaxChunkSize, numSections);
  }
  
  /**
   * Get the number of levels in the tenant index
   * 
   * @return The number of levels (1 for single-level, 2+ for multi-level)
   */
  public int getTenantIndexLevels() {
    return tenantIndexLevels;
  }
  
  /**
   * Get the maximum chunk size used in the tenant index
   * 
   * @return The maximum entries per index block
   */
  public int getTenantIndexMaxChunkSize() {
    return tenantIndexMaxChunkSize;
  }
  
  // Initialize our section location map from the index reader
  private void initSectionLocations() {
    for (SectionIndexManager.SectionIndexEntry entry : sectionIndexReader.getSections()) {
      sectionLocations.put(
          new ImmutableBytesWritable(entry.getTenantPrefix()),
          new SectionMetadata(entry.getOffset(), entry.getSectionSize()));
    }
    
    // Create sorted list for efficient binary search
    sortedSectionIds = new ArrayList<>(sectionLocations.keySet());
    sortedSectionIds.sort((a, b) -> Bytes.compareTo(a.get(), b.get()));
    LOG.debug("Initialized {} sorted section IDs for efficient navigation", sortedSectionIds.size());
  }
  
  // Get the number of sections
  private int getSectionCount() {
    return sectionLocations.size();
  }
  
  /**
   * Get the total number of tenant sections in this file
   * @return The number of sections
   */
  public int getTotalSectionCount() {
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
      
      // Get the table descriptor from the cache or Admin API
      TableName tableName = TableName.valueOf(fileContext.getTableName());
      
      try {
        // Try to get from cache first
        tableProperties = TABLE_PROPERTIES_CACHE.get(tableName, () -> {
          Map<String, String> props = new HashMap<>();
          try (Connection conn = ConnectionFactory.createConnection(getConf());
               Admin admin = conn.getAdmin()) {
            TableDescriptor tableDesc = admin.getDescriptor(tableName);
            if (tableDesc != null) {
              // Extract relevant properties for multi-tenant configuration
              tableDesc.getValues().forEach((k, v) -> {
                props.put(Bytes.toString(k.get()), Bytes.toString(v.get()));
              });
              LOG.debug("Loaded and cached table properties for {}", tableName);
            }
          }
          return props;
        });
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
    
    long getOffset() {
      return offset;
    }
    
    int getSize() {
      return size;
    }
  }
  
  /**
   * Get metadata for a tenant section
   * 
   * @param tenantSectionId The tenant section ID to look up
   * @return Section metadata or null if not found
   * @throws IOException If an error occurs during lookup
   */
  protected SectionMetadata getSectionMetadata(byte[] tenantSectionId) throws IOException {
    return sectionLocations.get(new ImmutableBytesWritable(tenantSectionId));
  }
  
  /**
   * Get or create a reader for a tenant section
   * 
   * @param tenantSectionId The tenant section ID for the section
   * @return A section reader or null if the section doesn't exist
   * @throws IOException If an error occurs creating the reader
   */
  protected SectionReader getSectionReader(byte[] tenantSectionId) throws IOException {
    ImmutableBytesWritable key = new ImmutableBytesWritable(tenantSectionId);
    // Lookup the section metadata
    SectionMetadata metadata = getSectionMetadata(tenantSectionId);
    if (metadata == null) {
      LOG.debug("No section found for tenant section ID: {}", Bytes.toStringBinary(tenantSectionId));
      return null;
    }
    try {
      // Use cache's get method with loader for atomic creation
      return sectionReaderCache.get(key, () -> {
        SectionReader reader = createSectionReader(tenantSectionId, metadata);
        LOG.debug("Created section reader for tenant section ID: {}", Bytes.toStringBinary(tenantSectionId));
        return reader;
      });
    } catch (Exception e) {
      if (e instanceof IOException) {
        throw (IOException) e;
      }
      throw new IOException("Failed to get section reader", e);
    }
  }
  
  /**
   * Create appropriate section reader based on type (to be implemented by subclasses)
   * 
   * @param tenantSectionId The tenant section ID
   * @param metadata The section metadata
   * @return A section reader
   * @throws IOException If an error occurs creating the reader
   */
  protected abstract SectionReader createSectionReader(
      byte[] tenantSectionId, SectionMetadata metadata) throws IOException;
  
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
    protected final byte[] tenantSectionId;
    protected final SectionMetadata metadata;
    protected HFileReaderImpl reader;
    protected boolean initialized = false;
    protected long sectionBaseOffset;
    
    public SectionReader(byte[] tenantSectionId, SectionMetadata metadata) {
      this.tenantSectionId = tenantSectionId;
      this.metadata = metadata;
      this.sectionBaseOffset = metadata.getOffset();
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
    protected final Configuration conf;
    protected final boolean cacheBlocks;
    protected final boolean pread;
    protected final boolean isCompaction;
    
    protected byte[] currentTenantSectionId;
    protected HFileScanner currentScanner;
    protected SectionReader currentSectionReader;
    protected boolean seeked = false;
    
    public MultiTenantScanner(Configuration conf, boolean cacheBlocks, 
        boolean pread, boolean isCompaction) {
      this.conf = conf;
      this.cacheBlocks = cacheBlocks;
      this.pread = pread;
      this.isCompaction = isCompaction;
    }
    
    /**
     * Switch to a new section reader, properly managing reference counts
     */
    private void switchToSectionReader(SectionReader newReader, byte[] sectionId) throws IOException {
      // Release previous reader
      if (currentSectionReader != null) {
        try {
          // Note: We don't close the reader here as it might be cached and reused
          // The cache eviction will handle the actual closing
          currentSectionReader = null;
          currentScanner = null;
        } catch (Exception e) {
          LOG.warn("Error releasing previous section reader", e);
        }
      }
      
      // Set new reader
      currentSectionReader = newReader;
      if (currentSectionReader != null) {
        currentTenantSectionId = sectionId;
        currentScanner = currentSectionReader.getScanner(conf, cacheBlocks, pread, isCompaction);
      } else {
        currentTenantSectionId = null;
        currentScanner = null;
      }
    }
    
    @Override
    public boolean isSeeked() {
      return seeked && currentScanner != null && currentScanner.isSeeked();
    }
    
    @Override
    public boolean seekTo() throws IOException {
      // Get the first section from the sorted section index
      if (!sortedSectionIds.isEmpty()) {
        // Get the first section ID from the sorted list
        byte[] firstSectionId = sortedSectionIds.get(0).get();
        
        SectionReader sectionReader = getSectionReader(firstSectionId);
        if (sectionReader != null) {
          switchToSectionReader(sectionReader, firstSectionId);
          boolean result = currentScanner.seekTo();
          seeked = result;
          return result;
        }
      }
      
      // If we reach here, no sections were found or seeking failed
      seeked = false;
      return false;
    }
    
    @Override
    public int seekTo(ExtendedCell key) throws IOException {
      // Extract tenant section ID
      byte[] tenantSectionId = tenantExtractor.extractTenantSectionId(key);
      
      // Get the scanner for this tenant section
      SectionReader sectionReader = getSectionReader(tenantSectionId);
      if (sectionReader == null) {
        seeked = false;
        return -1;
      }
      
      // Use the section scanner
      switchToSectionReader(sectionReader, tenantSectionId);
      int result = currentScanner.seekTo(key);
      if (result != -1) {
        seeked = true;
      } else {
        seeked = false;
      }
      
      return result;
    }
    
    @Override
    public int reseekTo(ExtendedCell key) throws IOException {
      assertSeeked();
      
      // Extract tenant section ID
      byte[] tenantSectionId = tenantExtractor.extractTenantSectionId(key);
      
      // If tenant section changed, we need to do a full seek
      if (!Bytes.equals(tenantSectionId, currentTenantSectionId)) {
        return seekTo(key);
      }
      
      // Reuse existing scanner for same tenant section
      int result = currentScanner.reseekTo(key);
      if (result == -1) {
        seeked = false;
      }
      return result;
    }
    
    @Override
    public boolean seekBefore(ExtendedCell key) throws IOException {
      // Extract tenant section ID
      byte[] tenantSectionId = tenantExtractor.extractTenantSectionId(key);
      
      // Get the scanner for this tenant section
      SectionReader sectionReader = getSectionReader(tenantSectionId);
      if (sectionReader == null) {
        seeked = false;
        return false;
      }
      
      // Use the section scanner
      switchToSectionReader(sectionReader, tenantSectionId);
      boolean result = currentScanner.seekBefore(key);
      if (result) {
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
        byte[] nextTenantSectionId = findNextTenantSectionId(currentTenantSectionId);
        if (nextTenantSectionId == null) {
          seeked = false;
          return false;
        }
        
        // Prefetch the section after next if enabled
        if (prefetchEnabled) {
          prefetchNextSection(nextTenantSectionId);
        }
        
        // Move to the next tenant section
        SectionReader nextSectionReader = getSectionReader(nextTenantSectionId);
        if (nextSectionReader == null) {
          seeked = false;
          return false;
        }
        
        switchToSectionReader(nextSectionReader, nextTenantSectionId);
        boolean result = currentScanner.seekTo();
        seeked = result;
        return result;
      }
      
      return true;
    }
    
    /**
     * Prefetch the next section after the given one for sequential access optimization
     */
    private void prefetchNextSection(byte[] currentSectionId) {
      try {
        byte[] nextSectionId = findNextTenantSectionId(currentSectionId);
        if (nextSectionId != null) {
          // Trigger async load by just getting the reader (cache will hold it)
          getSectionReader(nextSectionId);
          LOG.debug("Prefetched section: {}", Bytes.toStringBinary(nextSectionId));
        }
      } catch (Exception e) {
        // Prefetch is best-effort, don't fail the operation
        LOG.debug("Failed to prefetch next section", e);
      }
    }
    
    private byte[] findNextTenantSectionId(byte[] currentSectionId) {
      // Use binary search on sorted list for O(log n) performance
      int currentIndex = -1;
      
      // Binary search to find current position
      int low = 0;
      int high = sortedSectionIds.size() - 1;
      
      while (low <= high) {
        int mid = (low + high) >>> 1;
        int cmp = Bytes.compareTo(sortedSectionIds.get(mid).get(), currentSectionId);
        
        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          currentIndex = mid;
          break;
        }
      }
      
      // If we found the current section and there's a next one, return it
      if (currentIndex >= 0 && currentIndex < sortedSectionIds.size() - 1) {
        return sortedSectionIds.get(currentIndex + 1).get();
      }
      
      // If we didn't find exact match but low is valid, it's the next section
      if (currentIndex < 0 && low < sortedSectionIds.size()) {
        return sortedSectionIds.get(low).get();
      }
      
      return null;
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
      if (currentSectionReader != null) {
        // Don't close the section reader - let cache eviction handle it
        currentSectionReader = null;
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
   * Close all section readers and underlying resources, with optional block eviction
   */
  @Override
  public void close(boolean evictOnClose) throws IOException {
    // Close and invalidate all cached section readers
    // The removal listener will handle closing each reader
    sectionReaderCache.invalidateAll();
    
    // Close filesystem block reader streams
    if (fsBlockReader != null) {
      fsBlockReader.closeStreams();
    }
    
    // Unbuffer the main input stream wrapper
    context.getInputStreamWrapper().unbuffer();
  }

  /**
   * Get HFile version
   */
  @Override
  public int getMajorVersion() {
    return HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT;
  }

  /**
   * Build a section context with the appropriate offset translation wrapper
   * 
   * @param metadata The section metadata
   * @param readerType The type of reader (PREAD or STREAM)
   * @return A reader context for the section
   */
  protected ReaderContext buildSectionContext(SectionMetadata metadata, 
                                            ReaderContext.ReaderType readerType) throws IOException {
    // Create a special wrapper with offset translation capabilities
    FSDataInputStreamWrapper parentWrapper = context.getInputStreamWrapper();
    LOG.debug("Creating MultiTenantFSDataInputStreamWrapper with offset translation from parent at offset {}", 
             metadata.getOffset());
    
    MultiTenantFSDataInputStreamWrapper sectionWrapper = 
        new MultiTenantFSDataInputStreamWrapper(parentWrapper, metadata.getOffset());
    
    // In HFile format, each tenant section is a complete HFile with a trailer,
    // so we need to properly handle trailer positioning for each section
    
    // Calculate section size and endpoint
    int sectionSize = metadata.getSize();
    long sectionEndpoint = metadata.getOffset() + metadata.getSize();
    // HFile v3 trailer size is 4096 bytes (from FixedFileTrailer.getTrailerSize(3))
    // For sections, the trailer is at the end of each section
    int trailerSize = FixedFileTrailer.getTrailerSize(3); // HFile v3 sections are HFile v3 format
    
    if (sectionSize < trailerSize) {
      LOG.warn("Section size {} for offset {} is smaller than minimum trailer size {}",
               sectionSize, metadata.getOffset(), trailerSize);
      return null;
    }
    
    LOG.debug("Section context: offset={}, size={}, endPos={}, trailer expected at {}",
             metadata.getOffset(), sectionSize, sectionEndpoint, 
             sectionEndpoint - trailerSize);
    
    // Log additional debug information to validate blocks and headers
    LOG.debug("Block boundary details: section starts at absolute position {}, " +
             "first block header should be at this position", metadata.getOffset());
    
    // If this is not the first section, log detailed information about block alignment
    if (metadata.getOffset() > 0) {
      LOG.debug("Non-first section requires correct offset translation for all block operations");
      LOG.debug("First block in section: relative pos=0, absolute pos={}", metadata.getOffset());
      LOG.debug("CHECKSUM_TYPE_INDEX position should be translated from relative pos 24 to absolute pos {}",
               metadata.getOffset() + 24);
    }
    
    // Build the reader context with proper file size calculation
    // This ensures HFileReaderImpl correctly finds the trailer at (offset + size - trailerSize)
    ReaderContext sectionContext = ReaderContextBuilder.newBuilder(context)
        .withInputStreamWrapper(sectionWrapper)
        .withFilePath(context.getFilePath())
        .withReaderType(readerType)
        .withFileSystem(context.getFileSystem())
        .withFileSize(sectionSize) // Use section size; wrapper adds the offset when seeking
        .build();
    
    LOG.debug("Created section reader context: {}", sectionContext);
    return sectionContext;
  }

  /**
   * Find all tenant sections that could potentially match a partial row key.
   * This is used when the client provides a partial row key that doesn't have
   * enough information to definitively determine the tenant ID.
   * 
   * @param partialRowKey The partial row key
   * @return An array of tenant section IDs that could match the partial key
   */
  protected byte[][] findSectionsForPartialKey(byte[] partialRowKey) {
    if (partialRowKey == null || partialRowKey.length == 0) {
      // For empty key, return all sections
      return getAllTenantSectionIds();
    }
    
    // Special case: If the partial key is longer than needed to identify tenant,
    // we can use regular tenant extraction
    DefaultTenantExtractor defaultExtractor = getDefaultExtractor();
    if (defaultExtractor != null) {
      int neededLength = defaultExtractor.getPrefixLength();
      if (partialRowKey.length >= neededLength) {
        // We have enough information for exact tenant identification
        LOG.debug("Partial key contains full tenant information, using exact tenant lookup");
        // Create a dummy cell to extract tenant section ID
        Cell dummyCell = createDummyCellFromKey(partialRowKey);
        byte[] tenantSectionId = tenantExtractor.extractTenantSectionId(dummyCell);
        return new byte[][] { tenantSectionId };
      }
    }
    
    // Optimize: If all sections share a common prefix, we can quickly filter
    if (commonPrefixLength == -1) {
      computeCommonPrefixLength();
    }
    
    if (commonPrefixLength > 0 && partialRowKey.length >= commonPrefixLength) {
      // Check if the partial key matches the common prefix
      boolean matchesCommon = true;
      for (int i = 0; i < commonPrefixLength && i < partialRowKey.length; i++) {
        byte firstSectionByte = sortedSectionIds.get(0).get()[i];
        if (partialRowKey[i] != firstSectionByte) {
          matchesCommon = false;
          break;
        }
      }
      
      if (!matchesCommon) {
        // Partial key doesn't match common prefix - no sections will match
        LOG.debug("Partial key doesn't match common prefix, returning empty result");
        return new byte[0][];
      }
    }
    
    // For partial keys without complete tenant identification, find all
    // potential matching sections
    LOG.debug("Finding sections that could contain row key starting with: {}", 
              org.apache.hadoop.hbase.util.Bytes.toStringBinary(partialRowKey));
    
    // Build candidate list based on prefix matching
    return findPotentialTenantSectionsForPartialKey(partialRowKey);
  }
  
  /**
   * Compute the length of the common prefix shared by all sections
   */
  private void computeCommonPrefixLength() {
    if (sortedSectionIds.isEmpty()) {
      commonPrefixLength = 0;
      return;
    }
    
    if (sortedSectionIds.size() == 1) {
      // Only one section, common prefix is the entire section ID
      commonPrefixLength = sortedSectionIds.get(0).get().length;
      return;
    }
    
    // Compare first and last section IDs to find common prefix
    byte[] first = sortedSectionIds.get(0).get();
    byte[] last = sortedSectionIds.get(sortedSectionIds.size() - 1).get();
    
    int minLength = Math.min(first.length, last.length);
    commonPrefixLength = 0;
    
    for (int i = 0; i < minLength; i++) {
      if (first[i] == last[i]) {
        commonPrefixLength++;
      } else {
        break;
      }
    }
    
    LOG.debug("Computed common prefix length: {} bytes", commonPrefixLength);
  }
  
  /**
   * Create a dummy cell from a partial row key for tenant extraction
   * 
   * @param rowKey The row key to use
   * @return A cell using the provided row key
   */
  private Cell createDummyCellFromKey(byte[] rowKey) {
    // Create a KeyValue with the given row key and empty family/qualifier for tenant extraction
    return new KeyValue(rowKey, HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY);
  }
  
  /**
   * Get the default tenant extractor if it's the current implementation
   * 
   * @return The default tenant extractor, or null if using a custom implementation
   */
  private DefaultTenantExtractor getDefaultExtractor() {
    if (tenantExtractor instanceof DefaultTenantExtractor) {
      return (DefaultTenantExtractor) tenantExtractor;
    }
    return null;
  }
  
  /**
   * Get all tenant section IDs present in the file
   * 
   * @return An array of all tenant section IDs
   */
  protected byte[][] getAllTenantSectionIds() {
    byte[][] allIds = new byte[sectionLocations.size()][];
    int i = 0;
    for (ImmutableBytesWritable key : sectionLocations.keySet()) {
      allIds[i++] = key.copyBytes();
    }
    return allIds;
  }
  
  /**
   * Find all tenant sections that could potentially match a partial row key.
   * This implements the core logic to search for matching sections.
   * 
   * @param partialRowKey The partial row key
   * @return An array of tenant section IDs that could match the partial key
   */
  private byte[][] findPotentialTenantSectionsForPartialKey(byte[] partialRowKey) {
    // In order to handle partial keys, determine if the partial key
    // gives us any prefix information that can narrow down the sections
    DefaultTenantExtractor defaultExtractor = getDefaultExtractor();
    
    if (defaultExtractor != null) {
      // For the default extractor, we know the prefix length
      int prefixLength = defaultExtractor.getPrefixLength();
      
      // Extract the partial prefix information we have
      int availablePrefixLength = Math.min(partialRowKey.length, prefixLength);
      if (availablePrefixLength <= 0) {
        LOG.debug("No prefix information available, must scan all sections");
        return getAllTenantSectionIds();
      }
      
      // Extract the partial prefix we have (always from beginning of row)
      byte[] partialPrefix = new byte[availablePrefixLength];
      System.arraycopy(partialRowKey, 0, partialPrefix, 0, availablePrefixLength);
      LOG.debug("Using partial prefix for section filtering: {}", 
                org.apache.hadoop.hbase.util.Bytes.toStringBinary(partialPrefix));
      
      // Find all sections whose prefix starts with the partial prefix
      return findSectionsWithMatchingPrefix(partialPrefix, availablePrefixLength);
    } else {
      // With custom tenant extractors, we can't make assumptions about the structure
      // We need to include all sections
      LOG.debug("Using custom tenant extractor, must scan all sections");
      return getAllTenantSectionIds();
    }
  }
  
  /**
   * Find all sections whose tenant ID starts with the given partial prefix
   * 
   * @param partialPrefix The partial prefix to match
   * @param prefixLength The length of the partial prefix
   * @return An array of tenant section IDs that match the partial prefix
   */
  private byte[][] findSectionsWithMatchingPrefix(byte[] partialPrefix, int prefixLength) {
    java.util.List<byte[]> matchingSections = new java.util.ArrayList<>();
    
    // Scan all sections and check for prefix match
    for (ImmutableBytesWritable key : sectionLocations.keySet()) {
      byte[] sectionId = key.copyBytes();
      // Check if this section's ID starts with the partial prefix
      if (startsWith(sectionId, partialPrefix, prefixLength)) {
        LOG.debug("Section ID {} matches partial prefix {}", 
                  org.apache.hadoop.hbase.util.Bytes.toStringBinary(sectionId),
                  org.apache.hadoop.hbase.util.Bytes.toStringBinary(partialPrefix));
        matchingSections.add(sectionId);
      }
    }
    
    LOG.debug("Found {} sections matching partial prefix", matchingSections.size());
    return matchingSections.toArray(new byte[matchingSections.size()][]);
  }
  
  /**
   * Check if an array starts with the given prefix
   * 
   * @param array The array to check
   * @param prefix The prefix to check for
   * @param prefixLength The length of the prefix
   * @return true if the array starts with the prefix
   */
  private boolean startsWith(byte[] array, byte[] prefix, int prefixLength) {
    if (array.length < prefixLength) {
      return false;
    }
    
    for (int i = 0; i < prefixLength; i++) {
      if (array[i] != prefix[i]) {
        return false;
      }
    }
    
    return true;
  }
  
  /**
   * Get a scanner for scanning with a partial row key.
   * This creates a scanner that will scan all sections that could potentially
   * match the partial row key.
   * 
   * @param conf Configuration to use
   * @param cacheBlocks Whether to cache blocks
   * @param pread Whether to use positional read
   * @param isCompaction Whether this is for a compaction
   * @param partialRowKey The partial row key to scan for
   * @return A scanner that will scan all potentially matching sections
   */
  public HFileScanner getScannerForPartialKey(Configuration conf, boolean cacheBlocks, 
      boolean pread, boolean isCompaction, byte[] partialRowKey) {
    return new PartialKeyMultiTenantScanner(conf, cacheBlocks, pread, isCompaction, partialRowKey);
  }

  /**
   * Scanner implementation for multi-tenant HFiles that handles partial row keys
   * by scanning across multiple tenant sections.
   */
  protected class PartialKeyMultiTenantScanner extends MultiTenantScanner {
    private final byte[] partialRowKey;
    private final byte[][] candidateSectionIds;
    private int currentSectionIndex;
    
    public PartialKeyMultiTenantScanner(Configuration conf, boolean cacheBlocks, 
        boolean pread, boolean isCompaction, byte[] partialRowKey) {
      super(conf, cacheBlocks, pread, isCompaction);
      this.partialRowKey = partialRowKey;
      this.candidateSectionIds = findSectionsForPartialKey(partialRowKey);
      this.currentSectionIndex = 0;
      LOG.debug("Created PartialKeyMultiTenantScanner with {} candidate sections", 
                candidateSectionIds.length);
    }
    
    @Override
    public boolean seekTo() throws IOException {
      if (candidateSectionIds.length == 0) {
        return false;
      }
      
      // Start with the first candidate section
      return seekToNextCandidateSection(0);
    }
    
    @Override
    public int seekTo(ExtendedCell key) throws IOException {
      // If we have a complete key, use the parent implementation
      if (key != null) {
        return super.seekTo(key);
      }
      
      // Otherwise, start a partial key scan
      if (seekTo()) {
        // Successfully seeked to first position
        return 0;
      }
      return -1;
    }
    
    /**
     * Seek to the next candidate section starting from the given index
     * 
     * @param startIndex The index to start from
     * @return true if successfully seeked to a section
     * @throws IOException If an error occurs
     */
    private boolean seekToNextCandidateSection(int startIndex) throws IOException {
      for (int i = startIndex; i < candidateSectionIds.length; i++) {
        currentSectionIndex = i;
        byte[] sectionId = candidateSectionIds[i];
        LOG.debug("Attempting to seek to section {} (index {})", 
                 org.apache.hadoop.hbase.util.Bytes.toStringBinary(sectionId), i);
        
        // Try to seek to this section
        SectionReader sectionReader = getSectionReader(sectionId);
        if (sectionReader != null) {
          currentTenantSectionId = sectionId;
          currentScanner = sectionReader.getScanner(conf, cacheBlocks, pread, isCompaction);
          
          // If we have a partial row key, try to seek to it
          if (partialRowKey != null && partialRowKey.length > 0) {
            // Create a KeyValue with the partial row key to seek to
            KeyValue seekKey = new KeyValue(partialRowKey, HConstants.EMPTY_BYTE_ARRAY, 
                                           HConstants.EMPTY_BYTE_ARRAY);
            
            // Try to seek to or just before the partial key position
            int seekResult = currentScanner.seekTo(seekKey);
            if (seekResult >= 0) {
              // Found an exact or after match
              LOG.debug("Successfully seeked to position for partial key in section {}", 
                       org.apache.hadoop.hbase.util.Bytes.toStringBinary(sectionId));
              seeked = true;
              return true;
            } else if (currentScanner.seekTo()) {
              // If direct seek fails, try from the beginning of the section
              LOG.debug("Partial key seek failed, but successfully seeked to first position in section {}", 
                       org.apache.hadoop.hbase.util.Bytes.toStringBinary(sectionId));
              seeked = true;
              return true;
            } else {
              LOG.debug("Failed to seek to any position in section {}", 
                       org.apache.hadoop.hbase.util.Bytes.toStringBinary(sectionId));
            }
          } else {
            // For empty partial key, just seek to the start of the section
            if (currentScanner.seekTo()) {
              seeked = true;
              return true;
            }
          }
        } else {
          LOG.debug("Could not get section reader for section {}", 
                   org.apache.hadoop.hbase.util.Bytes.toStringBinary(sectionId));
        }
      }
      
      // No more sections to try
      return false;
    }
    
    @Override
    public boolean next() throws IOException {
      if (!seeked) {
        return seekTo();
      }
      
      // Try to advance within the current section
      boolean hasNext = currentScanner.next();
      if (hasNext) {
        return true;
      }
      
      // Try to move to the next section
      return seekToNextCandidateSection(currentSectionIndex + 1);
    }
  }

  /**
   * For multi-tenant HFiles, get the first key from the first available section.
   * This overrides the HFileReaderImpl implementation that requires dataBlockIndexReader.
   */
  @Override
  public Optional<ExtendedCell> getFirstKey() {
    try {
      // Get all section IDs in sorted order
      byte[][] sectionIds = getAllTenantSectionIds();
      if (sectionIds.length == 0) {
        return Optional.empty();
      }
      
      // Get the first section and try to read its first key
      for (byte[] sectionId : sectionIds) {
        try {
          SectionReader sectionReader = getSectionReader(sectionId);
          HFileReaderImpl reader = sectionReader.getReader();
          Optional<ExtendedCell> firstKey = reader.getFirstKey();
          if (firstKey.isPresent()) {
            return firstKey;
          }
        } catch (IOException e) {
          LOG.warn("Failed to get first key from section {}, trying next section", 
                   Bytes.toString(sectionId), e);
          // Continue to next section
        }
      }
      
      return Optional.empty();
    } catch (Exception e) {
      LOG.error("Failed to get first key from multi-tenant HFile", e);
      return Optional.empty();
    }
  }

  /**
   * For multi-tenant HFiles, get the last key from the last available section.
   * This overrides the HFileReaderImpl implementation that requires dataBlockIndexReader.
   */
  @Override
  public Optional<ExtendedCell> getLastKey() {
    try {
      // Get all section IDs in sorted order
      byte[][] sectionIds = getAllTenantSectionIds();
      if (sectionIds.length == 0) {
        return Optional.empty();
      }
      
      // Get the last section and try to read its last key
      // Iterate backwards to find the last available key
      for (int i = sectionIds.length - 1; i >= 0; i--) {
        byte[] sectionId = sectionIds[i];
        try {
          SectionReader sectionReader = getSectionReader(sectionId);
          HFileReaderImpl reader = sectionReader.getReader();
          Optional<ExtendedCell> lastKey = reader.getLastKey();
          if (lastKey.isPresent()) {
            return lastKey;
          }
        } catch (IOException e) {
          LOG.warn("Failed to get last key from section {}, trying previous section", 
                   Bytes.toString(sectionId), e);
          // Continue to previous section
        }
      }
      
      return Optional.empty();
    } catch (Exception e) {
      LOG.error("Failed to get last key from multi-tenant HFile", e);
      return Optional.empty();
    }
  }

  /**
   * Get cache statistics for monitoring
   * @return A map of cache statistics
   */
  public Map<String, Long> getCacheStats() {
    Map<String, Long> stats = new HashMap<>();
    stats.put("cacheSize", sectionReaderCache.size());
    stats.put("cacheHitCount", sectionReaderCache.stats().hitCount());
    stats.put("cacheMissCount", sectionReaderCache.stats().missCount());
    stats.put("cacheLoadCount", sectionReaderCache.stats().loadCount());
    stats.put("cacheEvictionCount", sectionReaderCache.stats().evictionCount());
    stats.put("totalSections", (long) sectionLocations.size());
    return stats;
  }
} 