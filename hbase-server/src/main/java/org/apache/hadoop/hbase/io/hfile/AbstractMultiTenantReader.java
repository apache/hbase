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

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.MultiTenantFSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.common.cache.Cache;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.common.cache.RemovalListener;
import org.apache.hbase.thirdparty.com.google.common.cache.RemovalNotification;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for multi-tenant HFile readers. This class handles the common
 * functionality for both pread and stream access modes, delegating specific reader
 * creation to subclasses.
 * <p>
 * The multi-tenant reader acts as a router that:
 * <ol>
 *   <li>Extracts tenant information from cell keys</li>
 *   <li>Locates the appropriate section in the HFile for that tenant</li>
 *   <li>Delegates reading operations to a standard v3 reader for that section</li>
 * </ol>
 * <p>
 * Key features:
 * <ul>
 * <li>Section-based caching with bounded size and LRU eviction</li>
 * <li>Multi-level tenant index support for efficient section lookup</li>
 * <li>Prefetching for sequential access optimization</li>
 * <li>Table property caching to avoid repeated Admin API calls</li>
 * <li>Transparent delegation to HFile v3 readers for each section</li>
 * </ul>
 */
@InterfaceAudience.Private
public abstract class AbstractMultiTenantReader extends HFileReaderImpl {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMultiTenantReader.class);
  
  /** Static cache for table properties to avoid repeated loading */
  private static final Cache<TableName, Map<String, String>> TABLE_PROPERTIES_CACHE = 
      CacheBuilder.newBuilder()
          .maximumSize(100)
          .expireAfterWrite(5, TimeUnit.MINUTES)
          .build();
  
  /** Tenant extractor for identifying tenant information from cells */
  protected final TenantExtractor tenantExtractor;
  /** Section index reader for locating tenant sections */
  protected final SectionIndexManager.Reader sectionIndexReader;
  
  /** Configuration key for section reader cache size */
  private static final String SECTION_READER_CACHE_SIZE = 
      "hbase.multi.tenant.reader.cache.size";
  /** Default size for section reader cache */
  private static final int DEFAULT_SECTION_READER_CACHE_SIZE = 100;
  
  /** Configuration key for section prefetch enablement */
  private static final String SECTION_PREFETCH_ENABLED = 
      "hbase.multi.tenant.reader.prefetch.enabled";
  /** Default prefetch enabled flag */
  private static final boolean DEFAULT_SECTION_PREFETCH_ENABLED = true;
  
  /** Cache for section readers with bounded size and eviction */
  protected final Cache<ImmutableBytesWritable, SectionReader> sectionReaderCache;
  
  /** Private map to store section metadata */
  private final Map<ImmutableBytesWritable, SectionMetadata> sectionLocations = 
      new LinkedHashMap<ImmutableBytesWritable, SectionMetadata>();
  
  /** List for section navigation */
  private List<ImmutableBytesWritable> sectionIds;
  
  /** Number of levels in the tenant index structure */
  private int tenantIndexLevels = 1;
  /** Maximum chunk size used in the tenant index */
  private int tenantIndexMaxChunkSize = SectionIndexManager.DEFAULT_MAX_CHUNK_SIZE;
  /** Whether prefetch is enabled for sequential access */
  private final boolean prefetchEnabled;
  
  /**
   * Constructor for multi-tenant reader.
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
          public void onRemoval(RemovalNotification<ImmutableBytesWritable, SectionReader> 
                               notification) {
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
    this.prefetchEnabled = conf.getBoolean(SECTION_PREFETCH_ENABLED, 
                                          DEFAULT_SECTION_PREFETCH_ENABLED);
    
    LOG.info("Initialized multi-tenant reader for {}", context.getFilePath());
  }
  
  /**
   * Initialize the section index from the file.
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
      LOG.debug("Seeking to load-on-open section at offset {}", 
                trailer.getLoadOnOpenDataOffset());
      
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
   * Load information about the tenant index structure from file info.
   * <p>
   * Extracts tenant index levels and chunk size configuration from the HFile
   * metadata to optimize section lookup performance.
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
   * Get the number of levels in the tenant index.
   * 
   * @return The number of levels (1 for single-level, 2+ for multi-level)
   */
  public int getTenantIndexLevels() {
    return tenantIndexLevels;
  }
  
  /**
   * Get the maximum chunk size used in the tenant index.
   * 
   * @return The maximum entries per index block
   */
  public int getTenantIndexMaxChunkSize() {
    return tenantIndexMaxChunkSize;
  }
  
  /**
   * Initialize our section location map from the index reader.
   * <p>
   * Populates the internal section metadata map and creates the section ID list
   * for efficient navigation during scanning operations.
   */
  private void initSectionLocations() {
    for (SectionIndexManager.SectionIndexEntry entry : sectionIndexReader.getSections()) {
      sectionLocations.put(
          new ImmutableBytesWritable(entry.getTenantPrefix()),
          new SectionMetadata(entry.getOffset(), entry.getSectionSize()));
    }
    
    // Create list for section navigation
    sectionIds = new ArrayList<>(sectionLocations.keySet());
    LOG.debug("Initialized {} section IDs for navigation", sectionIds.size());
  }
  
  /**
   * Get the number of sections.
   *
   * @return The number of sections in this file
   */
  private int getSectionCount() {
    return sectionLocations.size();
  }
  
  /**
   * Get the total number of tenant sections in this file.
   * 
   * @return The number of sections
   */
  public int getTotalSectionCount() {
    return sectionLocations.size();
  }
  
  /**
   * Get table properties from the file context if available.
   * <p>
   * Uses a bounded cache to avoid repeated Admin API calls for the same table.
   * Properties are used for tenant configuration and optimization settings.
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
   * Metadata for a tenant section within the HFile.
   */
  protected static class SectionMetadata {
    /** The offset where the section starts */
    final long offset;
    /** The size of the section in bytes */
    final int size;
    
    /**
     * Constructor for SectionMetadata.
     *
     * @param offset the file offset where the section starts
     * @param size the size of the section in bytes
     */
    SectionMetadata(long offset, int size) {
      this.offset = offset;
      this.size = size;
    }
    
    /**
     * Get the offset where the section starts.
     *
     * @return the section offset
     */
    long getOffset() {
      return offset;
    }
    
    /**
     * Get the size of the section.
     *
     * @return the section size in bytes
     */
    int getSize() {
      return size;
    }
  }
  
  /**
   * Get metadata for a tenant section.
   * 
   * @param tenantSectionId The tenant section ID to look up
   * @return Section metadata or null if not found
   * @throws IOException If an error occurs during lookup
   */
  protected SectionMetadata getSectionMetadata(byte[] tenantSectionId) throws IOException {
    return sectionLocations.get(new ImmutableBytesWritable(tenantSectionId));
  }
  
  /**
   * Get or create a reader for a tenant section.
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
      return null;
    }
    
    try {
      // Use cache's get method with loader for atomic creation
      SectionReader reader = sectionReaderCache.get(key, () -> {
        return createSectionReader(tenantSectionId, metadata);
      });
      
      return reader;
    } catch (Exception e) {
      if (e instanceof IOException) {
        throw (IOException) e;
      }
      throw new IOException("Failed to get section reader", e);
    }
  }
  
  /**
   * Create appropriate section reader based on type (to be implemented by subclasses).
   * 
   * @param tenantSectionId The tenant section ID
   * @param metadata The section metadata
   * @return A section reader
   * @throws IOException If an error occurs creating the reader
   */
  protected abstract SectionReader createSectionReader(
      byte[] tenantSectionId, SectionMetadata metadata) throws IOException;
  
  /**
   * Get a scanner for this file.
   *
   * @param conf Configuration to use
   * @param cacheBlocks Whether to cache blocks
   * @param pread Whether to use positional read
   * @param isCompaction Whether this is for a compaction
   * @return A scanner for this file
   */
  @Override
  public HFileScanner getScanner(Configuration conf, boolean cacheBlocks, 
      boolean pread, boolean isCompaction) {
    return new MultiTenantScanner(conf, cacheBlocks, pread, isCompaction);
  }
  
  /**
   * Simpler scanner method that delegates to the full method.
   *
   * @param conf Configuration to use
   * @param cacheBlocks Whether to cache blocks
   * @param pread Whether to use positional read
   * @return A scanner for this file
   */
  @Override
  public HFileScanner getScanner(Configuration conf, boolean cacheBlocks, boolean pread) {
    return getScanner(conf, cacheBlocks, pread, false);
  }
  
  /**
   * Abstract base class for section readers.
   * <p>
   * Each section reader manages access to a specific tenant section within the HFile,
   * providing transparent delegation to standard HFile v3 readers with proper offset
   * translation and resource management.
   */
  protected abstract class SectionReader {
    /** The tenant section ID for this reader */
    protected final byte[] tenantSectionId;
    /** The section metadata */
    protected final SectionMetadata metadata;
    /** The underlying HFile reader */
    protected HFileReaderImpl reader;
    /** Whether this reader has been initialized */
    protected boolean initialized = false;
    /** The base offset for this section */
    protected long sectionBaseOffset;
    
    /**
     * Constructor for SectionReader.
     *
     * @param tenantSectionId The tenant section ID
     * @param metadata The section metadata
     */
    public SectionReader(byte[] tenantSectionId, SectionMetadata metadata) {
      this.tenantSectionId = tenantSectionId.clone(); // Make defensive copy
      this.metadata = metadata;
      this.sectionBaseOffset = metadata.getOffset();
    }
    
    /**
     * Get or initialize the underlying reader.
     * 
     * @return The underlying HFile reader
     * @throws IOException If an error occurs initializing the reader
     */
    public abstract HFileReaderImpl getReader() throws IOException;
    
    /**
     * Get a scanner for this section.
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
     * Close the section reader.
     * 
     * @throws IOException If an error occurs closing the reader
     */
    public void close() throws IOException {
      close(false);
    }
    
    /**
     * Close the section reader.
     * 
     * @param evictOnClose whether to evict blocks on close
     * @throws IOException If an error occurs closing the reader
     */
    public abstract void close(boolean evictOnClose) throws IOException;
  }
  
  /**
   * Scanner implementation for multi-tenant HFiles.
   * <p>
   * This scanner provides transparent access across multiple tenant sections by:
   * <ul>
   * <li>Extracting tenant information from seek keys</li>
   * <li>Routing operations to the appropriate section reader</li>
   * <li>Managing section transitions during sequential scans</li>
   * <li>Optimizing performance through section prefetching</li>
   * </ul>
   */
  protected class MultiTenantScanner implements HFileScanner {
    /** Configuration to use */
    protected final Configuration conf;
    /** Whether to cache blocks */
    protected final boolean cacheBlocks;
    /** Whether to use positional read */
    protected final boolean pread;
    /** Whether this is for a compaction */
    protected final boolean isCompaction;
    
    /** Current tenant section ID */
    protected byte[] currentTenantSectionId;
    /** Current scanner instance */
    protected HFileScanner currentScanner;
    /** Current section reader */
    protected SectionReader currentSectionReader;
    /** Whether we have successfully seeked */
    protected boolean seeked = false;
    
    /**
     * Constructor for MultiTenantScanner.
     *
     * @param conf Configuration to use
     * @param cacheBlocks Whether to cache blocks
     * @param pread Whether to use positional read
     * @param isCompaction Whether this is for a compaction
     */
    public MultiTenantScanner(Configuration conf, boolean cacheBlocks, 
        boolean pread, boolean isCompaction) {
      this.conf = conf;
      this.cacheBlocks = cacheBlocks;
      this.pread = pread;
      this.isCompaction = isCompaction;
    }
    
    /**
     * Switch to a new section reader, properly managing reference counts.
     *
     * @param newReader The new section reader to switch to
     * @param sectionId The section ID for the new reader
     * @throws IOException If an error occurs during the switch
     */
    private void switchToSectionReader(SectionReader newReader, byte[] sectionId) 
        throws IOException {
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
      // Get the first section from the section index
      if (!sectionIds.isEmpty()) {
        // Get the first section ID from the list
        byte[] firstSectionId = sectionIds.get(0).get();
        
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
      // Handle empty or null keys by falling back to seekTo() without parameters
      if (key == null || key.getRowLength() == 0) {
        if (seekTo()) {
          return 0; // Successfully seeked to first position
        } else {
          return -1; // No data found
        }
      }
      
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
        // Keep the original result from the section scanner (0 for exact match, 1 for positioned after)
      } else {
        // If seekTo returned -1 (key is before first key in section), 
        // we need to check if this key actually belongs to this tenant section
        // by seeking to the first key and comparing tenant prefixes
        if (currentScanner.seekTo()) {
          ExtendedCell firstCell = currentScanner.getCell();
          if (firstCell != null) {
            // Extract tenant section ID from both the search key and the first cell
            byte[] searchTenantId = tenantExtractor.extractTenantSectionId(key);
            byte[] firstCellTenantId = tenantExtractor.extractTenantSectionId(firstCell);
            
            if (Bytes.equals(searchTenantId, firstCellTenantId)) {
              // The search key belongs to the same tenant as the first cell in this section
              // Now we need to compare the actual keys to determine the correct result
              seeked = true;
              int comparison = currentSectionReader.getReader().getComparator().compareRows(firstCell, key);
              
              if (comparison == 0) {
                result = 0; // Exact row match
              } else if (comparison > 0) {
                // Check if this is a scan operation with a prefix search
                // If the search key is a prefix of the first cell's row, treat it as a match
                byte[] firstCellRow = Arrays.copyOfRange(firstCell.getRowArray(), 
                                                        firstCell.getRowOffset(), 
                                                        firstCell.getRowOffset() + firstCell.getRowLength());
                byte[] searchKeyRow = Arrays.copyOfRange(key.getRowArray(), 
                                                        key.getRowOffset(), 
                                                        key.getRowOffset() + key.getRowLength());
                
                if (Bytes.startsWith(firstCellRow, searchKeyRow)) {
                  result = 0; // Treat as exact match for prefix scans
                } else {
                  result = 1; // Found key is after the search key
                }
              } else {
                // This shouldn't happen since we're at the first key in the section
                result = 1; // Default to "after"
              }
            } else {
              // The search key belongs to a different tenant, return -1
              seeked = false;
            }
          } else {
            seeked = false;
          }
        } else {
          seeked = false;
        }
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
        
        // Move to the next tenant section
        SectionReader nextSectionReader = getSectionReader(nextTenantSectionId);
        if (nextSectionReader == null) {
          seeked = false;
          return false;
        }
        
        // Prefetch the section after next if enabled
        if (prefetchEnabled) {
          prefetchNextSection(nextTenantSectionId);
        }
        
        switchToSectionReader(nextSectionReader, nextTenantSectionId);
        boolean result = currentScanner.seekTo();
        seeked = result;
        return result;
      }
      
      return true;
    }
    
    /**
     * Prefetch the next section after the given one for sequential access optimization.
     *
     * @param currentSectionId The current section ID
     */
    private void prefetchNextSection(byte[] currentSectionId) {
      try {
        byte[] nextSectionId = findNextTenantSectionId(currentSectionId);
        if (nextSectionId != null) {
          // Trigger async load by just getting the reader (cache will hold it)
          getSectionReader(nextSectionId);
        }
      } catch (Exception e) {
        // Prefetch is best-effort, don't fail the operation
        LOG.debug("Failed to prefetch next section", e);
      }
    }
    
    /**
     * Find the next tenant section ID after the current one.
     *
     * @param currentSectionId The current section ID
     * @return The next section ID, or null if none found
     */
    private byte[] findNextTenantSectionId(byte[] currentSectionId) {
      // Linear search to find current position and return next
      for (int i = 0; i < sectionIds.size(); i++) {
        if (Bytes.equals(sectionIds.get(i).get(), currentSectionId)) {
          // Found current section, return next if it exists
          if (i < sectionIds.size() - 1) {
            return sectionIds.get(i + 1).get();
          }
          break;
        }
      }
      
      return null;
    }
    
    /**
     * Assert that we have successfully seeked.
     *
     * @throws NotSeekedException if not seeked
     */
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
   * Close all section readers and release resources.
   *
   * @throws IOException If an error occurs during close
   */
  @Override
  public void close() throws IOException {
    close(false);
  }

  /**
   * Close all section readers and underlying resources, with optional block eviction.
   *
   * @param evictOnClose Whether to evict blocks on close
   * @throws IOException If an error occurs during close
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
   * Get HFile version.
   *
   * @return The major version number
   */
  @Override
  public int getMajorVersion() {
    return HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT;
  }

  /**
   * Build a section context with the appropriate offset translation wrapper.
   * <p>
   * Creates a specialized reader context for a tenant section that handles:
   * <ul>
   * <li>Offset translation from section-relative to file-absolute positions</li>
   * <li>Proper trailer positioning for HFile v3 section format</li>
   * <li>Block boundary validation and alignment</li>
   * <li>File size calculation for section boundaries</li>
   * </ul>
   * 
   * @param metadata The section metadata containing offset and size
   * @param readerType The type of reader (PREAD or STREAM)
   * @return A reader context for the section, or null if section is invalid
   * @throws IOException If an error occurs building the context
   */
  protected ReaderContext buildSectionContext(SectionMetadata metadata, 
                                            ReaderContext.ReaderType readerType) 
      throws IOException {
    // Create a special wrapper with offset translation capabilities
    FSDataInputStreamWrapper parentWrapper = context.getInputStreamWrapper();
    MultiTenantFSDataInputStreamWrapper sectionWrapper = 
        new MultiTenantFSDataInputStreamWrapper(parentWrapper, metadata.getOffset());
    
    // Calculate section size and validate minimum requirements
    int sectionSize = metadata.getSize();
    int trailerSize = FixedFileTrailer.getTrailerSize(3); // HFile v3 sections use v3 format
    
    if (sectionSize < trailerSize) {
      LOG.warn("Section size {} for offset {} is smaller than minimum trailer size {}",
               sectionSize, metadata.getOffset(), trailerSize);
      return null;
    }
    
    // Build the reader context with proper file size calculation
    ReaderContext sectionContext = ReaderContextBuilder.newBuilder(context)
        .withInputStreamWrapper(sectionWrapper)
        .withFilePath(context.getFilePath())
        .withReaderType(readerType)
        .withFileSystem(context.getFileSystem())
        .withFileSize(sectionSize) // Use section size; wrapper handles offset translation
        .build();
    
    LOG.debug("Created section reader context for offset {}, size {}", 
              metadata.getOffset(), sectionSize);
    return sectionContext;
  }

  /**
   * Get all tenant section IDs present in the file.
   * <p>
   * Returns a defensive copy of all section IDs for external iteration
   * without exposing internal data structures.
   * 
   * @return An array of all tenant section IDs
   */
  public byte[][] getAllTenantSectionIds() {
    byte[][] allIds = new byte[sectionLocations.size()][];
    int i = 0;
    for (ImmutableBytesWritable key : sectionLocations.keySet()) {
      allIds[i++] = key.copyBytes();
    }
    return allIds;
  }

  /**
   * Get cache statistics for monitoring and performance analysis.
   * <p>
   * Provides comprehensive metrics about section reader cache performance
   * including hit rates, eviction counts, and current cache utilization.
   * 
   * @return A map of cache statistics with metric names as keys
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

  /**
   * For multi-tenant HFiles, get the first key from the first available section.
   * This overrides the HFileReaderImpl implementation that requires dataBlockIndexReader.
   *
   * @return The first key if available
   */
  @Override
  public Optional<ExtendedCell> getFirstKey() {
    try {
      // Get the first section and try to read its first key
      for (ImmutableBytesWritable sectionKey : sectionLocations.keySet()) {
        byte[] sectionId = sectionKey.get();
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
   *
   * @return The last key if available
   */
  @Override
  public Optional<ExtendedCell> getLastKey() {
    try {
      // Get the last section and try to read its last key
      // Since LinkedHashMap maintains insertion order, iterate in reverse to get the last section first
      List<ImmutableBytesWritable> sectionKeys = new ArrayList<>(sectionLocations.keySet());
      for (int i = sectionKeys.size() - 1; i >= 0; i--) {
        byte[] sectionId = sectionKeys.get(i).get();
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
   * For HFile v4 multi-tenant files, meta blocks don't exist at the file level.
   * They exist within individual sections. This method is not supported.
   * 
   * @param metaBlockName the name of the meta block to retrieve
   * @param cacheBlock whether to cache the block
   * @return always null for multi-tenant HFiles
   * @throws IOException if an error occurs
   */
  @Override
  public HFileBlock getMetaBlock(String metaBlockName, boolean cacheBlock) throws IOException {
    // HFile v4 multi-tenant files don't have file-level meta blocks
    // Meta blocks exist within individual sections
    LOG.debug("Meta blocks not supported at file level for HFile v4 multi-tenant files: {}", 
              metaBlockName);
    return null;
  }

  /**
   * For HFile v4 multi-tenant files, bloom filter metadata doesn't exist at the file level.
   * It exists within individual sections. This method is not supported.
   * 
   * @return always null for multi-tenant HFiles
   * @throws IOException if an error occurs
   */
  @Override
  public DataInput getGeneralBloomFilterMetadata() throws IOException {
    // HFile v4 multi-tenant files don't have file-level bloom filters
    // Bloom filters exist within individual sections
    LOG.debug("General bloom filter metadata not supported at file level for HFile v4 multi-tenant files");
    return null;
  }

  /**
   * For HFile v4 multi-tenant files, delete bloom filter metadata doesn't exist at the file level.
   * It exists within individual sections. This method is not supported.
   * 
   * @return always null for multi-tenant HFiles
   * @throws IOException if an error occurs
   */
  @Override
  public DataInput getDeleteBloomFilterMetadata() throws IOException {
    // HFile v4 multi-tenant files don't have file-level delete bloom filters
    // Delete bloom filters exist within individual sections
    LOG.debug("Delete bloom filter metadata not supported at file level for HFile v4 multi-tenant files");
    return null;
  }

  /**
   * For HFile v4 multi-tenant files, index size is just the section index size.
   * 
   * @return the heap size of the section index
   */
  @Override
  public long indexSize() {
    if (sectionIndexReader != null) {
      int numSections = sectionIndexReader.getNumSections();
      // Estimate: each section entry is approximately 64 bytes (prefix + offset + size)
      return numSections * 64L;
    }
    return 0;
  }

  /**
   * Override mid-key calculation to find the middle key across all sections.
   * For HFile v4 multi-tenant files, midkey calculation is complex and not meaningful
   * at the file level since data is distributed across sections with different densities.
   * This method is not supported for multi-tenant HFiles.
   * 
   * @return empty optional for multi-tenant HFiles
   * @throws IOException if an error occurs
   */
  @Override
  public Optional<ExtendedCell> midKey() throws IOException {
    // HFile v4 multi-tenant files don't have a meaningful file-level midkey
    // since data distribution across sections can be highly variable
    LOG.debug("Midkey calculation not supported for HFile v4 multi-tenant files");
    return Optional.empty();
  }

  /**
   * Override block reading to support tenant-aware block access.
   * Routes block reads to the appropriate section based on offset.
   * 
   * @param dataBlockOffset the offset of the block to read
   * @param onDiskBlockSize the on-disk size of the block
   * @param cacheBlock whether to cache the block
   * @param pread whether to use positional read
   * @param isCompaction whether this is for a compaction
   * @param updateCacheMetrics whether to update cache metrics
   * @param expectedBlockType the expected block type
   * @param expectedDataBlockEncoding the expected data block encoding
   * @return the read block
   * @throws IOException if an error occurs reading the block
   */
  @Override
  public HFileBlock readBlock(long dataBlockOffset, long onDiskBlockSize, boolean cacheBlock,
      boolean pread, boolean isCompaction, boolean updateCacheMetrics,
      BlockType expectedBlockType, DataBlockEncoding expectedDataBlockEncoding) throws IOException {
    
    // Find the section that contains this offset
    SectionReader targetSectionReader = findSectionForOffset(dataBlockOffset);
    if (targetSectionReader == null) {
      throw new IOException("No section found for offset: " + dataBlockOffset + 
                           ", path=" + getPath());
    }
    
    try {
      HFileReaderImpl sectionReader = targetSectionReader.getReader();
      
      // Convert absolute offset to section-relative offset
      long sectionRelativeOffset = dataBlockOffset - targetSectionReader.sectionBaseOffset;
      
      return sectionReader.readBlock(sectionRelativeOffset, onDiskBlockSize, cacheBlock, 
                                   pread, isCompaction, updateCacheMetrics, 
                                   expectedBlockType, expectedDataBlockEncoding);
    } catch (IOException e) {
      LOG.error("Failed to read block at offset {} from section", dataBlockOffset, e);
      throw e;
    }
  }

  /**
   * Override block reading with cache-only flag.
   * 
   * @param dataBlockOffset the offset of the block to read
   * @param onDiskBlockSize the on-disk size of the block  
   * @param cacheBlock whether to cache the block
   * @param pread whether to use positional read
   * @param isCompaction whether this is for a compaction
   * @param updateCacheMetrics whether to update cache metrics
   * @param expectedBlockType the expected block type
   * @param expectedDataBlockEncoding the expected data block encoding
   * @param cacheOnly whether to only read from cache
   * @return the read block
   * @throws IOException if an error occurs reading the block
   */
  @Override
  public HFileBlock readBlock(long dataBlockOffset, long onDiskBlockSize, boolean cacheBlock,
      boolean pread, boolean isCompaction, boolean updateCacheMetrics,
      BlockType expectedBlockType, DataBlockEncoding expectedDataBlockEncoding, 
      boolean cacheOnly) throws IOException {
    
    // Find the section that contains this offset
    SectionReader targetSectionReader = findSectionForOffset(dataBlockOffset);
    if (targetSectionReader == null) {
      throw new IOException("No section found for offset: " + dataBlockOffset + 
                           ", path=" + getPath());
    }
    
    try {
      HFileReaderImpl sectionReader = targetSectionReader.getReader();
      
      // Convert absolute offset to section-relative offset
      long sectionRelativeOffset = dataBlockOffset - targetSectionReader.sectionBaseOffset;
      
      return sectionReader.readBlock(sectionRelativeOffset, onDiskBlockSize, cacheBlock, 
                                   pread, isCompaction, updateCacheMetrics, 
                                   expectedBlockType, expectedDataBlockEncoding, cacheOnly);
    } catch (IOException e) {
      LOG.error("Failed to read block at offset {} from section (cache-only={})", 
                dataBlockOffset, cacheOnly, e);
      throw e;
    }
  }

  /**
   * Find the section reader that contains the given absolute file offset.
   * 
   * @param absoluteOffset the absolute offset in the file
   * @return the section reader containing this offset, or null if not found
   */
  private SectionReader findSectionForOffset(long absoluteOffset) {
    for (Map.Entry<ImmutableBytesWritable, SectionMetadata> entry : sectionLocations.entrySet()) {
      SectionMetadata metadata = entry.getValue();
      if (absoluteOffset >= metadata.getOffset() && 
          absoluteOffset < metadata.getOffset() + metadata.getSize()) {
        try {
          return getSectionReader(entry.getKey().get());
        } catch (IOException e) {
          LOG.warn("Failed to get section reader for offset {}", absoluteOffset, e);
          return null;
        }
      }
    }
    return null;
  }

  /**
   * For HFile v4 multi-tenant files, MVCC information is determined from file info only.
   * 
   * @return true if file info indicates MVCC information is present
   */
  @Override
  public boolean hasMVCCInfo() {
    // HFile v4 multi-tenant files determine MVCC info from file info only
    return fileInfo.shouldIncludeMemStoreTS() && fileInfo.isDecodeMemstoreTS();
  }

  /**
   * For HFile v4 multi-tenant files, entry count is determined from trailer only.
   * 
   * @return the entry count from the trailer
   */
  @Override
  public long getEntries() {
    // HFile v4 multi-tenant files get entry count from trailer only
    if (trailer != null) {
      return trailer.getEntryCount();
    }
    return 0;
  }

  /**
   * Override unbuffer stream to handle all section contexts.
   */
  @Override
  public void unbufferStream() {
    // Unbuffer the main context
    super.unbufferStream();
    
    // Unbuffer all cached section readers
    for (SectionReader sectionReader : sectionReaderCache.asMap().values()) {
      try {
        HFileReaderImpl reader = sectionReader.getReader();
        reader.unbufferStream();
      } catch (Exception e) {
        LOG.debug("Failed to unbuffer stream for section reader", e);
      }
    }
  }

  /**
   * For HFile v4 multi-tenant files, effective encoding in cache is ignored.
   * 
   * @param isCompaction whether this is for a compaction
   * @return always NONE for multi-tenant HFiles
   */
  @Override
  public DataBlockEncoding getEffectiveEncodingInCache(boolean isCompaction) {
    // HFile v4 multi-tenant files ignore effective encoding in cache
    LOG.debug("Effective encoding in cache ignored for HFile v4 multi-tenant files");
    return DataBlockEncoding.NONE;
  }

  /**
   * Get section-specific statistics for monitoring and debugging.
   * 
   * @return a map of section statistics
   */
  public Map<String, Object> getSectionStatistics() {
    Map<String, Object> stats = new HashMap<>();
    
    stats.put("totalSections", sectionLocations.size());
    stats.put("cachedSections", sectionReaderCache.size());
    stats.put("tenantIndexLevels", tenantIndexLevels);
    stats.put("tenantIndexMaxChunkSize", tenantIndexMaxChunkSize);
    stats.put("prefetchEnabled", prefetchEnabled);
    
    // Cache statistics
    stats.putAll(getCacheStats());
    
    // Section size distribution
    List<Integer> sectionSizes = new ArrayList<>();
    for (SectionMetadata metadata : sectionLocations.values()) {
      sectionSizes.add(metadata.getSize());
    }
    if (!sectionSizes.isEmpty()) {
      stats.put("avgSectionSize", sectionSizes.stream().mapToInt(Integer::intValue).average().orElse(0.0));
      stats.put("minSectionSize", sectionSizes.stream().mapToInt(Integer::intValue).min().orElse(0));
      stats.put("maxSectionSize", sectionSizes.stream().mapToInt(Integer::intValue).max().orElse(0));
    }
    
    return stats;
  }

  /**
   * Get metadata for a specific tenant section by section ID.
   * 
   * @param tenantSectionId The tenant section ID to look up
   * @return Detailed metadata about the section including cached status
   */
  public Map<String, Object> getSectionInfo(byte[] tenantSectionId) {
    Map<String, Object> info = new HashMap<>();
    
    ImmutableBytesWritable key = new ImmutableBytesWritable(tenantSectionId);
    SectionMetadata metadata = sectionLocations.get(key);
    
    if (metadata != null) {
      info.put("exists", true);
      info.put("offset", metadata.getOffset());
      info.put("size", metadata.getSize());
      info.put("cached", sectionReaderCache.asMap().containsKey(key));
      
      // Try to get additional info from cached reader
      SectionReader cachedReader = sectionReaderCache.getIfPresent(key);
      if (cachedReader != null) {
        try {
          HFileReaderImpl reader = cachedReader.getReader();
          info.put("entries", reader.getEntries());
          info.put("indexSize", reader.indexSize());
          info.put("hasMVCC", reader.hasMVCCInfo());
        } catch (Exception e) {
          LOG.debug("Failed to get additional info for section {}", 
                    Bytes.toStringBinary(tenantSectionId), e);
        }
      }
    } else {
      info.put("exists", false);
    }
    
    return info;
  }

  /**
   * For HFile v4 multi-tenant files, data block encoding is ignored at file level.
   * 
   * @return always NONE for multi-tenant HFiles
   */
  @Override
  public DataBlockEncoding getDataBlockEncoding() {
    // HFile v4 multi-tenant files ignore data block encoding at file level
    LOG.debug("Data block encoding ignored for HFile v4 multi-tenant files");
    return DataBlockEncoding.NONE;
  }

  /**
   * Check if prefetch is complete for this multi-tenant file.
   * 
   * @return true if prefetching is complete for all sections
   */
  @Override
  public boolean prefetchComplete() {
    // For multi-tenant files, prefetch is complete when section loading is done
    // This is a simpler check than per-section prefetch status
    return true; // Multi-tenant files handle prefetch at section level
  }

  /**
   * Check if prefetch has started for this multi-tenant file.
   * 
   * @return true if prefetching has started
   */
  @Override
  public boolean prefetchStarted() {
    // Multi-tenant files start prefetch immediately on open
    return prefetchEnabled;
  }

  /**
   * Get file length from the context.
   * 
   * @return the file length in bytes
   */
  @Override
  public long length() {
    return context.getFileSize();
  }

  /**
   * Check if file info is loaded (always true for multi-tenant readers).
   * 
   * @return true as file info is always loaded during construction
   */
  public boolean isFileInfoLoaded() {
    return true;
  }

  /**
   * Enhanced toString with multi-tenant specific information.
   * 
   * @return detailed string representation of this reader
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("MultiTenantReader{");
    sb.append("path=").append(getPath());
    sb.append(", majorVersion=").append(getMajorVersion());
    sb.append(", sections=").append(sectionLocations.size());
    sb.append(", cachedSections=").append(sectionReaderCache.size());
    sb.append(", tenantIndexLevels=").append(tenantIndexLevels);
    sb.append(", fileSize=").append(length());
    
    if (!sectionLocations.isEmpty()) {
      try {
        Optional<ExtendedCell> firstKey = getFirstKey();
        Optional<ExtendedCell> lastKey = getLastKey();
        if (firstKey.isPresent()) {
          sb.append(", firstKey=").append(firstKey.get().toString());
        }
        if (lastKey.isPresent()) {
          sb.append(", lastKey=").append(lastKey.get().toString());
        }
      } catch (Exception e) {
        LOG.debug("Failed to get keys for toString", e);
      }
    }
    
    sb.append("}");
    return sb.toString();
  }
} 