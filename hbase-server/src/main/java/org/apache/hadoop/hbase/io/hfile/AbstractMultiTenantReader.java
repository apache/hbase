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
import java.util.ArrayList;
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
 * 
 * The multi-tenant reader acts as a router that:
 * <ol>
 *   <li>Extracts tenant information from cell keys</li>
 *   <li>Locates the appropriate section in the HFile for that tenant</li>
 *   <li>Delegates reading operations to a standard v3 reader for that section</li>
 * </ol>
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
    LOG.debug("getSectionReader called for tenant section ID: {}, cache key: {}", 
              Bytes.toStringBinary(tenantSectionId), key);
    
    // Lookup the section metadata
    SectionMetadata metadata = getSectionMetadata(tenantSectionId);
    if (metadata == null) {
      LOG.debug("No section found for tenant section ID: {}", 
                Bytes.toStringBinary(tenantSectionId));
      return null;
    }
    
    LOG.debug("Found section metadata for tenant section ID: {}, offset: {}, size: {}", 
              Bytes.toStringBinary(tenantSectionId), metadata.getOffset(), metadata.getSize());
    
    try {
      // Use cache's get method with loader for atomic creation
      SectionReader reader = sectionReaderCache.get(key, () -> {
        LOG.debug("Cache miss for tenant section ID: {}, creating new section reader", 
                  Bytes.toStringBinary(tenantSectionId));
        SectionReader newReader = createSectionReader(tenantSectionId, metadata);
        LOG.debug("Created section reader for tenant section ID: {}", 
                  Bytes.toStringBinary(tenantSectionId));
        return newReader;
      });
      
      LOG.debug("Returning section reader for tenant section ID: {}, reader: {}", 
                Bytes.toStringBinary(tenantSectionId), reader);
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
      LOG.debug("Switching section reader from {} to {}, section ID: {}", 
                currentSectionReader, newReader, Bytes.toStringBinary(sectionId));
      
      // Release previous reader
      if (currentSectionReader != null) {
        try {
          // Note: We don't close the reader here as it might be cached and reused
          // The cache eviction will handle the actual closing
          LOG.debug("Releasing previous section reader: {}, tenant section ID: {}", 
                    currentSectionReader, Bytes.toStringBinary(currentTenantSectionId));
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
        LOG.debug("Switched to new section reader: {}, scanner: {}, tenant section ID: {}", 
                  currentSectionReader, currentScanner, Bytes.toStringBinary(currentTenantSectionId));
      } else {
        currentTenantSectionId = null;
        currentScanner = null;
        LOG.debug("Cleared current section reader and scanner");
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
        LOG.debug("seekTo called with null or empty key, falling back to seekTo()");
        if (seekTo()) {
          return 0; // Successfully seeked to first position
        } else {
          return -1; // No data found
        }
      }
      
      // Extract tenant section ID
      byte[] tenantSectionId = tenantExtractor.extractTenantSectionId(key);
      
      LOG.debug("seekTo called with key: {}, extracted tenant section ID: {}", 
                key, Bytes.toStringBinary(tenantSectionId));
      
      // Get the scanner for this tenant section
      SectionReader sectionReader = getSectionReader(tenantSectionId);
      if (sectionReader == null) {
        LOG.warn("No section reader found for tenant section ID: {}", 
                 Bytes.toStringBinary(tenantSectionId));
        seeked = false;
        return -1;
      }
      
      LOG.debug("Got section reader for tenant section ID: {}, reader instance: {}", 
                Bytes.toStringBinary(tenantSectionId), sectionReader);
      
      // Use the section scanner
      switchToSectionReader(sectionReader, tenantSectionId);
      int result = currentScanner.seekTo(key);
      
      LOG.debug("seekTo result: {}, current scanner: {}", result, currentScanner);
      
      if (result != -1) {
        seeked = true;
        // Log what cell we actually found
        ExtendedCell foundCell = currentScanner.getCell();
        if (foundCell != null) {
          LOG.debug("Found cell after seekTo: row={}, value={}", 
                    Bytes.toStringBinary(foundCell.getRowArray(), foundCell.getRowOffset(), 
                                        foundCell.getRowLength()),
                    Bytes.toStringBinary(foundCell.getValueArray(), foundCell.getValueOffset(), 
                                        foundCell.getValueLength()));
        }
      } else {
        seeked = false;
        LOG.debug("seekTo failed for key in tenant section {}", 
                  Bytes.toStringBinary(tenantSectionId));
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
          LOG.debug("Prefetched section: {}", Bytes.toStringBinary(nextSectionId));
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
   * 
   * @param metadata The section metadata
   * @param readerType The type of reader (PREAD or STREAM)
   * @return A reader context for the section
   * @throws IOException If an error occurs building the context
   */
  protected ReaderContext buildSectionContext(SectionMetadata metadata, 
                                            ReaderContext.ReaderType readerType) 
      throws IOException {
    // Create a special wrapper with offset translation capabilities
    FSDataInputStreamWrapper parentWrapper = context.getInputStreamWrapper();
    LOG.debug("Creating MultiTenantFSDataInputStreamWrapper with offset translation " +
              "from parent at offset {}", metadata.getOffset());
    
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
      LOG.debug("CHECKSUM_TYPE_INDEX position should be translated from relative pos 24 " +
                "to absolute pos {}", metadata.getOffset() + 24);
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
   * Get all tenant section IDs present in the file.
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
   * Get cache statistics for monitoring.
   * 
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
      // Since LinkedHashMap maintains insertion order, get the last section
      ImmutableBytesWritable lastSectionKey = null;
      for (ImmutableBytesWritable sectionKey : sectionLocations.keySet()) {
        lastSectionKey = sectionKey;
      }
      
      if (lastSectionKey != null) {
        try {
          byte[] sectionId = lastSectionKey.get();
          SectionReader sectionReader = getSectionReader(sectionId);
          HFileReaderImpl reader = sectionReader.getReader();
          Optional<ExtendedCell> lastKey = reader.getLastKey();
          if (lastKey.isPresent()) {
            return lastKey;
          }
        } catch (IOException e) {
          LOG.warn("Failed to get last key from last section, trying all sections backwards", e);
          
          // Fallback: try all sections in reverse order
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
            } catch (IOException ex) {
              LOG.warn("Failed to get last key from section {}, trying previous section", 
                       Bytes.toString(sectionId), ex);
              // Continue to previous section
            }
          }
        }
      }
      
      return Optional.empty();
    } catch (Exception e) {
      LOG.error("Failed to get last key from multi-tenant HFile", e);
      return Optional.empty();
    }
  }
} 