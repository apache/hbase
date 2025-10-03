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
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.cache.Cache;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.common.cache.RemovalNotification;

/**
 * Abstract base class for multi-tenant HFile readers. This class handles the common functionality
 * for both pread and stream access modes, delegating specific reader creation to subclasses.
 * <p>
 * The multi-tenant reader acts as a router that:
 * <ol>
 * <li>Extracts tenant information from cell keys</li>
 * <li>Locates the appropriate section in the HFile for that tenant</li>
 * <li>Delegates reading operations to a standard v3 reader for that section</li>
 * </ol>
 * <p>
 * Key features:
 * <ul>
 * <li>Multi-level tenant index support for efficient section lookup</li>
 * <li>Prefetching for sequential access optimization</li>
 * <li>Table property loading to support tenant configuration</li>
 * <li>Transparent delegation to HFile v3 readers for each section</li>
 * </ul>
 */
@InterfaceAudience.Private
public abstract class AbstractMultiTenantReader extends HFileReaderImpl {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMultiTenantReader.class);

  /** Static storage for table properties to avoid repeated loading */
  private static final Cache<TableName, Map<String, String>> TABLE_PROPERTIES_CACHE =
    CacheBuilder.newBuilder().maximumSize(100).expireAfterWrite(5, TimeUnit.MINUTES).build();

  /** Tenant extractor for identifying tenant information from cells */
  protected final TenantExtractor tenantExtractor;
  /** Section index reader for locating tenant sections */
  protected final SectionIndexManager.Reader sectionIndexReader;

  /** Configuration key for section prefetch enablement */
  private static final String SECTION_PREFETCH_ENABLED =
    "hbase.multi.tenant.reader.prefetch.enabled";
  /** Default prefetch enabled flag */
  private static final boolean DEFAULT_SECTION_PREFETCH_ENABLED = true;

  /** Configuration key controlling meta block lookup caching */
  private static final String META_BLOCK_CACHE_ENABLED =
    "hbase.multi.tenant.reader.meta.cache.enabled";
  /** Default flag enabling meta block cache */
  private static final boolean DEFAULT_META_BLOCK_CACHE_ENABLED = true;
  /** Configuration key controlling cache size */
  private static final String META_BLOCK_CACHE_MAX_SIZE =
    "hbase.multi.tenant.reader.meta.cache.max";
  /** Default maximum cache entries */
  private static final int DEFAULT_META_BLOCK_CACHE_MAX_SIZE = 256;

  /** Configuration key for the maximum number of cached section readers */
  private static final String SECTION_READER_CACHE_MAX_SIZE =
    "hbase.multi.tenant.reader.section.cache.max";
  /** Default maximum number of cached section readers */
  private static final int DEFAULT_SECTION_READER_CACHE_MAX_SIZE = 8;
  /** Configuration key for section reader cache idle eviction (ms) */
  private static final String SECTION_READER_CACHE_EXPIRE_MS =
    "hbase.multi.tenant.reader.section.cache.expire.ms";
  /** Default idle eviction (1 minute) */
  private static final long DEFAULT_SECTION_READER_CACHE_EXPIRE_MS = TimeUnit.MINUTES.toMillis(1);

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
  /** Cache of section readers keyed by tenant section ID */
  private final Cache<ImmutableBytesWritable, SectionReaderHolder> sectionReaderCache;
  /** Whether we cache meta block to section mappings */
  private final boolean metaBlockCacheEnabled;
  /** Cache for meta block name to section mapping */
  private final Cache<String, ImmutableBytesWritable> metaBlockSectionCache;

  /**
   * Constructor for multi-tenant reader.
   * @param context   Reader context info
   * @param fileInfo  HFile info
   * @param cacheConf Cache configuration values
   * @param conf      Configuration
   * @throws IOException If an error occurs during initialization
   */
  public AbstractMultiTenantReader(ReaderContext context, HFileInfo fileInfo, CacheConfig cacheConf,
    Configuration conf) throws IOException {
    super(context, fileInfo, cacheConf, conf);

    // Initialize section index reader
    this.sectionIndexReader = new SectionIndexManager.Reader();

    // Load tenant index metadata before accessing the section index so we know how to interpret it
    loadTenantIndexMetadata();

    // Initialize section index using dataBlockIndexReader from parent
    initializeSectionIndex();

    // Log tenant index structure information once sections are available
    logTenantIndexStructureInfo();

    // Create tenant extractor with consistent configuration
    this.tenantExtractor = TenantExtractorFactory.createFromReader(this);

    // Initialize prefetch configuration
    this.prefetchEnabled =
      conf.getBoolean(SECTION_PREFETCH_ENABLED, DEFAULT_SECTION_PREFETCH_ENABLED);

    // Initialize cache for section readers
    this.sectionReaderCache = createSectionReaderCache(conf);

    this.metaBlockCacheEnabled = conf.getBoolean(META_BLOCK_CACHE_ENABLED,
      DEFAULT_META_BLOCK_CACHE_ENABLED);
    if (metaBlockCacheEnabled) {
      int maxEntries =
        conf.getInt(META_BLOCK_CACHE_MAX_SIZE, DEFAULT_META_BLOCK_CACHE_MAX_SIZE);
      this.metaBlockSectionCache = CacheBuilder.newBuilder().maximumSize(maxEntries).build();
    } else {
      this.metaBlockSectionCache = null;
    }

    LOG.info("Initialized multi-tenant reader for {}", context.getFilePath());
  }

  /**
   * Initialize the section index from the file.
   * @throws IOException If an error occurs loading the section index
   */
  protected void initializeSectionIndex() throws IOException {
    // Get the trailer directly
    FixedFileTrailer trailer = fileInfo.getTrailer();

    // Access the input stream through the context
    FSDataInputStreamWrapper fsWrapper = context.getInputStreamWrapper();
    FSDataInputStream fsdis = fsWrapper.getStream(fsWrapper.shouldUseHBaseChecksum());
    long originalPosition = fsdis.getPos();

    long sectionIndexOffset = getSectionIndexOffset();
    try {
      LOG.debug("Seeking to load-on-open section at offset {}", sectionIndexOffset);

      // Fall back to trailer-provided level count when metadata has not been loaded yet
      int trailerLevels = trailer.getNumDataIndexLevels();
      if (trailerLevels >= 1 && trailerLevels > tenantIndexLevels) {
        tenantIndexLevels = trailerLevels;
      }

      // In HFile v4, the tenant index is stored at the recorded section index offset
      HFileBlock rootIndexBlock =
        getUncachedBlockReader().readBlockData(sectionIndexOffset, -1, true, false, false);

      // Validate this is a root index block
      if (rootIndexBlock.getBlockType() != BlockType.ROOT_INDEX) {
        throw new IOException("Expected ROOT_INDEX block for tenant index in HFile v4, found "
          + rootIndexBlock.getBlockType() + " at offset " + trailer.getLoadOnOpenDataOffset());
      }

      HFileBlock blockToRead = null;
      try {
        blockToRead = rootIndexBlock.unpack(getFileContext(), getUncachedBlockReader());

        // Load the section index from the root block (support multi-level traversal)
        if (tenantIndexLevels <= 1) {
          sectionIndexReader.loadSectionIndex(blockToRead);
        } else {
          sectionIndexReader.loadSectionIndex(blockToRead, tenantIndexLevels,
            getUncachedBlockReader());
        }

        // Copy section info to our internal data structures
        initSectionLocations();

        LOG.debug("Initialized tenant section index with {} entries", getSectionCount());
      } finally {
        if (blockToRead != null) {
          blockToRead.release();
          if (blockToRead != rootIndexBlock) {
            rootIndexBlock.release();
          }
        } else {
          rootIndexBlock.release();
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to load tenant section index", e);
      throw e;
    } finally {
      // Restore original position
      fsdis.seek(originalPosition);
    }
  }

  private long getSectionIndexOffset() {
    long offset = trailer.getSectionIndexOffset();
    return offset >= 0 ? offset : trailer.getLoadOnOpenDataOffset();
  }

  /**
   * Load information about the tenant index structure from file info.
   * <p>
   * Extracts tenant index levels and chunk size configuration from the HFile metadata to optimize
   * section lookup performance.
   */
  private void logTenantIndexStructureInfo() {
    int numSections = getSectionCount();
    if (tenantIndexLevels > 1) {
      LOG.info("Multi-tenant HFile loaded with {} sections using {}-level tenant index "
        + "(maxChunkSize={})", numSections, tenantIndexLevels, tenantIndexMaxChunkSize);
    } else {
      LOG.info("Multi-tenant HFile loaded with {} sections using single-level tenant index",
        numSections);
    }

    LOG.debug("Tenant index details: levels={}, chunkSize={}, sections={}", tenantIndexLevels,
      tenantIndexMaxChunkSize, numSections);
  }

  private Cache<ImmutableBytesWritable, SectionReaderHolder>
    createSectionReaderCache(Configuration conf) {
    int maxSize = Math.max(1,
      conf.getInt(SECTION_READER_CACHE_MAX_SIZE, DEFAULT_SECTION_READER_CACHE_MAX_SIZE));
    long expireMs =
      conf.getLong(SECTION_READER_CACHE_EXPIRE_MS, DEFAULT_SECTION_READER_CACHE_EXPIRE_MS);

    CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder().maximumSize(maxSize);
    if (expireMs > 0) {
      builder.expireAfterAccess(expireMs, TimeUnit.MILLISECONDS);
    }

    builder.removalListener(
      (RemovalNotification<ImmutableBytesWritable, SectionReaderHolder> notification) -> {
        SectionReaderHolder holder = notification.getValue();
        if (holder != null) {
          holder.markEvicted(true);
        }
      });

    Cache<ImmutableBytesWritable, SectionReaderHolder> cache = builder.build();
    LOG.debug("Initialized section reader cache with maxSize={}, expireMs={}", maxSize, expireMs);
    return cache;
  }

  private void loadTenantIndexMetadata() {
    byte[] tenantIndexLevelsBytes =
      fileInfo.get(Bytes.toBytes(MultiTenantHFileWriter.FILEINFO_TENANT_INDEX_LEVELS));
    if (tenantIndexLevelsBytes != null) {
      int parsedLevels = Bytes.toInt(tenantIndexLevelsBytes);
      if (parsedLevels >= 1) {
        tenantIndexLevels = parsedLevels;
      } else {
        LOG.warn("Ignoring invalid tenant index level count {} in file info for {}", parsedLevels,
          context.getFilePath());
        tenantIndexLevels = 1;
      }
    }

    byte[] chunkSizeBytes =
      fileInfo.get(Bytes.toBytes(MultiTenantHFileWriter.FILEINFO_TENANT_INDEX_MAX_CHUNK));
    if (chunkSizeBytes != null) {
      int parsedChunkSize = Bytes.toInt(chunkSizeBytes);
      if (parsedChunkSize > 0) {
        tenantIndexMaxChunkSize = parsedChunkSize;
      } else {
        LOG.warn("Ignoring invalid tenant index chunk size {} in file info for {}", parsedChunkSize,
          context.getFilePath());
      }
    }
  }

  /**
   * Get the number of levels in the tenant index.
   * @return The number of levels (1 for single-level, 2+ for multi-level)
   */
  public int getTenantIndexLevels() {
    return tenantIndexLevels;
  }

  /**
   * Get the maximum chunk size used in the tenant index.
   * @return The maximum entries per index block
   */
  public int getTenantIndexMaxChunkSize() {
    return tenantIndexMaxChunkSize;
  }

  /**
   * Initialize our section location map from the index reader.
   * <p>
   * Populates the internal section metadata map and creates the section ID list for efficient
   * navigation during scanning operations.
   */
  private void initSectionLocations() {
    for (SectionIndexManager.SectionIndexEntry entry : sectionIndexReader.getSections()) {
      sectionLocations.put(new ImmutableBytesWritable(entry.getTenantPrefix()),
        new SectionMetadata(entry.getOffset(), entry.getSectionSize()));
    }

    // Create list for section navigation
    sectionIds = new ArrayList<>(sectionLocations.keySet());
    // Sort by tenant prefix to ensure lexicographic order
    sectionIds.sort((a, b) -> Bytes.compareTo(a.get(), b.get()));
    LOG.debug("Initialized {} section IDs for navigation", sectionIds.size());
  }

  /**
   * Get the number of sections in this file.
   * @return The number of sections in this file
   */
  private int getSectionCount() {
    return sectionLocations.size();
  }

  /**
   * Get the total number of tenant sections in this file.
   * @return The number of sections
   */
  public int getTotalSectionCount() {
    return sectionLocations.size();
  }

  /**
   * Get table properties from the file context if available.
   * <p>
   * Properties are used for tenant configuration and optimization settings.
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

      // Get the table descriptor from Admin API
      TableName tableName = TableName.valueOf(fileContext.getTableName());

      try {
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
              LOG.debug("Loaded table properties for {}", tableName);
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
     * @param offset the file offset where the section starts
     * @param size   the size of the section in bytes
     */
    SectionMetadata(long offset, int size) {
      this.offset = offset;
      this.size = size;
    }

    /**
     * Get the offset where the section starts.
     * @return the section offset
     */
    long getOffset() {
      return offset;
    }

    /**
     * Get the size of the section.
     * @return the section size in bytes
     */
    int getSize() {
      return size;
    }
  }

  /**
   * Get metadata for a tenant section.
   * @param tenantSectionId The tenant section ID to look up
   * @return Section metadata or null if not found
   * @throws IOException If an error occurs during lookup
   */
  protected SectionMetadata getSectionMetadata(byte[] tenantSectionId) throws IOException {
    return sectionLocations.get(new ImmutableBytesWritable(tenantSectionId));
  }

  /**
   * Create a reader for a tenant section on demand.
   * @param tenantSectionId The tenant section ID for the section
   * @return A section reader or null if the section doesn't exist
   * @throws IOException If an error occurs creating the reader
   */
  protected SectionReaderLease getSectionReader(byte[] tenantSectionId) throws IOException {
    SectionMetadata metadata = getSectionMetadata(tenantSectionId);
    if (metadata == null) {
      return null;
    }

    final ImmutableBytesWritable cacheKey =
      new ImmutableBytesWritable(tenantSectionId, 0, tenantSectionId.length);
    final SectionMetadata sectionMetadata = metadata;
    try {
      SectionReaderHolder holder = sectionReaderCache.get(cacheKey, () -> {
        byte[] sectionIdCopy = Bytes.copy(tenantSectionId);
        SectionReader sectionReader = createSectionReader(sectionIdCopy, sectionMetadata);
        return new SectionReaderHolder(cacheKey, sectionReader);
      });
      holder.retain();
      return new SectionReaderLease(cacheKey, holder);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(
        "Failed to create section reader for tenant " + Bytes.toStringBinary(tenantSectionId),
        cause);
    }
  }

  /**
   * Create appropriate section reader based on type (to be implemented by subclasses).
   * @param tenantSectionId The tenant section ID
   * @param metadata        The section metadata
   * @return A section reader
   * @throws IOException If an error occurs creating the reader
   */
  protected abstract SectionReader createSectionReader(byte[] tenantSectionId,
    SectionMetadata metadata) throws IOException;

  /**
   * Get a scanner for this file.
   * @param conf         Configuration to use
   * @param cacheBlocks  Whether to cache blocks
   * @param pread        Whether to use positional read
   * @param isCompaction Whether this is for a compaction
   * @return A scanner for this file
   */
  @Override
  public HFileScanner getScanner(Configuration conf, boolean cacheBlocks, boolean pread,
    boolean isCompaction) {
    return new MultiTenantScanner(conf, cacheBlocks, pread, isCompaction);
  }

  /**
   * Simpler scanner method that delegates to the full method.
   * @param conf        Configuration to use
   * @param cacheBlocks Whether to cache blocks
   * @param pread       Whether to use positional read
   * @return A scanner for this file
   */
  @Override
  public HFileScanner getScanner(Configuration conf, boolean cacheBlocks, boolean pread) {
    return getScanner(conf, cacheBlocks, pread, false);
  }

  /**
   * Abstract base class for section readers.
   * <p>
   * Each section reader manages access to a specific tenant section within the HFile, providing
   * transparent delegation to standard HFile v3 readers with proper offset translation and resource
   * management.
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
     * @param tenantSectionId The tenant section ID
     * @param metadata        The section metadata
     */
    public SectionReader(byte[] tenantSectionId, SectionMetadata metadata) {
      this.tenantSectionId = tenantSectionId.clone(); // Make defensive copy
      this.metadata = metadata;
      this.sectionBaseOffset = metadata.getOffset();
    }

    /**
     * Get or initialize the underlying reader.
     * @return The underlying HFile reader
     * @throws IOException If an error occurs initializing the reader
     */
    public abstract HFileReaderImpl getReader() throws IOException;

    /**
     * Get a scanner for this section.
     * @param conf         Configuration to use
     * @param cacheBlocks  Whether to cache blocks
     * @param pread        Whether to use positional read
     * @param isCompaction Whether this is for a compaction
     * @return A scanner for this section
     * @throws IOException If an error occurs creating the scanner
     */
    public abstract HFileScanner getScanner(Configuration conf, boolean cacheBlocks, boolean pread,
      boolean isCompaction) throws IOException;

    /**
     * Close the section reader.
     * @throws IOException If an error occurs closing the reader
     */
    public void close() throws IOException {
      close(false);
    }

    /**
     * Close the section reader.
     * @param evictOnClose whether to evict blocks on close
     * @throws IOException If an error occurs closing the reader
     */
    public abstract void close(boolean evictOnClose) throws IOException;
  }

  /**
   * Cache entry wrapper managing lifecycle and reference counting for section readers.
   */
  private final class SectionReaderHolder {
    private final ImmutableBytesWritable cacheKey;
    private final SectionReader sectionReader;
    private final AtomicInteger refCount = new AtomicInteger(0);
    private final AtomicBoolean evicted = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    SectionReaderHolder(ImmutableBytesWritable cacheKey, SectionReader sectionReader) {
      this.cacheKey = cacheKey;
      this.sectionReader = sectionReader;
    }

    SectionReader getSectionReader() {
      return sectionReader;
    }

    void retain() {
      int refs = refCount.incrementAndGet();
      if (LOG.isTraceEnabled()) {
        LOG.trace("Retained section reader {} (refCount={})",
          Bytes.toStringBinary(sectionReader.tenantSectionId), refs);
      }
    }

    void release(boolean evictOnClose) {
      int refs = refCount.decrementAndGet();
      if (refs < 0) {
        LOG.warn("Section reader {} released too many times",
          Bytes.toStringBinary(sectionReader.tenantSectionId));
        return;
      }
      if (refs == 0 && (evicted.get() || evictOnClose)) {
        closeInternal(evictOnClose);
      }
    }

    void markEvicted(boolean evictOnClose) {
      evicted.set(true);
      if (refCount.get() == 0) {
        closeInternal(evictOnClose);
      }
    }

    void forceClose(boolean evictOnClose) {
      evicted.set(true);
      closeInternal(evictOnClose);
    }

    private void closeInternal(boolean evictOnClose) {
      if (closed.compareAndSet(false, true)) {
        try {
          sectionReader.close(evictOnClose);
        } catch (IOException e) {
          LOG.warn("Failed to close section reader {}",
            Bytes.toStringBinary(sectionReader.tenantSectionId), e);
        }
      }
    }

    @Override
    public String toString() {
      return "SectionReaderHolder{" + "tenant="
        + Bytes.toStringBinary(sectionReader.tenantSectionId) + ", refCount=" + refCount.get()
        + ", evicted=" + evicted.get() + ", closed=" + closed.get() + '}';
    }
  }

  /**
   * Lease handle giving callers access to a cached section reader while ensuring proper release.
   */
  protected final class SectionReaderLease implements AutoCloseable {
    private final ImmutableBytesWritable cacheKey;
    private final SectionReaderHolder holder;
    private final SectionReader sectionReader;
    private boolean closed;

    SectionReaderLease(ImmutableBytesWritable cacheKey, SectionReaderHolder holder) {
      this.cacheKey = cacheKey;
      this.holder = holder;
      this.sectionReader = holder.getSectionReader();
    }

    public SectionReader getSectionReader() {
      return sectionReader;
    }

    public HFileReaderImpl getReader() throws IOException {
      return sectionReader.getReader();
    }

    @Override
    public void close() {
      release(false);
    }

    public void close(boolean evictOnClose) {
      release(evictOnClose);
    }

    private void release(boolean evictOnClose) {
      if (closed) {
        return;
      }
      closed = true;
      holder.release(evictOnClose);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Released lease for tenant {} (cacheKey={})",
          Bytes.toStringBinary(sectionReader.tenantSectionId), cacheKey);
      }
    }
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
    /** Current section reader lease */
    protected SectionReaderLease currentSectionLease;
    /** Current section reader */
    protected SectionReader currentSectionReader;
    /** Whether we have successfully seeked */
    protected boolean seeked = false;

    /**
     * Constructor for MultiTenantScanner.
     * @param conf         Configuration to use
     * @param cacheBlocks  Whether to cache blocks
     * @param pread        Whether to use positional read
     * @param isCompaction Whether this is for a compaction
     */
    public MultiTenantScanner(Configuration conf, boolean cacheBlocks, boolean pread,
      boolean isCompaction) {
      this.conf = conf;
      this.cacheBlocks = cacheBlocks;
      this.pread = pread;
      this.isCompaction = isCompaction;
    }

    /**
     * Switch to a new section reader, properly managing resource cleanup.
     * @param newReader The new section reader to switch to
     * @param sectionId The section ID for the new reader
     * @throws IOException If an error occurs during the switch
     */
    private void switchToSectionReader(SectionReaderLease newLease, byte[] sectionId)
      throws IOException {
      if (currentScanner != null) {
        currentScanner.close();
        currentScanner = null;
      }
      if (currentSectionLease != null) {
        currentSectionLease.close();
        currentSectionLease = null;
      }

      currentSectionReader = null;
      currentTenantSectionId = null;

      if (newLease != null) {
        currentSectionLease = newLease;
        currentSectionReader = newLease.getSectionReader();
        currentTenantSectionId = sectionId;
        try {
          currentScanner = currentSectionReader.getScanner(conf, cacheBlocks, pread, isCompaction);
        } catch (IOException | RuntimeException e) {
          currentSectionLease.close();
          currentSectionLease = null;
          currentSectionReader = null;
          currentTenantSectionId = null;
          throw e;
        }
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

        if (switchToSection(firstSectionId)) {
          boolean result = currentScanner.seekTo();
          seeked = result;
          return result;
        } else {
          LOG.debug("No section reader available for first section {}",
            Bytes.toStringBinary(firstSectionId));
        }
      }

      // If we reach here, no sections were found or seeking failed
      seeked = false;
      return false;
    }

    private boolean switchToSection(byte[] sectionId) throws IOException {
      SectionReaderLease lease = getSectionReader(sectionId);
      if (lease == null) {
        return false;
      }
      switchToSectionReader(lease, sectionId);
      return true;
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

      // Extract tenant section ID for the target key
      byte[] targetSectionId = tenantExtractor.extractTenantSectionId(key);

      // Find insertion point for the target section in the sorted sectionIds list
      int n = sectionIds.size();
      int insertionIndex = n; // default: after last
      boolean exactSectionMatch = false;
      for (int i = 0; i < n; i++) {
        byte[] sid = sectionIds.get(i).get();
        int cmp = Bytes.compareTo(sid, targetSectionId);
        if (cmp == 0) {
          insertionIndex = i;
          exactSectionMatch = true;
          break;
        }
        if (cmp > 0) {
          insertionIndex = i;
          break;
        }
      }

      // If there is no exact section for this tenant prefix
      if (!exactSectionMatch) {
        if (insertionIndex == 0) {
          // Key sorts before first section => before first key of entire file
          seeked = false;
          return -1;
        }
        return positionToPreviousSection(insertionIndex - 1);
      }

      // Exact section exists. Seek within that section first.
      byte[] matchedSectionId = sectionIds.get(insertionIndex).get();
      if (!switchToSection(matchedSectionId)) {
        if (insertionIndex == 0) {
          seeked = false;
          return -1;
        }
        return positionToPreviousSection(insertionIndex - 1);
      }

      int result = currentScanner.seekTo(key);
      if (result == -1) {
        // Key sorts before first key in this section. If this is the first section overall,
        // then the key is before the first key in the entire file.
        if (insertionIndex == 0) {
          seeked = false;
          return -1;
        }
        return positionToPreviousSection(insertionIndex - 1);
      }
      seeked = true;
      return result;
    }

    private int positionToPreviousSection(int startIndex) throws IOException {
      for (int i = startIndex; i >= 0; i--) {
        byte[] prevSectionId = sectionIds.get(i).get();
        if (!switchToSection(prevSectionId)) {
          continue;
        }
        try {
          Optional<ExtendedCell> lastKeyOpt = currentSectionReader.getReader().getLastKey();
          if (lastKeyOpt.isPresent()) {
            currentScanner.seekTo(lastKeyOpt.get());
            seeked = true;
            return 1;
          }
        } catch (IOException e) {
          LOG.warn("Failed to retrieve last key from section {}",
            Bytes.toStringBinary(prevSectionId), e);
        }
      }
      seeked = false;
      return -1;
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
      if (!switchToSection(tenantSectionId)) {
        seeked = false;
        return false;
      }
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
        if (!switchToSection(nextTenantSectionId)) {
          seeked = false;
          return false;
        }

        // Prefetch the section after next if enabled
        if (prefetchEnabled) {
          prefetchNextSection(nextTenantSectionId);
        }

        boolean result = currentScanner.seekTo();
        seeked = result;
        return result;
      }

      return true;
    }

    /**
     * Prefetch the next section after the given one for sequential access optimization.
     * @param currentSectionId The current section ID
     */
    private void prefetchNextSection(byte[] currentSectionId) {
      try {
        byte[] nextSectionId = findNextTenantSectionId(currentSectionId);
        if (nextSectionId != null) {
          // Trigger async load by creating the reader
          SectionReaderLease lease = getSectionReader(nextSectionId);
          if (lease != null) {
            lease.close();
          }
        }
      } catch (Exception e) {
        // Prefetch is best-effort, don't fail the operation
        LOG.debug("Failed to prefetch next section", e);
      }
    }

    /**
     * Find the next tenant section ID after the current one.
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
      if (currentSectionLease != null) {
        currentSectionLease.close();
        currentSectionLease = null;
      }
      currentSectionReader = null;
      currentTenantSectionId = null;
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
   * @throws IOException If an error occurs during close
   */
  @Override
  public void close() throws IOException {
    close(false);
  }

  /**
   * Close underlying resources, with optional block eviction.
   * @param evictOnClose Whether to evict blocks on close
   * @throws IOException If an error occurs during close
   */
  @Override
  public void close(boolean evictOnClose) throws IOException {
    sectionReaderCache.asMap().forEach((key, holder) -> {
      if (holder != null) {
        holder.forceClose(evictOnClose);
      }
    });
    sectionReaderCache.invalidateAll();
    sectionReaderCache.cleanUp();

    // Close filesystem block reader streams
    if (fsBlockReader != null) {
      fsBlockReader.closeStreams();
    }

    // Unbuffer the main input stream wrapper
    context.getInputStreamWrapper().unbuffer();
  }

  /**
   * Get HFile version.
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
   * @param metadata   The section metadata containing offset and size
   * @param readerType The type of reader (PREAD or STREAM)
   * @return A reader context for the section, or null if section is invalid
   * @throws IOException If an error occurs building the context
   */
  protected ReaderContext buildSectionContext(SectionMetadata metadata,
    ReaderContext.ReaderType readerType) throws IOException {
    // Create a special wrapper with offset translation capabilities
    FSDataInputStreamWrapper parentWrapper = context.getInputStreamWrapper();
    MultiTenantFSDataInputStreamWrapper sectionWrapper =
      new MultiTenantFSDataInputStreamWrapper(parentWrapper, metadata.getOffset());

    // Calculate section size and validate minimum requirements
    int sectionSize = metadata.getSize();
    int trailerSize = FixedFileTrailer.getTrailerSize(3); // HFile v3 sections use v3 format

    if (sectionSize < trailerSize) {
      LOG.warn("Section size {} for offset {} is smaller than minimum trailer size {}", sectionSize,
        metadata.getOffset(), trailerSize);
      return null;
    }

    // Build the reader context with proper file size calculation
    ReaderContext sectionContext = ReaderContextBuilder.newBuilder(context)
      .withInputStreamWrapper(sectionWrapper).withFilePath(context.getFilePath())
      .withReaderType(readerType).withFileSystem(context.getFileSystem()).withFileSize(sectionSize) // Use
                                                                                                    // section
                                                                                                    // size;
                                                                                                    // wrapper
                                                                                                    // handles
                                                                                                    // offset
                                                                                                    // translation
      .build();

    LOG.debug("Created section reader context for offset {}, size {}", metadata.getOffset(),
      sectionSize);
    return sectionContext;
  }

  /**
   * Get all tenant section IDs present in the file.
   * <p>
   * Returns a defensive copy of all section IDs for external iteration without exposing internal
   * data structures.
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
   * For multi-tenant HFiles, get the first key from the first available section. This overrides the
   * HFileReaderImpl implementation that requires dataBlockIndexReader.
   * @return The first key if available
   */
  @Override
  public Optional<ExtendedCell> getFirstKey() {
    try {
      // Get the first section and try to read its first key
      for (ImmutableBytesWritable sectionKey : sectionLocations.keySet()) {
        byte[] sectionId = sectionKey.get();
        try (SectionReaderLease lease = getSectionReader(sectionId)) {
          if (lease == null) {
            continue;
          }
          HFileReaderImpl reader = lease.getReader();
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
   * For multi-tenant HFiles, get the last key from the last available section. This overrides the
   * HFileReaderImpl implementation that requires dataBlockIndexReader.
   * @return The last key if available
   */
  @Override
  public Optional<ExtendedCell> getLastKey() {
    try {
      // Get the last section and try to read its last key
      // Since LinkedHashMap maintains insertion order, iterate in reverse to get the last section
      // first
      List<ImmutableBytesWritable> sectionKeys = new ArrayList<>(sectionLocations.keySet());
      for (int i = sectionKeys.size() - 1; i >= 0; i--) {
        byte[] sectionId = sectionKeys.get(i).get();
        try (SectionReaderLease lease = getSectionReader(sectionId)) {
          if (lease == null) {
            continue;
          }
          HFileReaderImpl reader = lease.getReader();
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
   * For HFile v4 multi-tenant files, meta blocks don't exist at the file level. They exist within
   * individual sections. This method is not supported.
   * @param metaBlockName the name of the meta block to retrieve
   * @param cacheBlock    whether to cache the block
   * @return always null for multi-tenant HFiles
   * @throws IOException if an error occurs
   */
  @Override
  public HFileBlock getMetaBlock(String metaBlockName, boolean cacheBlock) throws IOException {
    byte[] cachedSectionId = null;
    if (metaBlockCacheEnabled && metaBlockSectionCache != null) {
      ImmutableBytesWritable cached = metaBlockSectionCache.getIfPresent(metaBlockName);
      if (cached != null) {
        cachedSectionId = copySectionId(cached);
      }
    }

    if (cachedSectionId != null) {
      HFileBlock cachedBlock = loadMetaBlockFromSection(cachedSectionId, metaBlockName, cacheBlock);
      if (cachedBlock != null) {
        return cachedBlock;
      }
      if (metaBlockCacheEnabled && metaBlockSectionCache != null) {
        metaBlockSectionCache.invalidate(metaBlockName);
      }
    }

    if (sectionIds == null || sectionIds.isEmpty()) {
      return null;
    }

    for (ImmutableBytesWritable sectionId : sectionIds) {
      byte[] candidateSectionId = copySectionId(sectionId);
      if (cachedSectionId != null && Bytes.equals(candidateSectionId, cachedSectionId)) {
        continue;
      }
      HFileBlock sectionBlock = loadMetaBlockFromSection(candidateSectionId, metaBlockName,
        cacheBlock);
      if (sectionBlock != null) {
        if (metaBlockCacheEnabled && metaBlockSectionCache != null) {
          metaBlockSectionCache.put(metaBlockName, new ImmutableBytesWritable(candidateSectionId));
        }
        return sectionBlock;
      }
    }

    return null;
  }

  /**
   * For HFile v4 multi-tenant files, bloom filter metadata doesn't exist at the file level. It
   * exists within individual sections. This method is not supported.
   * @return always null for multi-tenant HFiles
   * @throws IOException if an error occurs
   */
  @Override
  public DataInput getGeneralBloomFilterMetadata() throws IOException {
    // HFile v4 multi-tenant files don't have file-level bloom filters
    // Bloom filters exist within individual sections
    LOG.debug(
      "General bloom filter metadata not supported at file level for HFile v4 multi-tenant files");
    return null;
  }

  /**
   * For HFile v4 multi-tenant files, delete bloom filter metadata doesn't exist at the file level.
   * It exists within individual sections. This method is not supported.
   * @return always null for multi-tenant HFiles
   * @throws IOException if an error occurs
   */
  @Override
  public DataInput getDeleteBloomFilterMetadata() throws IOException {
    // HFile v4 multi-tenant files don't have file-level delete bloom filters
    // Delete bloom filters exist within individual sections
    LOG.debug(
      "Delete bloom filter metadata not supported at file level for HFile v4 multi-tenant files");
    return null;
  }

  private HFileBlock loadMetaBlockFromSection(byte[] sectionId, String metaBlockName,
    boolean cacheBlock) throws IOException {
    try (SectionReaderLease lease = getSectionReader(sectionId)) {
      if (lease == null) {
        return null;
      }
      HFileReaderImpl reader = lease.getReader();
      return reader.getMetaBlock(metaBlockName, cacheBlock);
    }
  }

  private byte[] copySectionId(ImmutableBytesWritable sectionId) {
    return Bytes.copy(sectionId.get(), sectionId.getOffset(), sectionId.getLength());
  }

  /**
   * For HFile v4 multi-tenant files, index size is just the section index size.
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
   * Find a key at approximately the given position within a section.
   * @param reader         The section reader
   * @param targetProgress The target position as a percentage (0.0 to 1.0) within the section
   * @return A key near the target position, or empty if not found
   */

  /**
   * Override mid-key calculation to find the middle key that respects tenant boundaries. For single
   * tenant files, returns the midkey from the section. For multi-tenant files, finds the optimal
   * tenant boundary that best balances the split.
   * @return the middle key of the file
   * @throws IOException if an error occurs
   */
  @Override
  public Optional<ExtendedCell> midKey() throws IOException {
    // Handle empty file case
    if (sectionLocations.isEmpty()) {
      LOG.debug("No sections in file, returning empty midkey");
      return Optional.empty();
    }

    // If there's only one section (single tenant), use that section's midkey
    if (sectionLocations.size() == 1) {
      byte[] sectionId = sectionLocations.keySet().iterator().next().get();
      try (SectionReaderLease lease = getSectionReader(sectionId)) {
        if (lease == null) {
          throw new IOException("Unable to create section reader for single tenant section: "
            + Bytes.toStringBinary(sectionId));
        }

        HFileReaderImpl reader = lease.getReader();
        Optional<ExtendedCell> midKey = reader.midKey();
        LOG.debug("Single tenant midkey: {}", midKey.orElse(null));
        return midKey;
      }
    }

    // For multiple tenants, find the optimal tenant boundary for splitting
    // This ensures we never split within a tenant's data range
    return findOptimalTenantBoundaryForSplit();
  }

  /**
   * Find the optimal tenant boundary that best balances the region split. This method ensures that
   * splits always occur at tenant boundaries, preserving tenant isolation and maintaining proper
   * key ordering.
   * @return the optimal boundary key for splitting
   * @throws IOException if an error occurs
   */
  private Optional<ExtendedCell> findOptimalTenantBoundaryForSplit() throws IOException {
    // Calculate total data volume and ideal split point
    long totalFileSize = 0;
    List<TenantSectionInfo> tenantSections = new ArrayList<>();

    for (Map.Entry<ImmutableBytesWritable, SectionMetadata> entry : sectionLocations.entrySet()) {
      SectionMetadata metadata = entry.getValue();
      totalFileSize += metadata.getSize();

      tenantSections
        .add(new TenantSectionInfo(entry.getKey().get(), metadata.getSize(), totalFileSize // cumulative
                                                                                           // size
                                                                                           // up to
                                                                                           // this
                                                                                           // point
        ));
    }

    if (totalFileSize == 0) {
      LOG.debug("No data in file, returning empty midkey");
      return Optional.empty();
    }

    long idealSplitSize = totalFileSize / 2;

    // Find the tenant boundary that best approximates the ideal split
    TenantSectionInfo bestBoundary = findBestTenantBoundary(tenantSections, idealSplitSize);

    if (bestBoundary == null) {
      // Fallback: use the middle tenant if we can't find an optimal boundary
      int middleTenantIndex = tenantSections.size() / 2;
      bestBoundary = tenantSections.get(middleTenantIndex);
      LOG.debug("Using middle tenant as fallback boundary: {}",
        Bytes.toStringBinary(bestBoundary.tenantSectionId));
    }

    // Get the first key of the selected tenant section as the split point
    // This ensures the split happens exactly at the tenant boundary
    try (SectionReaderLease lease = getSectionReader(bestBoundary.tenantSectionId)) {
      if (lease == null) {
        throw new IOException("Unable to create section reader for boundary tenant: "
          + Bytes.toStringBinary(bestBoundary.tenantSectionId));
      }

      HFileReaderImpl reader = lease.getReader();
      Optional<ExtendedCell> firstKey = reader.getFirstKey();

      if (firstKey.isPresent()) {
        LOG.info("Selected tenant boundary midkey: {} (tenant: {}, split balance: {}/{})",
          firstKey.get(), Bytes.toStringBinary(bestBoundary.tenantSectionId),
          bestBoundary.cumulativeSize - bestBoundary.sectionSize, totalFileSize);
        return firstKey;
      }

      // If we can't get the first key, try the section's lastkey as fallback
      Optional<ExtendedCell> sectionLastKey = reader.getLastKey();
      if (sectionLastKey.isPresent()) {
        LOG.warn(
          "Using section last key as fallback (tenant boundary not available): {} (tenant: {})",
          sectionLastKey.get(), Bytes.toStringBinary(bestBoundary.tenantSectionId));
        return sectionLastKey;
      }

      throw new IOException("Unable to get any key from selected boundary tenant: "
        + Bytes.toStringBinary(bestBoundary.tenantSectionId));
    }
  }

  /**
   * Find the tenant boundary that provides the most balanced split. This uses a heuristic to find
   * the boundary that gets closest to a 50/50 split while maintaining tenant isolation.
   * @param tenantSections List of tenant sections with cumulative sizes
   * @param idealSplitSize The ideal size for the first region after split
   * @return The best tenant boundary, or null if none suitable
   */
  private TenantSectionInfo findBestTenantBoundary(List<TenantSectionInfo> tenantSections,
    long idealSplitSize) {
    TenantSectionInfo bestBoundary = null;
    long bestDeviation = Long.MAX_VALUE;

    // Evaluate each potential tenant boundary
    for (int i = 1; i < tenantSections.size(); i++) { // Start from 1 to exclude first tenant
      TenantSectionInfo boundary = tenantSections.get(i);

      // Calculate how balanced this split would be
      long leftSideSize = boundary.cumulativeSize - boundary.sectionSize; // Size before this tenant
      long deviation = Math.abs(leftSideSize - idealSplitSize);

      // Prefer boundaries that create more balanced splits
      if (deviation < bestDeviation) {
        bestDeviation = deviation;
        bestBoundary = boundary;
      }

      LOG.debug("Evaluating tenant boundary: {} (left: {}, deviation: {})",
        Bytes.toStringBinary(boundary.tenantSectionId), leftSideSize, deviation);
    }

    // Only use a boundary if it's reasonably balanced (within 30% of ideal)
    if (bestBoundary != null) {
      long leftSideSize = bestBoundary.cumulativeSize - bestBoundary.sectionSize;
      double balanceRatio = Math.abs((double) leftSideSize / idealSplitSize - 1.0);

      if (balanceRatio > 0.3) { // More than 30% deviation
        LOG.warn("Best tenant boundary has poor balance ratio: {:.1f}% (tenant: {})",
          balanceRatio * 100, Bytes.toStringBinary(bestBoundary.tenantSectionId));
        // Still return it - tenant boundary is more important than perfect balance
      }
    }

    return bestBoundary;
  }

  /**
   * Helper class to track tenant section information for split analysis.
   */
  private static class TenantSectionInfo {
    final byte[] tenantSectionId;
    final long sectionSize;
    final long cumulativeSize;

    TenantSectionInfo(byte[] tenantSectionId, long sectionSize, long cumulativeSize) {
      this.tenantSectionId = tenantSectionId;
      this.sectionSize = sectionSize;
      this.cumulativeSize = cumulativeSize;
    }
  }

  /**
   * Override block reading to support tenant-aware block access. Routes block reads to the
   * appropriate section based on offset.
   * @param dataBlockOffset           the offset of the block to read
   * @param onDiskBlockSize           the on-disk size of the block
   * @param cacheBlock                whether to cache the block
   * @param pread                     whether to use positional read
   * @param isCompaction              whether this is for a compaction
   * @param updateCacheMetrics        whether to update cache metrics
   * @param expectedBlockType         the expected block type
   * @param expectedDataBlockEncoding the expected data block encoding
   * @return the read block
   * @throws IOException if an error occurs reading the block
   */
  @Override
  public HFileBlock readBlock(long dataBlockOffset, long onDiskBlockSize, boolean cacheBlock,
    boolean pread, boolean isCompaction, boolean updateCacheMetrics, BlockType expectedBlockType,
    DataBlockEncoding expectedDataBlockEncoding) throws IOException {

    try (SectionReaderLease lease = findSectionForOffset(dataBlockOffset)) {
      if (lease == null) {
        throw new IOException(
          "No section found for offset: " + dataBlockOffset + ", path=" + getPath());
      }

      SectionReader targetSectionReader = lease.getSectionReader();
      HFileReaderImpl sectionReader = lease.getReader();

      // Convert absolute offset to section-relative offset
      long sectionRelativeOffset = dataBlockOffset - targetSectionReader.sectionBaseOffset;

      return sectionReader.readBlock(sectionRelativeOffset, onDiskBlockSize, cacheBlock, pread,
        isCompaction, updateCacheMetrics, expectedBlockType, expectedDataBlockEncoding);
    } catch (IOException e) {
      LOG.error("Failed to read block at offset {} from section", dataBlockOffset, e);
      throw e;
    }
  }

  /**
   * Override block reading with section routing.
   * @param dataBlockOffset           the offset of the block to read
   * @param onDiskBlockSize           the on-disk size of the block
   * @param cacheBlock                whether to cache the block
   * @param pread                     whether to use positional read
   * @param isCompaction              whether this is for a compaction
   * @param updateCacheMetrics        whether to update cache metrics
   * @param expectedBlockType         the expected block type
   * @param expectedDataBlockEncoding the expected data block encoding
   * @param cacheOnly                 whether to only read from cache
   * @return the read block
   * @throws IOException if an error occurs reading the block
   */
  @Override
  public HFileBlock readBlock(long dataBlockOffset, long onDiskBlockSize, boolean cacheBlock,
    boolean pread, boolean isCompaction, boolean updateCacheMetrics, BlockType expectedBlockType,
    DataBlockEncoding expectedDataBlockEncoding, boolean cacheOnly) throws IOException {

    try (SectionReaderLease lease = findSectionForOffset(dataBlockOffset)) {
      if (lease == null) {
        throw new IOException(
          "No section found for offset: " + dataBlockOffset + ", path=" + getPath());
      }

      SectionReader targetSectionReader = lease.getSectionReader();
      HFileReaderImpl sectionReader = lease.getReader();

      // Convert absolute offset to section-relative offset
      long sectionRelativeOffset = dataBlockOffset - targetSectionReader.sectionBaseOffset;

      return sectionReader.readBlock(sectionRelativeOffset, onDiskBlockSize, cacheBlock, pread,
        isCompaction, updateCacheMetrics, expectedBlockType, expectedDataBlockEncoding, cacheOnly);
    } catch (IOException e) {
      LOG.error("Failed to read block at offset {} from section", dataBlockOffset, e);
      throw e;
    }
  }

  /**
   * Find the section reader that contains the given absolute file offset.
   * @param absoluteOffset the absolute offset in the file
   * @return the section reader containing this offset, or null if not found
   */
  private SectionReaderLease findSectionForOffset(long absoluteOffset) throws IOException {
    for (Map.Entry<ImmutableBytesWritable, SectionMetadata> entry : sectionLocations.entrySet()) {
      SectionMetadata metadata = entry.getValue();
      if (
        absoluteOffset >= metadata.getOffset()
          && absoluteOffset < metadata.getOffset() + metadata.getSize()
      ) {
        return getSectionReader(entry.getKey().get());
      }
    }
    return null;
  }

  /**
   * For HFile v4 multi-tenant files, MVCC information is determined from file info only.
   * @return true if file info indicates MVCC information is present
   */
  @Override
  public boolean hasMVCCInfo() {
    // HFile v4 multi-tenant files determine MVCC info from file info only
    return fileInfo.shouldIncludeMemStoreTS() && fileInfo.isDecodeMemstoreTS();
  }

  /**
   * For HFile v4 multi-tenant files, entry count is determined from trailer only.
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
   * Override unbuffer stream to handle main context.
   */
  @Override
  public void unbufferStream() {
    // Unbuffer the main context
    super.unbufferStream();

    // Section readers are created on demand and managed by scanner
  }

  /**
   * For HFile v4 multi-tenant files, effective encoding is ignored.
   * @param isCompaction whether this is for a compaction
   * @return always NONE for multi-tenant HFiles
   */
  @Override
  public DataBlockEncoding getEffectiveEncodingInCache(boolean isCompaction) {
    // HFile v4 multi-tenant files ignore effective encoding
    LOG.debug("Effective encoding ignored for HFile v4 multi-tenant files");
    return DataBlockEncoding.NONE;
  }

  /**
   * Get section-specific statistics for monitoring and debugging.
   * @return a map of section statistics
   */
  public Map<String, Object> getSectionStatistics() {
    Map<String, Object> stats = new HashMap<>();

    stats.put("totalSections", sectionLocations.size());
    stats.put("tenantIndexLevels", tenantIndexLevels);
    stats.put("tenantIndexMaxChunkSize", tenantIndexMaxChunkSize);
    stats.put("prefetchEnabled", prefetchEnabled);
    stats.put("cachedSectionReaders", sectionReaderCache.size());

    // Section size distribution
    List<Integer> sectionSizes = new ArrayList<>();
    for (SectionMetadata metadata : sectionLocations.values()) {
      sectionSizes.add(metadata.getSize());
    }
    if (!sectionSizes.isEmpty()) {
      stats.put("avgSectionSize",
        sectionSizes.stream().mapToInt(Integer::intValue).average().orElse(0.0));
      stats.put("minSectionSize",
        sectionSizes.stream().mapToInt(Integer::intValue).min().orElse(0));
      stats.put("maxSectionSize",
        sectionSizes.stream().mapToInt(Integer::intValue).max().orElse(0));
    }

    return stats;
  }

  /**
   * Get metadata for a specific tenant section by section ID.
   * @param tenantSectionId The tenant section ID to look up
   * @return Detailed metadata about the section
   */
  public Map<String, Object> getSectionInfo(byte[] tenantSectionId) {
    Map<String, Object> info = new HashMap<>();

    ImmutableBytesWritable key = new ImmutableBytesWritable(tenantSectionId);
    SectionMetadata metadata = sectionLocations.get(key);

    if (metadata != null) {
      info.put("exists", true);
      info.put("offset", metadata.getOffset());
      info.put("size", metadata.getSize());
    } else {
      info.put("exists", false);
    }

    return info;
  }

  /**
   * Backward-compatibility shim for v3 expectations.
   * <p>
   * Some existing code paths and unit tests (e.g. TestSeekTo) expect that a reader exposes a
   * non-null data block index at the file level. For HFile v4 multi-tenant containers there is no
   * global data index. When the container holds exactly one tenant section (the common case when
   * multi-tenant writing is disabled), we can safely delegate to that section's v3 reader and
   * expose its data block index to preserve v3 semantics.
   */
  @Override
  public HFileBlockIndex.CellBasedKeyBlockIndexReader getDataBlockIndexReader() {
    // If already initialized by a previous call, return it
    HFileBlockIndex.CellBasedKeyBlockIndexReader existing = super.getDataBlockIndexReader();
    if (existing != null) {
      return existing;
    }

    // Only provide a delegating index reader for single-section files
    if (sectionLocations.size() == 1) {
      try {
        // Resolve the sole section reader
        byte[] sectionId = sectionIds.get(0).get();
        try (SectionReaderLease lease = getSectionReader(sectionId)) {
          if (lease == null) {
            return null;
          }
          HFileReaderImpl inner = lease.getReader();
          HFileBlockIndex.CellBasedKeyBlockIndexReader delegate = inner.getDataBlockIndexReader();
          // Cache on this reader so subsequent calls are fast and callers see a stable instance
          setDataBlockIndexReader(delegate);
          return delegate;
        }
      } catch (IOException e) {
        LOG.warn("Failed to obtain section data block index reader for v3 compatibility", e);
      }
    }

    // Multi-section containers intentionally do not expose a global data index
    return null;
  }

  /**
   * For HFile v4 multi-tenant files, data block encoding is ignored at file level.
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
   * @return true if prefetching has started
   */
  @Override
  public boolean prefetchStarted() {
    // Multi-tenant files start prefetch immediately on open
    return prefetchEnabled;
  }

  /**
   * Get file length from the context.
   * @return the file length in bytes
   */
  @Override
  public long length() {
    return context.getFileSize();
  }

  /**
   * Check if file info is loaded (always true for multi-tenant readers).
   * @return true as file info is always loaded during construction
   */
  public boolean isFileInfoLoaded() {
    return true;
  }

  /**
   * Override getHFileInfo to properly load FileInfo metadata for v4 files.
   * <p>
   * Since initMetaAndIndex() is skipped for v4 files, we need to manually load the FileInfo block
   * to expose the metadata written during file creation.
   * <p>
   * This method ensures that the FileInfo block is loaded on-demand when HFilePrettyPrinter or
   * other tools request the file metadata.
   * @return The HFileInfo object with loaded metadata
   */
  @Override
  public HFileInfo getHFileInfo() {
    // For v4 files, ensure FileInfo block is loaded on-demand
    if (fileInfo.isEmpty()) {
      try {
        loadFileInfoBlock();
      } catch (IOException e) {
        LOG.error("Failed to load FileInfo block for multi-tenant HFile", e);
        // Continue with empty fileInfo rather than throwing exception
      }
    }

    return fileInfo;
  }

  /**
   * Manually load the FileInfo block for multi-tenant HFiles.
   * <p>
   * This method replicates the FileInfo loading logic from HFileInfo.loadMetaInfo() but adapted for
   * the multi-tenant file structure.
   * @throws IOException if an error occurs loading the FileInfo block
   */
  private void loadFileInfoBlock() throws IOException {
    FixedFileTrailer trailer = getTrailer();

    // Get the FileInfo block offset from the trailer
    long fileInfoOffset = trailer.getFileInfoOffset();
    if (fileInfoOffset == 0) {
      LOG.debug("No FileInfo block found in multi-tenant HFile");
      return;
    }

    // Access the input stream through the context
    FSDataInputStreamWrapper fsWrapper = context.getInputStreamWrapper();
    FSDataInputStream fsdis = fsWrapper.getStream(fsWrapper.shouldUseHBaseChecksum());
    long originalPosition = fsdis.getPos();

    try {
      LOG.debug("Loading FileInfo block from offset {}", fileInfoOffset);

      // Read the FileInfo block
      HFileBlock fileInfoBlock =
        getUncachedBlockReader().readBlockData(fileInfoOffset, -1, true, false, false);
      HFileBlock blockToRead = null;

      // Validate this is a FileInfo block
      if (fileInfoBlock.getBlockType() != BlockType.FILE_INFO) {
        throw new IOException("Expected FILE_INFO block at offset " + fileInfoOffset + ", found "
          + fileInfoBlock.getBlockType());
      }

      // Parse the FileInfo data using the HFileInfo.read() method
      try {
        blockToRead = fileInfoBlock.unpack(getFileContext(), getUncachedBlockReader());
        try (DataInputStream dis = new DataInputStream(blockToRead.getByteStream())) {
          fileInfo.read(dis);
        }
        applyFileInfoMetadataToContext();
      } finally {
        if (blockToRead != null) {
          blockToRead.release();
          if (blockToRead != fileInfoBlock) {
            fileInfoBlock.release();
          }
        } else {
          fileInfoBlock.release();
        }
      }

      LOG.debug("Successfully loaded FileInfo with {} entries", fileInfo.size());
    } catch (IOException e) {
      LOG.error("Failed to load FileInfo block from offset {}", fileInfoOffset, e);
      throw e;
    } finally {
      // Restore original position
      try {
        fsdis.seek(originalPosition);
      } catch (IOException e) {
        LOG.warn("Failed to restore stream position", e);
      }
    }
  }

  private void applyFileInfoMetadataToContext() {
    HFileContext fileContext = getFileContext();

    byte[] creationTimeBytes = fileInfo.get(HFileInfo.CREATE_TIME_TS);
    if (creationTimeBytes != null) {
      fileContext.setFileCreateTime(Bytes.toLong(creationTimeBytes));
    }

    byte[] maxTagsLenBytes = fileInfo.get(HFileInfo.MAX_TAGS_LEN);
    boolean includesTags = maxTagsLenBytes != null;
    fileContext.setIncludesTags(includesTags);
    if (includesTags) {
      byte[] tagsCompressedBytes = fileInfo.get(HFileInfo.TAGS_COMPRESSED);
      boolean tagsCompressed = tagsCompressedBytes != null && Bytes.toBoolean(tagsCompressedBytes);
      fileContext.setCompressTags(tagsCompressed);
    }

    byte[] keyValueVersionBytes = fileInfo.get(HFileWriterImpl.KEY_VALUE_VERSION);
    boolean includesMvcc = keyValueVersionBytes != null
      && Bytes.toInt(keyValueVersionBytes) == HFileWriterImpl.KEY_VALUE_VER_WITH_MEMSTORE;
    fileContext.setIncludesMvcc(includesMvcc);
  }

  /**
   * Enhanced toString with multi-tenant specific information.
   * @return detailed string representation of this reader
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("MultiTenantReader{");
    sb.append("path=").append(getPath());
    sb.append(", majorVersion=").append(getMajorVersion());
    sb.append(", sections=").append(sectionLocations.size());
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
