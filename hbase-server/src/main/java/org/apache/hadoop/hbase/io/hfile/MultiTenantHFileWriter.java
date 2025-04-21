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

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;

import static org.apache.hadoop.hbase.io.hfile.BlockCompressedSizePredicator.MAX_BLOCK_SIZE_UNCOMPRESSED;

/**
 * An HFile writer that supports multiple tenants by sectioning the data within a single file.
 * This implementation takes advantage of the fact that HBase data is always written
 * in sorted order, so once we move to a new tenant, we'll never go back to a previous one.
 * 
 * Instead of creating separate physical files for each tenant, this writer creates a 
 * single HFile with internal sections that are indexed by tenant prefix.
 */
@InterfaceAudience.Private
public class MultiTenantHFileWriter implements HFile.Writer {
  private static final Logger LOG = LoggerFactory.getLogger(MultiTenantHFileWriter.class);
  
  // Tenant identification configuration at cluster level
  public static final String TENANT_PREFIX_LENGTH = "hbase.multi.tenant.prefix.length";
  public static final String TENANT_PREFIX_OFFSET = "hbase.multi.tenant.prefix.offset";
  
  // Tenant identification configuration at table level (higher precedence)
  public static final String TABLE_TENANT_PREFIX_LENGTH = "TENANT_PREFIX_LENGTH";
  public static final String TABLE_TENANT_PREFIX_OFFSET = "TENANT_PREFIX_OFFSET";
  
  // Table-level property to enable/disable multi-tenant sectioning
  public static final String TABLE_MULTI_TENANT_ENABLED = "MULTI_TENANT_HFILE";
  private static final byte[] DEFAULT_TENANT_PREFIX = new byte[0]; // Empty prefix for default tenant
  
  /**
   * Class that manages tenant configuration with proper precedence:
   * 1. Table level settings have highest precedence
   * 2. Cluster level settings are used as fallback
   * 3. Default values are used if neither is specified
   */
  // TenantConfiguration class removed - use TenantExtractorFactory instead
  
  private final TenantExtractor tenantExtractor;
  private final FileSystem fs;
  private final Path path;
  private final Configuration conf;
  private final CacheConfig cacheConf;
  private final HFileContext fileContext;
  
  // Main file writer components
  private final FSDataOutputStream outputStream;
  private HFileBlock.Writer blockWriter;
  private SectionIndexManager.Writer sectionIndexWriter;
  
  // Section tracking
  private VirtualSectionWriter currentSectionWriter;
  private byte[] currentTenantPrefix;
  private long sectionStartOffset;
  private int sectionCount = 0;
  
  // Stats for the entire file
  private Cell lastCell = null; // Keep this for internal tracking but don't use in global structures
  private long entryCount = 0;
  private long totalKeyLength = 0;
  private long totalValueLength = 0;
  private long lenOfBiggestCell = 0;
  private int maxTagsLength = 0;
  private long totalUncompressedBytes = 0;
  
  // Additional field added to support v4
  private int majorVersion = HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT;
  
  // Temporary path used for atomic writes
  private final Path tmpPath;
  
  /**
   * Creates a multi-tenant HFile writer that writes sections to a single file.
   * 
   * @param fs Filesystem to write to
   * @param path Path for the HFile
   * @param conf Configuration settings
   * @param cacheConf Cache configuration
   * @param tenantExtractor Extractor for tenant information
   * @param fileContext HFile context
   */
  public MultiTenantHFileWriter(
      FileSystem fs,
      Path path,
      Configuration conf,
      CacheConfig cacheConf,
      TenantExtractor tenantExtractor,
      HFileContext fileContext) throws IOException {
    // write into a .tmp file to allow atomic rename
    this.tmpPath = new Path(path.toString() + ".tmp");
    this.fs = fs;
    this.path = path;
    this.conf = conf;
    this.cacheConf = cacheConf;
    this.tenantExtractor = tenantExtractor;
    this.fileContext = fileContext;
    // create output stream on temp path
    this.outputStream = fs.create(tmpPath);
    // initialize blockWriter and sectionIndexWriter after creating stream
    initialize();
  }
  
  /**
   * Factory method to create a MultiTenantHFileWriter with configuration from both table and cluster levels.
   * 
   * @param fs Filesystem to write to
   * @param path Path for the HFile
   * @param conf Configuration settings that include cluster-level tenant configuration
   * @param cacheConf Cache configuration
   * @param tableProperties Table properties that may include table-level tenant configuration
   * @param fileContext HFile context
   * @return A configured MultiTenantHFileWriter
   */
  public static MultiTenantHFileWriter create(
      FileSystem fs,
      Path path,
      Configuration conf,
      CacheConfig cacheConf,
      Map<String, String> tableProperties,
      HFileContext fileContext) throws IOException {
    
    // Check if multi-tenant functionality is enabled for this table
    boolean multiTenantEnabled = true; // Default to enabled
    if (tableProperties != null && tableProperties.containsKey(TABLE_MULTI_TENANT_ENABLED)) {
      multiTenantEnabled = Boolean.parseBoolean(tableProperties.get(TABLE_MULTI_TENANT_ENABLED));
    }
    
    // Create tenant extractor using configuration and table properties
    TenantExtractor tenantExtractor;
    if (multiTenantEnabled) {
      // Normal multi-tenant operation: extract tenant prefix from row keys
      tenantExtractor = TenantExtractorFactory.createTenantExtractor(conf, tableProperties);
      LOG.info("Creating MultiTenantHFileWriter with multi-tenant functionality enabled");
    } else {
      // Single-tenant mode: always return the default tenant prefix regardless of cell
      tenantExtractor = new SingleTenantExtractor();
      LOG.info("Creating MultiTenantHFileWriter with multi-tenant functionality disabled " +
               "(all data will be written to a single section)");
    }
    
    // HFile version 4 inherently implies multi-tenant
    return new MultiTenantHFileWriter(fs, path, conf, cacheConf, tenantExtractor, fileContext);
  }
  
  private void initialize() throws IOException {
    // Initialize the block writer
    blockWriter = new HFileBlock.Writer(conf, 
                                        NoOpDataBlockEncoder.INSTANCE,
                                        fileContext, 
                                        cacheConf.getByteBuffAllocator(),
                                        conf.getInt(MAX_BLOCK_SIZE_UNCOMPRESSED, 
                                                   fileContext.getBlocksize() * 10));
    
    // Initialize the section index using SectionIndexManager
    boolean cacheIndexesOnWrite = cacheConf.shouldCacheIndexesOnWrite();
    String nameForCaching = cacheIndexesOnWrite ? path.getName() : null;
    
    sectionIndexWriter = new SectionIndexManager.Writer(
        blockWriter,
        cacheIndexesOnWrite ? cacheConf : null, 
        nameForCaching);
    
    // Configure multi-level tenant indexing based on configuration
    int maxChunkSize = conf.getInt(SectionIndexManager.SECTION_INDEX_MAX_CHUNK_SIZE, 
                                   SectionIndexManager.DEFAULT_MAX_CHUNK_SIZE);
    int minIndexNumEntries = conf.getInt(SectionIndexManager.SECTION_INDEX_MIN_NUM_ENTRIES,
                                        SectionIndexManager.DEFAULT_MIN_INDEX_NUM_ENTRIES);
    
    sectionIndexWriter.setMaxChunkSize(maxChunkSize);
    sectionIndexWriter.setMinIndexNumEntries(minIndexNumEntries);
    
    LOG.info("Initialized MultiTenantHFileWriter with multi-level section indexing for path: {} " +
             "(maxChunkSize={}, minIndexNumEntries={})", 
             path, maxChunkSize, minIndexNumEntries);
  }
  
  @Override
  public void append(ExtendedCell cell) throws IOException {
    if (cell == null) {
      throw new IOException("Cannot append null cell");
    }
    
    // Extract tenant prefix from the cell
    byte[] tenantPrefix = tenantExtractor.extractTenantPrefix(cell);
    
    // If this is the first cell or tenant has changed, switch to new section
    if (currentSectionWriter == null || !Arrays.equals(currentTenantPrefix, tenantPrefix)) {
      if (currentSectionWriter != null) {
        closeCurrentSection();
      }
      createNewSection(tenantPrefix);
    }
    
    // Write the cell to the current section
    currentSectionWriter.append(cell);
    
    // Track statistics for the entire file
    lastCell = cell; // Keep tracking for internal purposes
    entryCount++;
    totalKeyLength += PrivateCellUtil.estimatedSerializedSizeOfKey(cell);
    totalValueLength += cell.getValueLength();
    
    int cellSize = PrivateCellUtil.estimatedSerializedSizeOf(cell);
    if (lenOfBiggestCell < cellSize) {
      lenOfBiggestCell = cellSize;
    }
    
    int tagsLength = cell.getTagsLength();
    if (tagsLength > this.maxTagsLength) {
      this.maxTagsLength = tagsLength;
    }
  }
  
  private void closeCurrentSection() throws IOException {
    LOG.info("Closing section for tenant prefix: {}", 
        currentTenantPrefix == null ? "null" : Bytes.toStringBinary(currentTenantPrefix));
    
    // Record the section start position
    long sectionStartOffset = currentSectionWriter.getSectionStartOffset();
    
    // Finish writing the current section
    currentSectionWriter.close();
    //outputStream.hsync(); // Ensure section data (incl. trailer) is synced to disk
    
    // Get current position to calculate section size
    long sectionEndOffset = outputStream.getPos();
    long sectionSize = sectionEndOffset - sectionStartOffset;
    
    // Record section in the index
    sectionIndexWriter.addEntry(currentTenantPrefix, sectionStartOffset, (int)sectionSize);
    
    // Add to total uncompressed bytes
    totalUncompressedBytes += currentSectionWriter.getTotalUncompressedBytes();
    
    LOG.info("Section closed: start={}, size={}", sectionStartOffset, sectionSize);
  }
  
  private void createNewSection(byte[] tenantPrefix) throws IOException {
    // Set the start offset for this section
    sectionStartOffset = outputStream.getPos();
    
    // Create a new virtual section writer
    currentSectionWriter = new VirtualSectionWriter(
        conf, 
        cacheConf, 
        outputStream, 
        fileContext, 
        tenantPrefix, 
        sectionStartOffset);
    
    currentTenantPrefix = tenantPrefix;
    sectionCount++;
    
    LOG.info("Created new section writer for tenant prefix: {}, offset: {}", 
        tenantPrefix == null ? "null" : Bytes.toStringBinary(tenantPrefix), 
        sectionStartOffset);
  }
  
  @Override
  public void close() throws IOException {
    // Ensure all sections are closed and resources flushed
    if (currentSectionWriter != null) {
      closeCurrentSection();
      currentSectionWriter = null;
    }
    
    // Write indexes, file info, and trailer
    LOG.info("Writing section index");
    long sectionIndexOffset = sectionIndexWriter.writeIndexBlocks(outputStream);

    // Write a tenant-wide meta index block
    HFileBlockIndex.BlockIndexWriter metaBlockIndexWriter = new HFileBlockIndex.BlockIndexWriter();
    DataOutputStream dos = blockWriter.startWriting(BlockType.ROOT_INDEX);
    metaBlockIndexWriter.writeSingleLevelIndex(dos, "meta");
    blockWriter.writeHeaderAndData(outputStream);

    // Write file info
    LOG.info("Writing file info");
    FixedFileTrailer trailer = new FixedFileTrailer(getMajorVersion(), getMinorVersion());
    trailer.setFileInfoOffset(outputStream.getPos());

    // Add HFile metadata to the info block
    HFileInfo fileInfo = new HFileInfo();
    finishFileInfo(fileInfo);

    DataOutputStream out = blockWriter.startWriting(BlockType.FILE_INFO);
    fileInfo.write(out);
    blockWriter.writeHeaderAndData(outputStream);

    // Set up the trailer
    trailer.setLoadOnOpenOffset(sectionIndexOffset);
    trailer.setDataIndexCount(sectionIndexWriter.getNumRootEntries());
    trailer.setNumDataIndexLevels(sectionIndexWriter.getNumLevels());
    trailer.setUncompressedDataIndexSize(sectionIndexWriter.getTotalUncompressedSize());
    trailer.setComparatorClass(fileContext.getCellComparator().getClass());

    // Serialize the trailer
    trailer.serialize(outputStream);

    LOG.info("MultiTenantHFileWriter closed: path={}, sections={}", path, sectionCount);

    // close and cleanup resources
    try {
      outputStream.close();
      blockWriter.release();
      // atomically rename tmp -> final path
      fs.rename(tmpPath, path);
    } catch (IOException e) {
      // log rename or close failure
      LOG.error("Error closing MultiTenantHFileWriter, tmpPath={}, path={}", tmpPath, path, e);
      throw e;
    }
  }
  
  private void finishFileInfo(HFileInfo fileInfo) throws IOException {
    // Don't store the last key in global file info
    // This is intentionally removed to ensure we don't track first/last keys globally
    
    // Average key length
    int avgKeyLen = entryCount == 0 ? 0 : (int) (totalKeyLength / entryCount);
    fileInfo.append(HFileInfo.AVG_KEY_LEN, Bytes.toBytes(avgKeyLen), false);
    fileInfo.append(HFileInfo.CREATE_TIME_TS, Bytes.toBytes(fileContext.getFileCreateTime()), false);

    // Average value length
    int avgValueLength = entryCount == 0 ? 0 : (int) (totalValueLength / entryCount);
    fileInfo.append(HFileInfo.AVG_VALUE_LEN, Bytes.toBytes(avgValueLength), false);

    // Biggest cell info (key removed for tenant isolation)
    // Only store length which doesn't expose key information
    if (lenOfBiggestCell > 0) {
      fileInfo.append(HFileInfo.LEN_OF_BIGGEST_CELL, Bytes.toBytes(lenOfBiggestCell), false);
    }

    // Tags metadata
    if (fileContext.isIncludesTags()) {
      fileInfo.append(HFileInfo.MAX_TAGS_LEN, Bytes.toBytes(maxTagsLength), false);
      boolean tagsCompressed = (fileContext.getDataBlockEncoding() != DataBlockEncoding.NONE)
        && fileContext.isCompressTags();
      fileInfo.append(HFileInfo.TAGS_COMPRESSED, Bytes.toBytes(tagsCompressed), false);
    }
    
    // Section count information
    fileInfo.append(Bytes.toBytes("SECTION_COUNT"), Bytes.toBytes(sectionCount), false);
    
    // Add tenant index level information
    fileInfo.append(Bytes.toBytes("TENANT_INDEX_LEVELS"), 
                    Bytes.toBytes(sectionIndexWriter.getNumLevels()), false);
    if (sectionIndexWriter.getNumLevels() > 1) {
      fileInfo.append(Bytes.toBytes("TENANT_INDEX_MAX_CHUNK"), 
                     Bytes.toBytes(conf.getInt(SectionIndexManager.SECTION_INDEX_MAX_CHUNK_SIZE, 
                                              SectionIndexManager.DEFAULT_MAX_CHUNK_SIZE)), false);
    }
  }
  
  @Override
  public void appendFileInfo(byte[] key, byte[] value) throws IOException {
    if (currentSectionWriter != null) {
      currentSectionWriter.appendFileInfo(key, value);
    }
  }
  
  @Override
  public void appendMetaBlock(String metaBlockName, Writable content) {
    if (currentSectionWriter != null) {
      currentSectionWriter.appendMetaBlock(metaBlockName, content);
    }
  }
  
  @Override
  public void addInlineBlockWriter(InlineBlockWriter ibw) {
    if (currentSectionWriter != null) {
      currentSectionWriter.addInlineBlockWriter(ibw);
    }
  }
  
  @Override
  public void addGeneralBloomFilter(BloomFilterWriter bfw) {
    if (currentSectionWriter != null) {
      currentSectionWriter.addGeneralBloomFilter(bfw);
    }
  }
  
  @Override
  public void addDeleteFamilyBloomFilter(BloomFilterWriter bfw) {
    if (currentSectionWriter != null) {
      currentSectionWriter.addDeleteFamilyBloomFilter(bfw);
    }
  }
  
  @Override
  public void beforeShipped() throws IOException {
    if (currentSectionWriter != null) {
      currentSectionWriter.beforeShipped();
    }
    
    // Clone cells for thread safety if necessary
    if (this.lastCell != null) {
      this.lastCell = KeyValueUtil.toNewKeyCell((ExtendedCell)this.lastCell);
    }
  }
  
  @Override
  public Path getPath() {
    return path;
  }
  
  @Override
  public HFileContext getFileContext() {
    return fileContext;
  }
  
  public long getEntryCount() {
    return entryCount;
  }

  public Cell getLastCell() {
    return lastCell; // Keep API, but note this won't be used in global structures
  }
  
  /**
   * The multi-tenant HFile writer always returns version 4, which is the first version
   * to support multi-tenant HFiles.
   * 
   * @return The major version for multi-tenant HFiles (4)
   */
  protected int getMajorVersion() {
    return HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT;
  }
  
  /**
   * The minor version of HFile format.
   */
  protected int getMinorVersion() {
    return 0;
  }
  
  /**
   * Get the current number of tenant sections.
   * @return The section count
   */
  public int getSectionCount() {
    return sectionCount;
  }
  
  /**
   * A virtual writer for a tenant section within the HFile.
   * This handles writing data for a specific tenant section.
   */
  private class VirtualSectionWriter extends HFileWriterImpl {
    private final byte[] tenantPrefix;
    private final long sectionStartOffset;
    private boolean closed = false;
    
    // Track original stream when using relative position wrapper
    private FSDataOutputStream originalOutputStream = null;
    
    public VirtualSectionWriter(
        Configuration conf,
        CacheConfig cacheConf,
        FSDataOutputStream outputStream,
        HFileContext fileContext,
        byte[] tenantPrefix,
        long sectionStartOffset) throws IOException {
      // Call the parent constructor with the shared outputStream
      super(conf, cacheConf, null, outputStream, fileContext);
      
      this.tenantPrefix = tenantPrefix;
      this.sectionStartOffset = sectionStartOffset;
      
      // Add tenant information to the section's file info
      if (tenantPrefix != null) {
        appendFileInfo(Bytes.toBytes("TENANT_PREFIX"), tenantPrefix);
      }
      
      LOG.debug("Created section writer at offset {} for tenant {}", 
          sectionStartOffset, tenantPrefix == null ? "default" : Bytes.toStringBinary(tenantPrefix));
    }
    
    /**
     * Enable relative position translation by replacing the output stream with a wrapper
     */
    private void enableRelativePositionTranslation() {
      if (originalOutputStream != null) {
        return; // Already using a relative stream
      }
      
      // Store the original stream
      originalOutputStream = outputStream;
      final long baseOffset = sectionStartOffset;
      
      // Create a position-translating wrapper
      outputStream = new FSDataOutputStream(originalOutputStream.getWrappedStream(), null) {
        @Override
        public long getPos() {
          // Get absolute position
          long absolutePos = 0;
          try {
            absolutePos = originalOutputStream.getPos();
          } catch (Exception e) {
            LOG.error("Error getting position", e);
          }
          
          // Convert to position relative to section start
          return absolutePos - baseOffset;
        }
        
        @Override
        public void write(byte[] b, int off, int len) throws IOException {
          originalOutputStream.write(b, off, len);
        }
        
        @Override
        public void flush() throws IOException {
          originalOutputStream.flush();
        }
      };
    }
    
    /**
     * Restore the original output stream after using enableRelativePositionTranslation()
     */
    private void disableRelativePositionTranslation() {
      if (originalOutputStream != null) {
        outputStream = originalOutputStream;
        originalOutputStream = null;
      }
    }
    
    @Override
    public void append(ExtendedCell cell) throws IOException {
      checkNotClosed();
      
      // Use relative positions during append
      enableRelativePositionTranslation();
      
      try {
        super.append(cell);
      } finally {
        // Always restore original stream after operation
        disableRelativePositionTranslation();
      }
    }
    
    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      
      // Use relative positions during close
      enableRelativePositionTranslation();
      
      try {
        super.close();
        closed = true;
      } finally {
        // Always restore original stream after operation
        disableRelativePositionTranslation();
      }
      
      LOG.debug("Closed section for tenant: {}", 
          tenantPrefix == null ? "default" : Bytes.toStringBinary(tenantPrefix));
    }
    
    /**
     * Get the starting offset of this section in the file.
     * @return The section's starting offset
     */
    public long getSectionStartOffset() {
      return sectionStartOffset;
    }
    
    @Override
    public Path getPath() {
      // Return the parent file path
      return MultiTenantHFileWriter.this.path;
    }
    
    @Override
    public void appendFileInfo(byte[] key, byte[] value) throws IOException {
      checkNotClosed();
      
      if (originalOutputStream == null) {
        enableRelativePositionTranslation();
      }
      
      try {
        super.appendFileInfo(key, value);
      } finally {
        disableRelativePositionTranslation();
      }
    }
    
    @Override
    public void appendMetaBlock(String metaBlockName, Writable content) {
      checkNotClosed();
      
      if (originalOutputStream == null) {
        enableRelativePositionTranslation();
      }
      
      try {
        super.appendMetaBlock(metaBlockName, content);
      } finally {
        disableRelativePositionTranslation();
      }
    }
    
    @Override
    public void addInlineBlockWriter(InlineBlockWriter ibw) {
      checkNotClosed();
      
      if (originalOutputStream == null) {
        enableRelativePositionTranslation();
      }
      
      try {
        super.addInlineBlockWriter(ibw);
      } finally {
        disableRelativePositionTranslation();
      }
    }
    
    @Override
    public void addGeneralBloomFilter(BloomFilterWriter bfw) {
      checkNotClosed();
      
      if (originalOutputStream == null) {
        enableRelativePositionTranslation();
      }
      
      try {
        super.addGeneralBloomFilter(bfw);
      } finally {
        disableRelativePositionTranslation();
      }
    }
    
    @Override
    public void addDeleteFamilyBloomFilter(BloomFilterWriter bfw) {
      checkNotClosed();
      
      if (originalOutputStream == null) {
        enableRelativePositionTranslation();
      }
      
      try {
        super.addDeleteFamilyBloomFilter(bfw);
      } finally {
        disableRelativePositionTranslation();
      }
    }
    
    @Override
    public void beforeShipped() throws IOException {
      checkNotClosed();
      
      if (originalOutputStream == null) {
        enableRelativePositionTranslation();
      }
      
      try {
        super.beforeShipped();
      } finally {
        disableRelativePositionTranslation();
      }
    }
    
    private void checkNotClosed() {
      if (closed) {
        throw new IllegalStateException("Section writer already closed");
      }
    }
    
    // Override protected methods to make version 3 for each section
    @Override
    protected int getMajorVersion() {
      return 3; // Each section uses version 3 format
    }
    
    public long getTotalUncompressedBytes() {
      return this.totalUncompressedBytes;
    }
  }
  
  /**
   * An implementation of TenantExtractor that always returns the default tenant prefix.
   * Used when multi-tenant functionality is disabled via the TABLE_MULTI_TENANT_ENABLED property.
   */
  private static class SingleTenantExtractor implements TenantExtractor {
    @Override
    public byte[] extractTenantPrefix(Cell cell) {
      return DEFAULT_TENANT_PREFIX;
    }
  }
  
  /*
   * Tenant Identification Configuration Hierarchy
   * --------------------------------------------
   * 
   * The tenant configuration follows this precedence order:
   * 
   * 1. Table Level Configuration (highest precedence)
   *    - Property: TENANT_PREFIX_LENGTH 
   *      Table-specific tenant prefix length
   *    - Property: TENANT_PREFIX_OFFSET
   *      Byte offset for tenant prefix extraction (default: 0)
   *    - Property: MULTI_TENANT_HFILE
   *      Boolean flag indicating if this table uses multi-tenant sectioning (default: true)
   * 
   * 2. Cluster Level Configuration (used as fallback)
   *    - Property: hbase.multi.tenant.prefix.extractor.class
   *      Defines the implementation class for TenantExtractor
   *    - Property: hbase.multi.tenant.prefix.length
   *      Default prefix length if using fixed-length prefixes
   *    - Property: hbase.multi.tenant.prefix.offset
   *      Default prefix offset if using fixed-length prefixes
   * 
   * 3. Default Values (used if neither above is specified)
   *    - Default prefix length: 0 bytes
   *    - Default prefix offset: 0 bytes
   * 
   * When creating a MultiTenantHFileWriter, the system will:
   * 1. First check table properties for tenant configuration
   * 2. If not found, use cluster-wide configuration from hbase-site.xml
   * 3. If neither is specified, fall back to default values
   * 
   * Important notes:
   * - HFile version 4 inherently implies multi-tenancy 
   * - Tenant configuration is obtained only from cluster configuration and table properties
   * - HFileContext does not contain any tenant-specific configuration
   * - The TenantExtractor is created directly from the configuration parameters
   * 
   * This design ensures:
   * - Tables can override the cluster-wide tenant configuration
   * - Each table can have its own tenant prefix configuration
   * - Tenant configuration is separate from the low-level file format concerns
   * - Sensible defaults are used if no explicit configuration is provided
   */

  /**
   * Creates a specialized writer factory for multi-tenant HFiles format version 4
   */
  public static class WriterFactory extends HFile.WriterFactory {
    // Maintain our own copy of the file context
    private HFileContext writerFileContext;
    
    public WriterFactory(Configuration conf, CacheConfig cacheConf) {
      super(conf, cacheConf);
    }
    
    @Override
    public HFile.WriterFactory withFileContext(HFileContext fileContext) {
      this.writerFileContext = fileContext;
      return super.withFileContext(fileContext);
    }
    
    @Override
    public HFile.Writer create() throws IOException {
      if ((path != null ? 1 : 0) + (ostream != null ? 1 : 0) != 1) {
        throw new AssertionError("Please specify exactly one of filesystem/path or path");
      }
      
      if (path != null) {
        ostream = HFileWriterImpl.createOutputStream(conf, fs, path, favoredNodes);
        try {
          ostream.setDropBehind(shouldDropBehind && cacheConf.shouldDropBehindCompaction());
        } catch (UnsupportedOperationException uoe) {
          LOG.trace("Unable to set drop behind on {}", path, uoe);
          LOG.debug("Unable to set drop behind on {}", path.getName());
        }
      }
      
      // Extract table properties for tenant configuration from table descriptor
      Map<String, String> tableProperties = new java.util.HashMap<>();
      
      // Get the table descriptor if available
      TableDescriptor tableDesc = getTableDescriptor(writerFileContext);
      if (tableDesc != null) {
        // Extract relevant properties for multi-tenant configuration
        // More properties can be added here as needed
        for (Entry<Bytes, Bytes> entry : tableDesc.getValues().entrySet()) {
          String key = Bytes.toString(entry.getKey().get());
          tableProperties.put(key, Bytes.toString(entry.getValue().get()));
        }
        LOG.debug("Creating MultiTenantHFileWriter with table properties from descriptor for table: {}", 
                  tableDesc.getTableName());
      } else {
        LOG.debug("Creating MultiTenantHFileWriter with default properties (no table descriptor available)");
      }
      
      // Create the writer using the factory method
      return MultiTenantHFileWriter.create(fs, path, conf, cacheConf, tableProperties, writerFileContext);
    }
    
    /**
     * Get the table descriptor from the HFile context if available
     * @param fileContext The HFile context potentially containing a table name
     * @return The table descriptor or null if not available
     */
    private TableDescriptor getTableDescriptor(HFileContext fileContext) {
      try {
        // If file context or table name is not available, return null
        if (fileContext == null || fileContext.getTableName() == null) {
          LOG.debug("Table name not available in HFileContext");
          return null;
        }
        
        // Get the table descriptor from the Admin API
        TableName tableName = TableName.valueOf(fileContext.getTableName());
        try (Connection conn = ConnectionFactory.createConnection(conf);
             Admin admin = conn.getAdmin()) {
          return admin.getDescriptor(tableName);
        } catch (Exception e) {
          LOG.warn("Failed to get table descriptor using Admin API for {}", tableName, e);
          return null;
        }
      } catch (Exception e) {
        LOG.warn("Error getting table descriptor", e);
        return null;
      }
    }
  }
}
