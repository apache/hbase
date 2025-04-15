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

/**
 * CHANGES REQUIRED IN HFileContext.java:
 * 1. Remove tenant-specific fields:
 *    - private int pbePrefixLength;
 *    - private int prefixOffset;
 *    - private boolean isMultiTenant;
 * 
 * 2. Remove tenant-specific getter methods:
 *    - getPbePrefixLength()
 *    - getPrefixOffset()
 *    - isMultiTenant()
 * 
 * 3. Remove tenant-specific parameters in constructors:
 *    a. Copy constructor should not copy tenant fields
 *    b. Remove constructor with multi-tenant parameters: HFileContext(..., pbePrefixLength, prefixOffset, isMultiTenant)
 * 
 * 4. Update toString() to remove tenant-specific fields
 * 
 * CHANGES REQUIRED IN HFileContextBuilder.java:
 * 1. Remove tenant-specific fields:
 *    - private int pbePrefixLength = 0;
 *    - private int prefixOffset = 0;
 *    - private boolean isMultiTenant = false;
 * 
 * 2. Remove tenant-specific builder methods:
 *    - withPbePrefixLength(int pbePrefixLength)
 *    - withPrefixOffset(int prefixOffset)
 *    - withMultiTenant(boolean isMultiTenant)
 * 
 * 3. Simplify build() method by removing the check for isMultiTenant
 * 4. Remove buildWithMultiTenant() method entirely
 * 
 * REASON FOR CHANGES:
 * Tenant configuration should be completely separate from file format concerns.
 * HFileContext should only handle format-specific details, while tenant configuration
 * is managed by TenantExtractorFactory using cluster configuration and table properties.
 * The HFile version (v4) inherently implies multi-tenant support without needing additional flags.
 */

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ByteBufferExtendedCell;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.hadoop.hbase.io.hfile.HFile.WriterFactory;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public static final String TENANT_PREFIX_EXTRACTOR_CLASS = "hbase.multi.tenant.prefix.extractor.class";
  public static final String TENANT_PREFIX_LENGTH = "hbase.multi.tenant.prefix.length";
  public static final String TENANT_PREFIX_OFFSET = "hbase.multi.tenant.prefix.offset";
  
  // Tenant identification configuration at table level (higher precedence)
  public static final String TABLE_TENANT_PREFIX_LENGTH = "TENANT_PREFIX_LENGTH";
  public static final String TABLE_TENANT_PREFIX_OFFSET = "TENANT_PREFIX_OFFSET";
  
  // Default values
  private static final int DEFAULT_PREFIX_LENGTH = 4;
  private static final int DEFAULT_PREFIX_OFFSET = 0;
  
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
  private HFileBlockIndex.BlockIndexWriter sectionIndexWriter;
  
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
  private byte[] keyOfBiggestCell;
  private int maxTagsLength = 0;
  private long totalUncompressedBytes = 0;
  
  // Store the last key to ensure the keys are in sorted order.
  private byte[] lastKeyBuffer = null;
  private int lastKeyOffset;
  private int lastKeyLength;

  private volatile boolean closed = false;
  
  // Additional field added to support v4
  private int majorVersion = HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT;
  
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
    this.fs = fs;
    this.path = path;
    this.conf = conf;
    this.cacheConf = cacheConf;
    this.tenantExtractor = tenantExtractor;
    this.fileContext = fileContext;
    
    // Create the output stream
    this.outputStream = fs.create(path);
    
    // Initialize components
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
    
    // Create tenant extractor using configuration and table properties
    // without relying on HFileContext for tenant-specific configuration
    TenantExtractor tenantExtractor = TenantExtractorFactory.createTenantExtractor(conf, tableProperties);
    
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
    
    // Initialize the section index
    boolean cacheIndexesOnWrite = cacheConf.shouldCacheIndexesOnWrite();
    String nameForCaching = cacheIndexesOnWrite ? path.getName() : null;
    
    sectionIndexWriter = new HFileBlockIndex.BlockIndexWriter(
        blockWriter,
        cacheIndexesOnWrite ? cacheConf : null, 
        nameForCaching,
        NoOpIndexBlockEncoder.INSTANCE);
    
    // Initialize tracking
    this.sectionStartOffset = 0;
    
    LOG.info("Initialized MultiTenantHFileWriter for path: {}", path);
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
      keyOfBiggestCell = PrivateCellUtil.getCellKeySerializedAsKeyValueKey((ExtendedCell)cell);
    }
    
    int tagsLength = cell.getTagsLength();
    if (tagsLength > this.maxTagsLength) {
      this.maxTagsLength = tagsLength;
    }
  }
  
  private void closeCurrentSection() throws IOException {
    LOG.info("Closing section for tenant prefix: {}", 
        currentTenantPrefix == null ? "null" : Bytes.toStringBinary(currentTenantPrefix));
    
    // Finish writing the current section
    currentSectionWriter.close();
    
    // Add to total uncompressed bytes
    totalUncompressedBytes += currentSectionWriter.getTotalUncompressedBytes();
    
    // Record section in the index
    long sectionEndOffset = outputStream.getPos();
    int sectionSize = (int)(sectionEndOffset - sectionStartOffset);
    
    sectionIndexWriter.addEntry(currentTenantPrefix, sectionStartOffset, sectionSize);
    
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
    if (outputStream == null) {
      return;
    }
    
    if (currentSectionWriter != null) {
      closeCurrentSection();
      currentSectionWriter = null;
    }
    
    // Write the section index
    LOG.info("Writing section index");
    long sectionIndexOffset = sectionIndexWriter.writeIndexBlocks(outputStream);
    
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
    
    // Close the output stream
    outputStream.close();
    blockWriter.release();
    
    LOG.info("MultiTenantHFileWriter closed: path={}, sections={}", path, sectionCount);
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
  public Path getPath() {
    return path;
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
  public HFileContext getFileContext() {
    return fileContext;
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
    
    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      
      // Call the parent close method, which will write all necessary blocks
      // for a complete HFile v3 section including:
      // - Meta blocks
      // - File info
      // - Data block index
      // - Meta block index
      // - Fixed file trailer
      super.close();
      closed = true;
      
      LOG.debug("Closed section for tenant: {}", 
          tenantPrefix == null ? "default" : Bytes.toStringBinary(tenantPrefix));
    }
    
    @Override
    public Path getPath() {
      // Return the parent file path
      return MultiTenantHFileWriter.this.path;
    }
    
    @Override
    public void append(ExtendedCell cell) throws IOException {
      checkNotClosed();
      super.append(cell);
    }
    
    @Override
    public void appendFileInfo(byte[] key, byte[] value) throws IOException {
      checkNotClosed();
      super.appendFileInfo(key, value);
    }
    
    @Override
    public void appendMetaBlock(String metaBlockName, Writable content) {
      checkNotClosed();
      super.appendMetaBlock(metaBlockName, content);
    }
    
    @Override
    public void addInlineBlockWriter(InlineBlockWriter ibw) {
      checkNotClosed();
      super.addInlineBlockWriter(ibw);
    }
    
    @Override
    public void addGeneralBloomFilter(BloomFilterWriter bfw) {
      checkNotClosed();
      super.addGeneralBloomFilter(bfw);
    }
    
    @Override
    public void addDeleteFamilyBloomFilter(BloomFilterWriter bfw) {
      checkNotClosed();
      super.addDeleteFamilyBloomFilter(bfw);
    }
    
    @Override
    public void beforeShipped() throws IOException {
      checkNotClosed();
      super.beforeShipped();
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
   *    - Default prefix length: 4 bytes
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
   * 
   * SUMMARY OF CHANGES NEEDED:
   * --------------------------
   * 
   * 1. In HFileContext.java:
   *    - Remove fields: pbePrefixLength, prefixOffset, isMultiTenant
   *    - Remove methods: getPbePrefixLength(), getPrefixOffset(), isMultiTenant()
   *    - Remove constructor with tenant parameters
   *    - Update copy constructor and toString() to remove tenant fields
   * 
   * 2. In HFileContextBuilder.java:
   *    - Remove fields: pbePrefixLength, prefixOffset, isMultiTenant
   *    - Remove methods: withPbePrefixLength(), withPrefixOffset(), withMultiTenant()
   *    - Remove buildWithMultiTenant() method
   *    - Simplify build() method to not check isMultiTenant
   * 
   * 3. In TenantExtractorFactory.java:
   *    - Remove method that takes HFileContext
   *    - Focus only on method that takes Configuration and table properties
   * 
   * 4. In MultiTenantHFileWriter.java:
   *    - Use TenantExtractorFactory directly
   *    - Remove TenantConfiguration class
   *    - Remove any dependency on HFileContext for tenant configuration
   * 
   * 5. In HFile.java:
   *    - Update MultiTenantWriterFactory to get tenant configuration from 
   *      TenantExtractorFactory, not from HFileContext
   * 
   * These changes ensure a clean separation between file format concerns (handled by 
   * HFileContext) and tenant configuration (handled by TenantExtractorFactory).
   */
}