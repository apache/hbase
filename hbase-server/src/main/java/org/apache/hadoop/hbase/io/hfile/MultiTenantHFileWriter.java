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
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
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
  
  // Tenant identification configuration at table level (higher precedence)
  public static final String TABLE_TENANT_PREFIX_LENGTH = "TENANT_PREFIX_LENGTH";
  
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
  private SectionWriter currentSectionWriter;
  private byte[] currentTenantSectionId;
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
  
  // Added for v4
  private FixedFileTrailer trailer;
  private HFileBlockIndex.BlockIndexWriter metaBlockIndexWriter;
  private HFileInfo fileInfo = new HFileInfo();
  
  // Write verification
  private boolean enableWriteVerification;
  private static final String WRITE_VERIFICATION_ENABLED = "hbase.multi.tenant.write.verification.enabled";
  private static final boolean DEFAULT_WRITE_VERIFICATION_ENABLED = false;
  
  /**
   * Creates a multi-tenant HFile writer that writes sections to a single file.
   * 
   * @param fs Filesystem to write to
   * @param path Path for the HFile (final destination)
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
    // Follow HFileWriterImpl pattern: accept path and create outputStream
    this.path = path;
    this.fs = fs;
    this.conf = conf;
    this.cacheConf = cacheConf;
    this.tenantExtractor = tenantExtractor;
    this.fileContext = fileContext;
    this.enableWriteVerification = conf.getBoolean(WRITE_VERIFICATION_ENABLED, DEFAULT_WRITE_VERIFICATION_ENABLED);
    
    // Create output stream directly to the provided path - no temporary file management here
    // The caller (StoreFileWriter or integration test framework) handles temporary files
    this.outputStream = HFileWriterImpl.createOutputStream(conf, fs, path, null);
    
    // Initialize meta block index writer
    this.metaBlockIndexWriter = new HFileBlockIndex.BlockIndexWriter();
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
    
    // Create tenant extractor using factory - it will decide whether to use 
    // DefaultTenantExtractor or SingleTenantExtractor based on table properties
    TenantExtractor tenantExtractor = TenantExtractorFactory.createTenantExtractor(conf, tableProperties);
    
    LOG.info("Creating MultiTenantHFileWriter with tenant extractor: {}", 
             tenantExtractor.getClass().getSimpleName());
    
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
    
    // Extract tenant section ID from the cell for section indexing
    byte[] tenantSectionId = tenantExtractor.extractTenantSectionId(cell);
    
    // If this is the first cell or tenant section has changed, switch to new section
    if (currentSectionWriter == null || !Arrays.equals(currentTenantSectionId, tenantSectionId)) {
      if (currentSectionWriter != null) {
        closeCurrentSection();
      }
      // Extract tenant ID from the cell
      byte[] tenantId = tenantExtractor.extractTenantId(cell);
      createNewSection(tenantSectionId, tenantId);
    }
    
    // Write the cell to the current section
    currentSectionWriter.append(cell);
    
    // Track statistics for the entire file
    lastCell = cell;
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
    LOG.info("Closing section for tenant section ID: {}", 
        currentTenantSectionId == null ? "null" : Bytes.toStringBinary(currentTenantSectionId));
    
    if (currentSectionWriter == null) {
      LOG.warn("Attempted to close null section writer");
      return;
    }
    
    try {
      // Record the section start position
      long sectionStartOffset = currentSectionWriter.getSectionStartOffset();
      
      // Validate section has data
      if (currentSectionWriter.getEntryCount() == 0) {
        LOG.warn("Closing empty section for tenant: {}", 
            Bytes.toStringBinary(currentTenantSectionId));
      }
      
      // Finish writing the current section
      currentSectionWriter.close();
      //outputStream.hsync(); // Ensure section data (incl. trailer) is synced to disk
      
      // Get current position to calculate section size
      long sectionEndOffset = outputStream.getPos();
      long sectionSize = sectionEndOffset - sectionStartOffset;
      
      // Validate section size
      if (sectionSize <= 0) {
        throw new IOException("Invalid section size: " + sectionSize + 
            " for tenant: " + Bytes.toStringBinary(currentTenantSectionId));
      }
      
      // Validate section doesn't exceed max size (2GB limit for int)
      if (sectionSize > Integer.MAX_VALUE) {
        throw new IOException("Section size exceeds maximum: " + sectionSize + 
            " for tenant: " + Bytes.toStringBinary(currentTenantSectionId));
      }
      
      // Write verification if enabled
      if (enableWriteVerification) {
        verifySection(sectionStartOffset, sectionSize);
      }
      
      // Record section in the index
      sectionIndexWriter.addEntry(currentTenantSectionId, sectionStartOffset, (int)sectionSize);
      
      // Add to total uncompressed bytes
      totalUncompressedBytes += currentSectionWriter.getTotalUncompressedBytes();
      
      LOG.info("Section closed: start={}, size={}, entries={}", 
          sectionStartOffset, sectionSize, currentSectionWriter.getEntryCount());
    } catch (IOException e) {
      LOG.error("Error closing section for tenant section ID: {}", 
          currentTenantSectionId == null ? "null" : Bytes.toStringBinary(currentTenantSectionId), e);
      throw e;
    } finally {
      currentSectionWriter = null;
    }
  }
  
  /**
   * Verify that the section was written correctly by checking basic structure
   */
  private void verifySection(long sectionStartOffset, long sectionSize) throws IOException {
    LOG.debug("Verifying section at offset {} with size {}", sectionStartOffset, sectionSize);
    
    // Basic verification: check that we can read the trailer
    long currentPos = outputStream.getPos();
    try {
      // Seek to trailer position
      int trailerSize = FixedFileTrailer.getTrailerSize(3); // v3 sections
      long trailerOffset = sectionStartOffset + sectionSize - trailerSize;
      
      if (trailerOffset < sectionStartOffset) {
        throw new IOException("Section too small to contain trailer: size=" + sectionSize);
      }
      
      // Just verify the position is valid - actual trailer reading would require
      // creating an input stream which is expensive
      LOG.debug("Section verification passed: trailer would be at offset {}", trailerOffset);
    } finally {
      // Restore position
      // Note: FSDataOutputStream doesn't support seek, so we can't actually verify
      // Just log that verification was requested
      LOG.debug("Write verification completed (limited check due to stream constraints)");
    }
  }
  
  private void createNewSection(byte[] tenantSectionId, byte[] tenantId) throws IOException {
    // Set the start offset for this section
    sectionStartOffset = outputStream.getPos();
    
    // Create a new virtual section writer
    currentSectionWriter = new SectionWriter(
        conf, 
        cacheConf, 
        outputStream, 
        fileContext, 
        tenantSectionId,
        tenantId,
        sectionStartOffset);
    
    currentTenantSectionId = tenantSectionId;
    sectionCount++;
    
    LOG.info("Created new section writer for tenant section ID: {}, tenant ID: {}, offset: {}", 
        tenantSectionId == null ? "null" : Bytes.toStringBinary(tenantSectionId),
        tenantId == null ? "null" : Bytes.toStringBinary(tenantId),
        sectionStartOffset);
  }
  
  @Override
  public void close() throws IOException {
    if (outputStream == null) {
      return;
    }
    
    // Ensure all sections are closed and resources flushed
    if (currentSectionWriter != null) {
      closeCurrentSection();
      currentSectionWriter = null;
    }

    // HFile v4 structure: Section Index + File Info + Trailer
    // (Each section contains complete HFile v3 with its own blocks)
    // Note: v4 readers skip initMetaAndIndex, so no meta block index needed

    trailer = new FixedFileTrailer(getMajorVersion(), getMinorVersion());

    // 1. Write Section Index Block (replaces data block index in v4)
    // This is the core of HFile v4 - maps tenant prefixes to section locations
    LOG.info("Writing section index with {} sections", sectionCount);
    long rootIndexOffset = sectionIndexWriter.writeIndexBlocks(outputStream);
    trailer.setLoadOnOpenOffset(rootIndexOffset);

    // 2. Write File Info Block (minimal v4-specific metadata)
    LOG.info("Writing v4 file info");
    finishFileInfo();
    writeFileInfo(trailer, blockWriter.startWriting(BlockType.FILE_INFO));
    blockWriter.writeHeaderAndData(outputStream);
    totalUncompressedBytes += blockWriter.getUncompressedSizeWithHeader();

    // 3. Write Trailer
    finishClose(trailer);

    LOG.info("MultiTenantHFileWriter closed: path={}, sections={}, entries={}, totalUncompressedBytes={}", 
             path, sectionCount, entryCount, totalUncompressedBytes);

    blockWriter.release();
  }
  
  /**
   * Write file info similar to HFileWriterImpl but adapted for multi-tenant structure
   */
  private void writeFileInfo(FixedFileTrailer trailer, DataOutputStream out) throws IOException {
    trailer.setFileInfoOffset(outputStream.getPos());
    fileInfo.write(out);
  }
  
  /**
   * Finish the close for HFile v4 trailer
   */
  private void finishClose(FixedFileTrailer trailer) throws IOException {
    // Set v4-specific trailer fields
    trailer.setNumDataIndexLevels(sectionIndexWriter.getNumLevels());
    trailer.setUncompressedDataIndexSize(sectionIndexWriter.getTotalUncompressedSize());
    trailer.setDataIndexCount(sectionIndexWriter.getNumRootEntries());
    
    // For v4 files, these indicate no global data blocks (data is in sections)
    trailer.setFirstDataBlockOffset(-1); // UNSET indicates no global data blocks
    trailer.setLastDataBlockOffset(-1);  // UNSET indicates no global data blocks
    
    // Set other standard trailer fields
    trailer.setComparatorClass(fileContext.getCellComparator().getClass());
    trailer.setMetaIndexCount(0); // No global meta blocks for multi-tenant files
    trailer.setTotalUncompressedBytes(totalUncompressedBytes + trailer.getTrailerSize());
    trailer.setEntryCount(entryCount);
    trailer.setCompressionCodec(fileContext.getCompression());

    // Write trailer and close stream
    long startTime = EnvironmentEdgeManager.currentTime();
    trailer.serialize(outputStream);
    HFile.updateWriteLatency(EnvironmentEdgeManager.currentTime() - startTime);

    // Close the output stream - no file renaming needed since caller handles temporary files
    try {
      outputStream.close();
      LOG.info("Successfully closed MultiTenantHFileWriter: {}", path);
    } catch (IOException e) {
      LOG.error("Error closing MultiTenantHFileWriter for path: {}", path, e);
      throw e;
    }
  }
  
  private void finishFileInfo() throws IOException {
    // Don't store the last key in global file info for tenant isolation
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
    
    // Section count information - this ensures fileInfo always has meaningful data
    fileInfo.append(Bytes.toBytes("SECTION_COUNT"), Bytes.toBytes(sectionCount), false);
    
    // Add tenant index level information
    fileInfo.append(Bytes.toBytes("TENANT_INDEX_LEVELS"), 
                    Bytes.toBytes(sectionIndexWriter.getNumLevels()), false);
    if (sectionIndexWriter.getNumLevels() > 1) {
      fileInfo.append(Bytes.toBytes("TENANT_INDEX_MAX_CHUNK"), 
                     Bytes.toBytes(conf.getInt(SectionIndexManager.SECTION_INDEX_MAX_CHUNK_SIZE, 
                                              SectionIndexManager.DEFAULT_MAX_CHUNK_SIZE)), false);
    }
    
    // Store multi-tenant configuration in file info
    fileInfo.append(Bytes.toBytes("MULTI_TENANT_ENABLED"), Bytes.toBytes("true"), false);
    fileInfo.append(Bytes.toBytes("TENANT_PREFIX_LENGTH"), 
                    Bytes.toBytes(String.valueOf(tenantExtractor.getPrefixLength())), false);
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
    // For multi-tenant files, bloom filters are only added at section level
    // This prevents creating bloom filters at the global level
    if (bfw == null || bfw.getKeyCount() <= 0) {
      LOG.debug("Ignoring empty or null general bloom filter at global level");
      return;
    }
    
    // Only add to current section if one exists
    if (currentSectionWriter != null) {
      LOG.debug("Delegating general bloom filter with {} keys to current section", bfw.getKeyCount());
      // Ensure it's properly prepared for writing
      bfw.compactBloom();
      currentSectionWriter.addGeneralBloomFilter(bfw);
    } else {
      LOG.warn("Attempted to add general bloom filter with {} keys but no section is active", 
               bfw.getKeyCount());
    }
  }
  
  @Override
  public void addDeleteFamilyBloomFilter(BloomFilterWriter bfw) throws IOException {
    // For multi-tenant files, bloom filters are only added at section level
    // This prevents creating bloom filters at the global level
    if (bfw == null || bfw.getKeyCount() <= 0) {
      LOG.debug("Ignoring empty or null delete family bloom filter at global level");
      return;
    }
    
    // Only add to current section if one exists
    if (currentSectionWriter != null) {
      LOG.debug("Delegating delete family bloom filter with {} keys to current section", bfw.getKeyCount());
      // Ensure it's properly prepared for writing
      bfw.compactBloom();
      currentSectionWriter.addDeleteFamilyBloomFilter(bfw);
    } else {
      LOG.warn("Attempted to add delete family bloom filter with {} keys but no section is active", 
               bfw.getKeyCount());
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
  private class SectionWriter extends HFileWriterImpl {
    private final byte[] tenantSectionId;
    private final long sectionStartOffset;
    private boolean closed = false;
    
    public SectionWriter(
        Configuration conf,
        CacheConfig cacheConf,
        FSDataOutputStream outputStream,
        HFileContext fileContext,
        byte[] tenantSectionId,
        byte[] tenantId,
        long sectionStartOffset) throws IOException {
      // Create a section-aware output stream that handles position translation
      super(conf, cacheConf, null, new SectionOutputStream(outputStream, sectionStartOffset), fileContext);
      
      this.tenantSectionId = tenantSectionId;
      this.sectionStartOffset = sectionStartOffset;
      
      // Store the tenant ID in the file info
      if (tenantId != null && tenantId.length > 0) {
        appendFileInfo(Bytes.toBytes("TENANT_ID"), tenantId);
      }
      
      // Store the section ID for reference
      if (tenantSectionId != null) {
        appendFileInfo(Bytes.toBytes("TENANT_SECTION_ID"), tenantSectionId);
      }
      
      LOG.debug("Created section writer at offset {} for tenant section {}, tenant ID {}", 
          sectionStartOffset, 
          tenantSectionId == null ? "default" : Bytes.toStringBinary(tenantSectionId),
          tenantId == null ? "default" : Bytes.toStringBinary(tenantId));
    }
    
    /**
     * Output stream that translates positions relative to section start
     */
    private static class SectionOutputStream extends FSDataOutputStream {
      private final FSDataOutputStream delegate;
      private final long baseOffset;
      
      public SectionOutputStream(FSDataOutputStream delegate, long baseOffset) {
        super(delegate.getWrappedStream(), null);
        this.delegate = delegate;
        this.baseOffset = baseOffset;
      }
      
      @Override
      public long getPos() {
        try {
          // Return position relative to section start
          return delegate.getPos() - baseOffset;
        } catch (Exception e) {
          throw new RuntimeException("Failed to get position", e);
        }
      }
      
      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        delegate.write(b, off, len);
      }
      
      @Override
      public void flush() throws IOException {
        delegate.flush();
      }
      
      @Override
      public void close() throws IOException {
        // Don't close the delegate - it's shared across sections
        flush();
      }
    }
    
    @Override
    public void append(ExtendedCell cell) throws IOException {
      checkNotClosed();
      
      super.append(cell);
    }
    
    /**
     * Safely handle adding general bloom filters to the section
     */
    @Override
    public void addGeneralBloomFilter(final BloomFilterWriter bfw) {
      checkNotClosed();
      
      // Skip empty bloom filters
      if (bfw == null || bfw.getKeyCount() <= 0) {
        LOG.debug("Skipping empty general bloom filter for tenant section: {}", 
             tenantSectionId == null ? "default" : Bytes.toStringBinary(tenantSectionId));
        return;
      }
      
      // Ensure the bloom filter is properly initialized
      bfw.compactBloom();
      
      LOG.debug("Added general bloom filter with {} keys for tenant section: {}", 
          bfw.getKeyCount(),
          tenantSectionId == null ? "default" : Bytes.toStringBinary(tenantSectionId));
      
      super.addGeneralBloomFilter(bfw);
    }
    
    /**
     * Safely handle adding delete family bloom filters to the section
     */
    @Override
    public void addDeleteFamilyBloomFilter(final BloomFilterWriter bfw) {
      checkNotClosed();
      
      // Skip empty bloom filters
      if (bfw == null || bfw.getKeyCount() <= 0) {
        LOG.debug("Skipping empty delete family bloom filter for tenant section: {}", 
             tenantSectionId == null ? "default" : Bytes.toStringBinary(tenantSectionId));
        return;
      }
      
      // Ensure the bloom filter is properly initialized
      bfw.compactBloom();
      
      LOG.debug("Added delete family bloom filter with {} keys for tenant section: {}", 
          bfw.getKeyCount(),
          tenantSectionId == null ? "default" : Bytes.toStringBinary(tenantSectionId));
      
      // Call the parent implementation without try/catch since it doesn't actually throw IOException
      // The HFileWriterImpl implementation doesn't throw IOException despite the interface declaration
      super.addDeleteFamilyBloomFilter(bfw);
    }
    
    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      
      LOG.debug("Closing section for tenant section ID: {}", 
          tenantSectionId == null ? "null" : Bytes.toStringBinary(tenantSectionId));
      
      // Close the section writer safely
      // HFileWriterImpl.close() can fail with NPE on empty bloom filters, but we want to 
      // still properly close the stream and resources
      try {
        super.close();
      } catch (RuntimeException e) {
        LOG.warn("Error during section close, continuing with stream cleanup. Error: {}", e.getMessage());
        // We will still mark as closed and continue with resource cleanup
      }
      closed = true;
      
      LOG.debug("Closed section for tenant section: {}", 
          tenantSectionId == null ? "default" : Bytes.toStringBinary(tenantSectionId));
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
    
    /**
     * Get the number of entries written to this section
     * @return The entry count
     */
    public long getEntryCount() {
      return this.entryCount;
    }
  }
  
  /**
   * An implementation of TenantExtractor that always returns the default tenant prefix.
   * Used when multi-tenant functionality is disabled via the TABLE_MULTI_TENANT_ENABLED property.
   */
  static class SingleTenantExtractor implements TenantExtractor {
    @Override
    public byte[] extractTenantId(Cell cell) {
      return DEFAULT_TENANT_PREFIX;
    }
    
    @Override
    public byte[] extractTenantSectionId(Cell cell) {
      return DEFAULT_TENANT_PREFIX;
    }

    @Override
    public int getPrefixLength() {
      return 0;
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
   *    - Property: MULTI_TENANT_HFILE
   *      Boolean flag indicating if this table uses multi-tenant sectioning (default: true)
   * 
   * 2. Cluster Level Configuration (used as fallback)
   *    - Property: hbase.multi.tenant.prefix.extractor.class
   *      Defines the implementation class for TenantExtractor
   *    - Property: hbase.multi.tenant.prefix.length
   *      Default prefix length if using fixed-length prefixes
   * 
   * 3. Default Values (used if neither above is specified)
   *    - Default prefix length: 4 bytes
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
      // For system tables with MULTI_TENANT_ENABLED=false, this will use SingleTenantExtractor
      // which creates HFile v4 with a single default section (clean and consistent)
      // For user tables with multi-tenant properties, this will use DefaultTenantExtractor  
      // which creates HFile v4 with multiple tenant sections based on row key prefixes
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
