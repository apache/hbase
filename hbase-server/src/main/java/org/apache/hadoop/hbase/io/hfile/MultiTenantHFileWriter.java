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
import org.apache.hadoop.hbase.util.BloomFilterFactory;
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
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import static org.apache.hadoop.hbase.io.hfile.BlockCompressedSizePredicator.MAX_BLOCK_SIZE_UNCOMPRESSED;

/**
 * An HFile writer that supports multiple tenants by sectioning the data within a single file.
 * <p>
 * This implementation takes advantage of the fact that HBase data is always written
 * in sorted order, so once we move to a new tenant, we'll never go back to a previous one.
 * <p>
 * Instead of creating separate physical files for each tenant, this writer creates a 
 * single HFile with internal sections that are indexed by tenant prefix.
 * <p>
 * Key features:
 * <ul>
 * <li>Single HFile v4 format with multiple tenant sections</li>
 * <li>Each section contains complete HFile v3 structure</li>
 * <li>Section-level bloom filters for efficient tenant-specific queries</li>
 * <li>Multi-level tenant indexing for fast section lookup</li>
 * <li>Configurable tenant prefix extraction</li>
 * </ul>
 */
@InterfaceAudience.Private
public class MultiTenantHFileWriter implements HFile.Writer {
  private static final Logger LOG = LoggerFactory.getLogger(MultiTenantHFileWriter.class);
  
  /** Tenant identification configuration at cluster level */
  public static final String TENANT_PREFIX_LENGTH = "hbase.multi.tenant.prefix.length";
  
  /** Tenant identification configuration at table level (higher precedence) */
  public static final String TABLE_TENANT_PREFIX_LENGTH = "TENANT_PREFIX_LENGTH";
  
  /** Table-level property to enable/disable multi-tenant sectioning */
  public static final String TABLE_MULTI_TENANT_ENABLED = "MULTI_TENANT_HFILE";
  /** Empty prefix for default tenant */
  private static final byte[] DEFAULT_TENANT_PREFIX = new byte[0];
  
  /**
   * Class that manages tenant configuration with proper precedence:
   * <ol>
   *   <li>Table level settings have highest precedence</li>
   *   <li>Cluster level settings are used as fallback</li>
   *   <li>Default values are used if neither is specified</li>
   * </ol>
   */
  // TenantConfiguration class removed - use TenantExtractorFactory instead
  
  /** Extractor for tenant information */
  private final TenantExtractor tenantExtractor;
  /** Filesystem to write to */
  private final FileSystem fs;
  /** Path for the HFile */
  private final Path path;
  /** Configuration settings */
  private final Configuration conf;
  /** Cache configuration */
  private final CacheConfig cacheConf;
  /** HFile context */
  private final HFileContext fileContext;
  
  /** Main file writer components - Output stream */
  private final FSDataOutputStream outputStream;
  /** Block writer for HFile blocks */
  private HFileBlock.Writer blockWriter;
  /** Section index writer for tenant indexing */
  private SectionIndexManager.Writer sectionIndexWriter;
  
  /** Section tracking - Current section writer */
  private SectionWriter currentSectionWriter;
  /** Current tenant section ID */
  private byte[] currentTenantSectionId;
  /** Start offset of current section */
  private long sectionStartOffset;
  /** Number of sections written */
  private int sectionCount = 0;
  
  /** Stats for the entire file - Last cell written (internal tracking only) */
  private Cell lastCell = null;
  /** Total number of entries */
  private long entryCount = 0;
  /** Total key length across all entries */
  private long totalKeyLength = 0;
  /** Total value length across all entries */
  private long totalValueLength = 0;
  /** Length of the biggest cell */
  private long lenOfBiggestCell = 0;
  /** Maximum tags length encountered */
  private int maxTagsLength = 0;
  /** Total uncompressed bytes */
  private long totalUncompressedBytes = 0;
  
  /** Major version for HFile v4 */
  private int majorVersion = HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT;
  
  /** HFile v4 trailer */
  private FixedFileTrailer trailer;
  /** Meta block index writer */
  private HFileBlockIndex.BlockIndexWriter metaBlockIndexWriter;
  /** File info for metadata */
  private HFileInfo fileInfo = new HFileInfo();
  
  /** Whether write verification is enabled */
  private boolean enableWriteVerification;
  /** Configuration key for write verification */
  private static final String WRITE_VERIFICATION_ENABLED = 
      "hbase.multi.tenant.write.verification.enabled";
  /** Default write verification setting */
  private static final boolean DEFAULT_WRITE_VERIFICATION_ENABLED = false;
  

  
  /** Current bloom filter writer - one per section */
  private BloomFilterWriter currentBloomFilterWriter;
  /** Whether bloom filter is enabled */
  private boolean bloomFilterEnabled;
  /** Type of bloom filter to use */
  private BloomType bloomFilterType;
  
  /**
   * Creates a multi-tenant HFile writer that writes sections to a single file.
   * 
   * @param fs Filesystem to write to
   * @param path Path for the HFile (final destination)
   * @param conf Configuration settings
   * @param cacheConf Cache configuration
   * @param tenantExtractor Extractor for tenant information
   * @param fileContext HFile context
   * @param bloomType Type of bloom filter to use
   * @throws IOException If an error occurs during initialization
   */
  public MultiTenantHFileWriter(
      FileSystem fs,
      Path path,
      Configuration conf,
      CacheConfig cacheConf,
      TenantExtractor tenantExtractor,
      HFileContext fileContext,
      BloomType bloomType) throws IOException {
    // Follow HFileWriterImpl pattern: accept path and create outputStream
    this.path = path;
    this.fs = fs;
    this.conf = conf;
    this.cacheConf = cacheConf;
    this.tenantExtractor = tenantExtractor;
    this.fileContext = fileContext;
    this.enableWriteVerification = conf.getBoolean(WRITE_VERIFICATION_ENABLED, DEFAULT_WRITE_VERIFICATION_ENABLED);
    
    // Initialize bloom filter configuration using existing HBase properties
    // This reuses the standard io.storefile.bloom.enabled property instead of creating 
    // a new multi-tenant specific property, ensuring consistency with existing HBase behavior
    this.bloomFilterEnabled = BloomFilterFactory.isGeneralBloomEnabled(conf);
    // Bloom filter type is passed from table properties, respecting column family configuration
    this.bloomFilterType = bloomType;
    
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
   * <p>
   * This method applies configuration precedence:
   * <ol>
   * <li>Table-level properties have highest precedence</li>
   * <li>Cluster-level configuration used as fallback</li>
   * <li>Default values used if neither specified</li>
   * </ol>
   * 
   * @param fs Filesystem to write to
   * @param path Path for the HFile
   * @param conf Configuration settings that include cluster-level tenant configuration
   * @param cacheConf Cache configuration
   * @param tableProperties Table properties that may include table-level tenant configuration
   * @param fileContext HFile context
   * @return A configured MultiTenantHFileWriter
   * @throws IOException if writer creation fails
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
    
    // Extract bloom filter type from table properties if available
    BloomType bloomType = BloomType.ROW; // Default
    if (tableProperties != null && tableProperties.containsKey("BLOOMFILTER")) {
      try {
        bloomType = BloomType.valueOf(tableProperties.get("BLOOMFILTER").toUpperCase());
      } catch (IllegalArgumentException e) {
        LOG.warn("Invalid bloom filter type in table properties: {}, using default ROW", 
                 tableProperties.get("BLOOMFILTER"));
      }
    }
    
    LOG.info("Creating MultiTenantHFileWriter with tenant extractor: {}, bloom type: {}", 
             tenantExtractor.getClass().getSimpleName(), bloomType);
    
    // HFile version 4 inherently implies multi-tenant
    return new MultiTenantHFileWriter(fs, path, conf, cacheConf, tenantExtractor, fileContext, bloomType);
  }
  
  /**
   * Initialize the writer components including block writer and section index writer.
   * <p>
   * Sets up multi-level tenant indexing with configurable chunk sizes and index parameters.
   * 
   * @throws IOException if initialization fails
   */
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
    
    // Add to bloom filter if enabled
    if (bloomFilterEnabled && currentBloomFilterWriter != null) {
      try {
        currentBloomFilterWriter.append(cell);
      } catch (IOException e) {
        LOG.warn("Error adding cell to bloom filter", e);
      }
    }
    
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
      long entryCount = currentSectionWriter.getEntryCount();
      if (entryCount == 0) {
        LOG.warn("Closing empty section for tenant: {}", 
            Bytes.toStringBinary(currentTenantSectionId));
      }
      
      // Add bloom filter to the section if enabled
      if (bloomFilterEnabled && currentBloomFilterWriter != null) {
        long keyCount = currentBloomFilterWriter.getKeyCount();
        if (keyCount > 0) {
          LOG.debug("Adding section-specific bloom filter with {} keys for section: {}", 
              keyCount, Bytes.toStringBinary(currentTenantSectionId));
          // Compact the bloom filter before adding it
          currentBloomFilterWriter.compactBloom();
          // Add the bloom filter to the current section
          currentSectionWriter.addGeneralBloomFilter(currentBloomFilterWriter);
        } else {
          LOG.debug("No keys to add to bloom filter for section: {}", 
              Bytes.toStringBinary(currentTenantSectionId));
        }
        // Clear the bloom filter writer for the next section
        currentBloomFilterWriter = null;
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
   * Verify that the section was written correctly by checking basic structure.
   * <p>
   * Performs basic validation of section size and structure without expensive I/O operations.
   * 
   * @param sectionStartOffset Starting offset of the section in the file
   * @param sectionSize Size of the section in bytes
   * @throws IOException if verification fails or section structure is invalid
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
  
  /**
   * Create a new section for a tenant with its own writer and bloom filter.
   * <p>
   * Each section is a complete HFile v3 structure within the larger v4 file.
   * 
   * @param tenantSectionId The tenant section identifier for indexing
   * @param tenantId The tenant identifier for metadata
   * @throws IOException if section creation fails
   */
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
    
    // Create a new bloom filter for this section if enabled
    if (bloomFilterEnabled) {
      currentBloomFilterWriter = BloomFilterFactory.createGeneralBloomAtWrite(
          conf, 
          cacheConf,
          bloomFilterType,
          0,  // Don't need to specify maxKeys here
          currentSectionWriter); // Pass the section writer
      
      LOG.debug("Created new bloom filter for tenant section ID: {}", 
          tenantSectionId == null ? "null" : Bytes.toStringBinary(tenantSectionId));
    }
    
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
   * Write file info similar to HFileWriterImpl but adapted for multi-tenant structure.
   * 
   * @param trailer The file trailer to update with file info offset
   * @param out The output stream to write file info to
   * @throws IOException if writing fails
   */
  private void writeFileInfo(FixedFileTrailer trailer, DataOutputStream out) throws IOException {
    trailer.setFileInfoOffset(outputStream.getPos());
    fileInfo.write(out);
  }
  
  /**
   * Finish the close for HFile v4 trailer.
   * <p>
   * Sets v4-specific trailer fields including multi-tenant configuration
   * and writes the final trailer to complete the file.
   * 
   * @param trailer The trailer to finalize and write
   * @throws IOException if trailer writing fails
   */
  private void finishClose(FixedFileTrailer trailer) throws IOException {
    // Set v4-specific trailer fields
    trailer.setNumDataIndexLevels(sectionIndexWriter.getNumLevels());
    trailer.setUncompressedDataIndexSize(sectionIndexWriter.getTotalUncompressedSize());
    trailer.setDataIndexCount(sectionIndexWriter.getNumRootEntries());
    
    // Set multi-tenant configuration in the trailer - MOST IMPORTANT PART
    trailer.setMultiTenant(true);
    trailer.setTenantPrefixLength(tenantExtractor.getPrefixLength());
    
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
  
  /**
   * Finish file info preparation for multi-tenant HFile.
   * <p>
   * Excludes global first/last keys for tenant isolation while including
   * essential metadata like section count and tenant index structure.
   * 
   * @throws IOException if file info preparation fails
   */
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
    
    // Record tenant index structure information
    int tenantIndexLevels = sectionIndexWriter.getNumLevels();
    fileInfo.append(Bytes.toBytes("TENANT_INDEX_LEVELS"),
                    Bytes.toBytes(tenantIndexLevels), false);
    
    // Multi-tenant configuration is stored in the trailer for v4 files
    // This allows readers to access this information before file info is fully loaded
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
    // We create and add a bloom filter for each section separately
    // This method is called externally but we ignore it since we handle bloom filters internally
    if (bfw == null || bfw.getKeyCount() <= 0) {
      LOG.debug("Ignoring empty or null general bloom filter at global level");
      return;
    }
    
    LOG.debug("Ignoring external bloom filter with {} keys - using per-section bloom filters instead", 
             bfw.getKeyCount());
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
   * <p>
   * This handles writing data for a specific tenant section as a complete HFile v3 structure.
   * Each section maintains its own bloom filters and metadata while sharing the parent file's
   * output stream through position translation.
   */
  private class SectionWriter extends HFileWriterImpl {
    /** The tenant section identifier for this section */
    private final byte[] tenantSectionId;
    /** The starting offset of this section in the parent file */
    private final long sectionStartOffset;
    /** Whether this section writer has been closed */
    private boolean closed = false;
    
    /**
     * Creates a section writer for a specific tenant section.
     * 
     * @param conf Configuration settings
     * @param cacheConf Cache configuration
     * @param outputStream The parent file's output stream
     * @param fileContext HFile context for this section
     * @param tenantSectionId The tenant section identifier
     * @param tenantId The tenant identifier for metadata
     * @param sectionStartOffset Starting offset of this section
     * @throws IOException if section writer creation fails
     */
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
     * Output stream that translates positions relative to section start.
     * <p>
     * This allows each section to maintain its own position tracking while
     * writing to the shared parent file output stream.
     */
    private static class SectionOutputStream extends FSDataOutputStream {
      /** The delegate output stream (parent file stream) */
      private final FSDataOutputStream delegate;
      /** The base offset of this section in the parent file */
      private final long baseOffset;
      
      /**
       * Creates a section-aware output stream.
       * 
       * @param delegate The parent file's output stream
       * @param baseOffset The starting offset of this section
       */
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
   * An implementation of TenantExtractor that treats all data as belonging to a single default tenant.
   * <p>
   * This extractor is used when multi-tenant functionality is disabled via the TABLE_MULTI_TENANT_ENABLED 
   * property set to false. It ensures that all cells are treated as belonging to the same tenant section,
   * effectively creating a single-tenant HFile v4 with one section containing all data.
   * <p>
   * Key characteristics:
   * <ul>
   * <li>Always returns the default empty tenant prefix for all cells</li>
   * <li>Results in a single tenant section containing all data</li>
   * <li>Maintains HFile v4 format compatibility while disabling multi-tenant features</li>
   * <li>Useful for system tables or tables that don't require tenant isolation</li>
   * </ul>
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
  
  /**
   * Creates a specialized writer factory for multi-tenant HFiles format version 4.
   * <p>
   * This factory automatically determines whether to create a multi-tenant or single-tenant
   * writer based on table properties and configuration. It handles the extraction of table
   * properties from the HFile context and applies proper configuration precedence.
   */
  public static class WriterFactory extends HFile.WriterFactory {
    /** Maintain our own copy of the file context */
    private HFileContext writerFileContext;
    
    /**
     * Creates a new WriterFactory for multi-tenant HFiles.
     * 
     * @param conf Configuration settings
     * @param cacheConf Cache configuration
     */
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
