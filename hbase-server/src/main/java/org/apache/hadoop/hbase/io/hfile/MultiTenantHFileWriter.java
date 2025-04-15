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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

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

    // Biggest cell info
    if (keyOfBiggestCell != null) {
      fileInfo.append(HFileInfo.KEY_OF_BIGGEST_CELL, keyOfBiggestCell, false);
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
}