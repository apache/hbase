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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the section index for multi-tenant HFile version 4.
 * This class contains both writer and reader functionality for section indices,
 * which map tenant prefixes to file sections, allowing for efficient
 * lookup of tenant-specific data in a multi-tenant HFile.
 */
@InterfaceAudience.Private
public class SectionIndexManager {
  private static final Logger LOG = LoggerFactory.getLogger(SectionIndexManager.class);
  
  /**
   * Default maximum number of entries in a single index block
   */
  public static final int DEFAULT_MAX_CHUNK_SIZE = 128;
  
  /**
   * Default minimum number of entries in the root index block
   */
  public static final int DEFAULT_MIN_INDEX_NUM_ENTRIES = 16;
  
  /**
   * Configuration key for maximum chunk size
   */
  public static final String SECTION_INDEX_MAX_CHUNK_SIZE = 
      "hbase.section.index.max.chunk.size";
  
  /**
   * Configuration key for minimum number of root entries
   */
  public static final String SECTION_INDEX_MIN_NUM_ENTRIES = 
      "hbase.section.index.min.num.entries";
  
  /**
   * Represents a tenant section entry in the index.
   */
  public static class SectionIndexEntry {
    private final byte[] tenantPrefix;
    private final long offset;
    private final int sectionSize;
    
    public SectionIndexEntry(byte[] tenantPrefix, long offset, int sectionSize) {
      this.tenantPrefix = tenantPrefix;
      this.offset = offset;
      this.sectionSize = sectionSize;
    }
    
    public byte[] getTenantPrefix() {
      return tenantPrefix;
    }
    
    public long getOffset() {
      return offset;
    }
    
    public int getSectionSize() {
      return sectionSize;
    }
    
    @Override
    public String toString() {
      return "SectionIndexEntry{" +
          "tenantPrefix=" + Bytes.toStringBinary(tenantPrefix) +
          ", offset=" + offset +
          ", sectionSize=" + sectionSize +
          '}';
    }
  }
  
  /**
   * Represents a block in the multi-level section index.
   */
  private static class SectionIndexBlock {
    private final List<SectionIndexEntry> entries = new ArrayList<>();
    private long blockOffset;
    private int blockSize;
    
    public void addEntry(SectionIndexEntry entry) {
      entries.add(entry);
    }
    
    public List<SectionIndexEntry> getEntries() {
      return entries;
    }
    
    public int getEntryCount() {
      return entries.size();
    }
    
    public SectionIndexEntry getFirstEntry() {
      return entries.isEmpty() ? null : entries.get(0);
    }
    
    public void setBlockMetadata(long offset, int size) {
      this.blockOffset = offset;
      this.blockSize = size;
    }
    
    public long getBlockOffset() {
      return blockOffset;
    }
    
    public int getBlockSize() {
      return blockSize;
    }
  }
  
  /**
   * Writer for section indices in multi-tenant HFile version 4.
   * This writer collects section entries and writes them to the file
   * as a multi-level index to support large tenant sets efficiently.
   */
  public static class Writer {
    private static final Logger LOG = LoggerFactory.getLogger(Writer.class);
    
    private final List<SectionIndexEntry> entries = new ArrayList<>();
    private final HFileBlock.Writer blockWriter;
    private final CacheConfig cacheConf;
    private final String nameForCaching;
    
    private int maxChunkSize = DEFAULT_MAX_CHUNK_SIZE;
    private int minIndexNumEntries = DEFAULT_MIN_INDEX_NUM_ENTRIES;
    private int totalUncompressedSize = 0;
    private int numLevels = 1;
    
    // Track leaf and intermediate blocks for building the multi-level index
    private final List<SectionIndexBlock> leafBlocks = new ArrayList<>();
    private final List<SectionIndexBlock> intermediateBlocks = new ArrayList<>();
    
    /**
     * Constructor for Writer.
     * 
     * @param blockWriter block writer to use for index blocks
     * @param cacheConf cache configuration
     * @param nameForCaching file name to use for caching, or null if no caching
     */
    public Writer(
        HFileBlock.Writer blockWriter,
        CacheConfig cacheConf,
        String nameForCaching) {
      this.blockWriter = blockWriter;
      this.cacheConf = cacheConf;
      this.nameForCaching = nameForCaching;
    }
    
    /**
     * Set the maximum number of entries in a single index block.
     * 
     * @param maxChunkSize The maximum number of entries per block
     */
    public void setMaxChunkSize(int maxChunkSize) {
      this.maxChunkSize = maxChunkSize;
    }
    
    /**
     * Set the minimum number of entries in the root-level index block.
     * 
     * @param minIndexNumEntries The minimum number of entries
     */
    public void setMinIndexNumEntries(int minIndexNumEntries) {
      this.minIndexNumEntries = minIndexNumEntries;
    }
    
    /**
     * Add a section entry to the index.
     * 
     * @param tenantPrefix the tenant prefix for this section
     * @param offset the file offset where the section starts
     * @param sectionSize the size of the section in bytes
     */
    public void addEntry(byte[] tenantPrefix, long offset, int sectionSize) {
      SectionIndexEntry entry = new SectionIndexEntry(
          tenantPrefix != null ? tenantPrefix : new byte[0],
          offset,
          sectionSize);
      entries.add(entry);
      
      LOG.debug("Added section index entry: tenant={}, offset={}, size={}",
          tenantPrefix != null ? Bytes.toStringBinary(tenantPrefix) : "default",
          offset,
          sectionSize);
    }
    
    /**
     * Helper to write a single section index entry (prefix, offset, size).
     */
    private void writeEntry(DataOutputStream out, SectionIndexEntry entry) throws IOException {
      byte[] prefix = entry.getTenantPrefix();
      out.writeInt(prefix.length);
      out.write(prefix);
      out.writeLong(entry.getOffset());
      out.writeInt(entry.getSectionSize());
    }
    
    /**
     * Write the section index blocks to the output stream.
     * For large tenant sets, this builds a multi-level index.
     * 
     * @param out the output stream to write to
     * @return the offset where the section index root block starts
     * @throws IOException if an I/O error occurs
     */
    public long writeIndexBlocks(FSDataOutputStream out) throws IOException {
      if (entries.isEmpty()) {
        throw new IOException("No tenant sections to write in the index");
      }
      
      // Sort entries by tenant prefix for binary search later
      Collections.sort(entries, (a, b) -> 
          Bytes.compareTo(a.getTenantPrefix(), b.getTenantPrefix()));
      
      // Determine if we need a multi-level index based on entry count
      boolean multiLevel = entries.size() > maxChunkSize;
      
      // Clear any existing block tracking
      leafBlocks.clear();
      intermediateBlocks.clear();
      
      // For small indices, just write a single-level root block
      if (!multiLevel) {
        numLevels = 1;
        return writeSingleLevelIndex(out);
      }
      
      // Split entries into leaf blocks
      int numLeafBlocks = (entries.size() + maxChunkSize - 1) / maxChunkSize;
      for (int i = 0; i < numLeafBlocks; i++) {
        SectionIndexBlock block = new SectionIndexBlock();
        int startIdx = i * maxChunkSize;
        int endIdx = Math.min((i + 1) * maxChunkSize, entries.size());
        
        for (int entryIdx = startIdx; entryIdx < endIdx; entryIdx++) {
          block.addEntry(entries.get(entryIdx));
        }
        
        leafBlocks.add(block);
      }
      
      // Write leaf blocks
      writeLeafBlocks(out);
      
      // If we have few enough leaf blocks, root can point directly to them
      if (leafBlocks.size() <= minIndexNumEntries) {
        numLevels = 2; // Root + leaf level
        return writeIntermediateBlock(out, leafBlocks, true);
      }
      
      // Otherwise, we need intermediate blocks
      numLevels = 3; // Root + intermediate + leaf
      
      // Group leaf blocks into intermediate blocks
      int intermBlocksNeeded = (leafBlocks.size() + maxChunkSize - 1) / maxChunkSize;
      for (int i = 0; i < intermBlocksNeeded; i++) {
        SectionIndexBlock block = new SectionIndexBlock();
        int startIdx = i * maxChunkSize;
        int endIdx = Math.min((i + 1) * maxChunkSize, leafBlocks.size());
        
        for (int leafIdx = startIdx; leafIdx < endIdx; leafIdx++) {
          SectionIndexBlock leafBlock = leafBlocks.get(leafIdx);
          // Add the first entry from this leaf block to the intermediate block
          block.addEntry(leafBlock.getFirstEntry());
        }
        
        intermediateBlocks.add(block);
      }
      
      // Write intermediate blocks
      writeIntermediateBlocks(out);
      
      // Write root block (pointing to intermediate blocks)
      return writeIntermediateBlock(out, intermediateBlocks, true);
    }
    
    /**
     * Write a single-level index (just the root block).
     */
    private long writeSingleLevelIndex(FSDataOutputStream out) throws IOException {
      // Record root offset
      long rootOffset = out.getPos();
      
      // Write root block containing all entries
      DataOutputStream dos = blockWriter.startWriting(BlockType.ROOT_INDEX);
      writeRootBlock(dos, entries);
      blockWriter.writeHeaderAndData(out);
      
      // Update metrics
      totalUncompressedSize += blockWriter.getOnDiskSizeWithHeader();
      
      LOG.info("Wrote single-level section index with {} entries at offset {}",
          entries.size(), rootOffset);
      
      return rootOffset;
    }
    
    /**
     * Write all leaf-level blocks.
     */
    private void writeLeafBlocks(FSDataOutputStream out) throws IOException {
      for (SectionIndexBlock block : leafBlocks) {
        // Write leaf block
        long blockOffset = out.getPos();
        DataOutputStream dos = blockWriter.startWriting(BlockType.LEAF_INDEX);
        writeIndexBlock(dos, block.getEntries());
        blockWriter.writeHeaderAndData(out);
        
        // Record block metadata for higher levels
        block.setBlockMetadata(blockOffset, blockWriter.getOnDiskSizeWithHeader());
        
        // Update metrics
        totalUncompressedSize += blockWriter.getUncompressedSizeWithHeader();
        
        LOG.debug("Wrote leaf section index block with {} entries at offset {}",
            block.getEntryCount(), blockOffset);
      }
    }
    
    /**
     * Write all intermediate-level blocks.
     */
    private void writeIntermediateBlocks(FSDataOutputStream out) throws IOException {
      for (SectionIndexBlock block : intermediateBlocks) {
        // Write intermediate block
        long blockOffset = out.getPos();
        List<SectionIndexEntry> blockEntries = block.getEntries();
        DataOutputStream dos = blockWriter.startWriting(BlockType.INTERMEDIATE_INDEX);
        
        // For intermediate blocks, we include offset/size of target blocks
        writeIntermediateBlock(dos, blockEntries);
        blockWriter.writeHeaderAndData(out);
        
        // Record block metadata for higher levels
        block.setBlockMetadata(blockOffset, blockWriter.getOnDiskSizeWithHeader());
        
        // Update metrics
        totalUncompressedSize += blockWriter.getUncompressedSizeWithHeader();
        
        LOG.debug("Wrote intermediate section index block with {} entries at offset {}",
            block.getEntryCount(), blockOffset);
      }
    }
    
    /**
     * Write an intermediate or root block that points to other blocks.
     */
    private long writeIntermediateBlock(FSDataOutputStream out, List<SectionIndexBlock> blocks, 
                                       boolean isRoot) throws IOException {
      long blockOffset = out.getPos();
      DataOutputStream dos = blockWriter.startWriting(
          isRoot ? BlockType.ROOT_INDEX : BlockType.INTERMEDIATE_INDEX);
      
      // Write block count
      dos.writeInt(blocks.size());
      
      // Write entries using helper + block metadata
      for (SectionIndexBlock block : blocks) {
        SectionIndexEntry firstEntry = block.getFirstEntry();
        writeEntry(dos, firstEntry);
        dos.writeLong(block.getBlockOffset());
        dos.writeInt(block.getBlockSize());
      }
      
      blockWriter.writeHeaderAndData(out);
      
      // Update metrics
      totalUncompressedSize += blockWriter.getUncompressedSizeWithHeader();
      
      LOG.debug("Wrote {} section index block with {} entries at offset {}",
          isRoot ? "root" : "intermediate", blocks.size(), blockOffset);
      
      return blockOffset;
    }
    
    /**
     * Write a standard index block with section entries.
     */
    private void writeIndexBlock(DataOutputStream out, List<SectionIndexEntry> blockEntries) 
        throws IOException {
      // Write entry count
      out.writeInt(blockEntries.size());
      
      // Write each entry using helper
      for (SectionIndexEntry entry : blockEntries) {
        writeEntry(out, entry);
      }
    }
    
    /**
     * Write an intermediate block with references to other blocks.
     */
    private void writeIntermediateBlock(DataOutputStream out, List<SectionIndexEntry> blockEntries)
        throws IOException {
      // Write entry count
      out.writeInt(blockEntries.size());
      
      // For intermediate blocks, we only write the first entry's tenant prefix
      // and the target block information
      for (int i = 0; i < blockEntries.size(); i++) {
        SectionIndexEntry entry = blockEntries.get(i);
        
        // Write tenant prefix
        byte[] prefix = entry.getTenantPrefix();
        out.writeInt(prefix.length);
        out.write(prefix);
        
        // Write target leaf block offset and size
        int leafBlockIndex = i * maxChunkSize;
        if (leafBlockIndex < leafBlocks.size()) {
          SectionIndexBlock leafBlock = leafBlocks.get(leafBlockIndex);
          out.writeLong(leafBlock.getBlockOffset());
          out.writeInt(leafBlock.getBlockSize());
        } else {
          // This shouldn't happen but we need to write something
          out.writeLong(0);
          out.writeInt(0);
          LOG.warn("Invalid leaf block index in intermediate block: {}", leafBlockIndex);
        }
      }
    }
    
    /**
     * Write a root block.
     */
    private void writeRootBlock(DataOutputStream out, List<SectionIndexEntry> entries) 
        throws IOException {
      // Just delegate to the standard index block writer
      writeIndexBlock(out, entries);
    }
    
    /**
     * Get the number of root entries in the index.
     * 
     * @return the number of entries at the root level
     */
    public int getNumRootEntries() {
      if (numLevels == 1) {
        return entries.size();
      } else if (numLevels == 2) {
        return leafBlocks.size();
      } else {
        return intermediateBlocks.size();
      }
    }
    
    /**
     * Get the number of levels in this index.
     * 
     * @return the number of levels (1 for single level, 2+ for multi-level)
     */
    public int getNumLevels() {
      return numLevels;
    }
    
    /**
     * Get the total uncompressed size of the index.
     * 
     * @return the total uncompressed size in bytes
     */
    public int getTotalUncompressedSize() {
      return totalUncompressedSize;
    }
    
    /**
     * Clear all entries from the index.
     */
    public void clear() {
      entries.clear();
      leafBlocks.clear();
      intermediateBlocks.clear();
      totalUncompressedSize = 0;
      numLevels = 1;
    }
  }
  
  /**
   * Reader for section indices in multi-tenant HFile version 4.
   * Supports both single-level and multi-level indices.
   */
  public static class Reader {
    private static final Logger LOG = LoggerFactory.getLogger(Reader.class);
    
    private final List<SectionIndexEntry> sections = new ArrayList<>();
    private int numLevels = 1;
    
    /**
     * Default constructor for Reader.
     */
    public Reader() {
      // Empty constructor
    }
    
    /**
     * Load a section index from an HFile block.
     * 
     * @param block the HFile block containing the section index
     * @throws IOException if an I/O error occurs
     */
    public void loadSectionIndex(HFileBlock block) throws IOException {
      if (block.getBlockType() != BlockType.ROOT_INDEX) {
        throw new IOException("Block is not a ROOT_INDEX for section index: " + block.getBlockType());
      }
      
      sections.clear();
      DataInputStream in = block.getByteStream();
      
      try {
        // Read the number of sections
        int numSections = in.readInt();
        
        // Read each section entry
        for (int i = 0; i < numSections; i++) {
          // Read tenant prefix
          int prefixLength = in.readInt();
          byte[] prefix = new byte[prefixLength];
          in.readFully(prefix);
          
          // Read offset and size
          long offset = in.readLong();
          int size = in.readInt();
          
          // Add the entry
          sections.add(new SectionIndexEntry(prefix, offset, size));
        }
        
        LOG.debug("Loaded section index with {} entries", sections.size());
      } catch (IOException e) {
        LOG.error("Failed to load section index", e);
        sections.clear();
        throw e;
      }
    }
    
    /**
     * Find the section entry for a given tenant prefix.
     * 
     * @param tenantPrefix the tenant prefix to look up
     * @return the section entry, or null if not found
     */
    public SectionIndexEntry findSection(byte[] tenantPrefix) {
      for (SectionIndexEntry entry : sections) {
        if (Bytes.equals(entry.getTenantPrefix(), tenantPrefix)) {
          return entry;
        }
      }
      return null;
    }
    
    /**
     * Get all section entries in the index.
     * 
     * @return the list of section entries
     */
    public List<SectionIndexEntry> getSections() {
      return new ArrayList<>(sections);
    }
    
    /**
     * Get the number of sections in the index.
     * 
     * @return the number of sections
     */
    public int getNumSections() {
      return sections.size();
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("SectionIndexReader{sections=").append(sections.size()).append(", entries=[");
      for (int i = 0; i < sections.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(sections.get(i));
      }
      sb.append("]}");
      return sb.toString();
    }
  }
} 