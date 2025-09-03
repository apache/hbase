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
    /** The tenant prefix for this section */
    private final byte[] tenantPrefix;
    /** The file offset where the section starts */
    private final long offset;
    /** The size of the section in bytes */
    private final int sectionSize;
    
    /**
     * Constructor for SectionIndexEntry.
     *
     * @param tenantPrefix the tenant prefix for this section
     * @param offset the file offset where the section starts
     * @param sectionSize the size of the section in bytes
     */
    public SectionIndexEntry(byte[] tenantPrefix, long offset, int sectionSize) {
      this.tenantPrefix = tenantPrefix;
      this.offset = offset;
      this.sectionSize = sectionSize;
    }
    
    /**
     * Get the tenant prefix for this section.
     *
     * @return the tenant prefix
     */
    public byte[] getTenantPrefix() {
      return tenantPrefix;
    }
    
    /**
     * Get the file offset where the section starts.
     *
     * @return the offset
     */
    public long getOffset() {
      return offset;
    }
    
    /**
     * Get the size of the section in bytes.
     *
     * @return the section size
     */
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
    /** List of entries in this block */
    private final List<SectionIndexEntry> entries = new ArrayList<>();
    /** The offset of this block in the file */
    private long blockOffset;
    /** The size of this block in bytes */
    private int blockSize;
    
    /**
     * Add an entry to this block.
     *
     * @param entry the entry to add
     */
    public void addEntry(SectionIndexEntry entry) {
      entries.add(entry);
    }
    
    /**
     * Get all entries in this block.
     *
     * @return the list of entries
     */
    public List<SectionIndexEntry> getEntries() {
      return entries;
    }
    
    /**
     * Get the number of entries in this block.
     *
     * @return the entry count
     */
    public int getEntryCount() {
      return entries.size();
    }
    
    /**
     * Get the first entry in this block.
     *
     * @return the first entry, or null if the block is empty
     */
    public SectionIndexEntry getFirstEntry() {
      return entries.isEmpty() ? null : entries.get(0);
    }
    
    /**
     * Set the metadata for this block.
     *
     * @param offset the offset of this block in the file
     * @param size the size of this block in bytes
     */
    public void setBlockMetadata(long offset, int size) {
      this.blockOffset = offset;
      this.blockSize = size;
    }
    
    /**
     * Get the offset of this block in the file.
     *
     * @return the block offset
     */
    public long getBlockOffset() {
      return blockOffset;
    }
    
    /**
     * Get the size of this block in bytes.
     *
     * @return the block size
     */
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
    
    /** List of all section entries */
    private final List<SectionIndexEntry> entries = new ArrayList<>();
    /** Block writer to use for index blocks */
    private final HFileBlock.Writer blockWriter;
    /** Cache configuration (unused for section index blocks) */
    @SuppressWarnings("unused")
    private final CacheConfig cacheConf;
    /** File name to use for caching, or null if no caching (unused) */
    @SuppressWarnings("unused")
    private final String nameForCaching;
    
    /** Maximum number of entries in a single index block */
    private int maxChunkSize = DEFAULT_MAX_CHUNK_SIZE;
    /** Minimum number of entries in the root-level index block */
    private int minIndexNumEntries = DEFAULT_MIN_INDEX_NUM_ENTRIES;
    /** Total uncompressed size of the index */
    private int totalUncompressedSize = 0;
    /** Number of levels in this index */
    private int numLevels = 1;
    
    /** Track leaf blocks for building the multi-level index */
    private final List<SectionIndexBlock> leafBlocks = new ArrayList<>();
    /** Track intermediate blocks for building the multi-level index */
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
     * @param outputStream the output stream to write to
     * @return the offset where the section index root block starts
     * @throws IOException if an I/O error occurs
     */
    public long writeIndexBlocks(FSDataOutputStream outputStream) throws IOException {
      // Handle empty indexes like HFileBlockIndex does - write valid empty structure
      if (entries.isEmpty()) {
        LOG.info("Writing empty section index (no tenant sections)");
        return writeEmptyIndex(outputStream);
      }
      
      // Keep entries in their original order for sequential access
      
      // Determine if we need a multi-level index based on entry count
      boolean multiLevel = entries.size() > maxChunkSize;
      
      // Clear any existing block tracking
      leafBlocks.clear();
      intermediateBlocks.clear();
      
      // For small indices, just write a single-level root block
      if (!multiLevel) {
        numLevels = 1;
        return writeSingleLevelIndex(outputStream);
      }
      
      // Split entries into leaf blocks
      int numLeafBlocks = (entries.size() + maxChunkSize - 1) / maxChunkSize;
      for (int blockIndex = 0; blockIndex < numLeafBlocks; blockIndex++) {
        SectionIndexBlock block = new SectionIndexBlock();
        int startIndex = blockIndex * maxChunkSize;
        int endIndex = Math.min((blockIndex + 1) * maxChunkSize, entries.size());
        
        for (int entryIndex = startIndex; entryIndex < endIndex; entryIndex++) {
          block.addEntry(entries.get(entryIndex));
        }
        
        leafBlocks.add(block);
      }
      
      // Write leaf blocks
      writeLeafBlocks(outputStream);
      
      // If we have few enough leaf blocks, root can point directly to them
      if (leafBlocks.size() <= minIndexNumEntries) {
        numLevels = 2; // Root + leaf level
        return writeIntermediateBlock(outputStream, leafBlocks, true);
      }
      
      // Otherwise, we need intermediate blocks
      numLevels = 3; // Root + intermediate + leaf
      
      // Group leaf blocks into intermediate blocks
      int intermediateBlocksNeeded = (leafBlocks.size() + maxChunkSize - 1) / maxChunkSize;
      for (int blockIndex = 0; blockIndex < intermediateBlocksNeeded; blockIndex++) {
        SectionIndexBlock block = new SectionIndexBlock();
        int startIndex = blockIndex * maxChunkSize;
        int endIndex = Math.min((blockIndex + 1) * maxChunkSize, leafBlocks.size());
        
        for (int leafIndex = startIndex; leafIndex < endIndex; leafIndex++) {
          SectionIndexBlock leafBlock = leafBlocks.get(leafIndex);
          // Add the first entry from this leaf block to the intermediate block
          block.addEntry(leafBlock.getFirstEntry());
        }
        
        intermediateBlocks.add(block);
      }
      
      // Write intermediate blocks
      writeIntermediateBlocks(outputStream);
      
      // Write root block (pointing to intermediate blocks)
      return writeIntermediateBlock(outputStream, intermediateBlocks, true);
    }
    
    /**
     * Write an empty index structure. This creates a valid but empty root block
     * similar to how HFileBlockIndex handles empty indexes.
     * 
     * @param out the output stream to write to
     * @return the offset where the empty root block starts
     * @throws IOException if an I/O error occurs
     */
    private long writeEmptyIndex(FSDataOutputStream out) throws IOException {
      // Record root offset
      long rootOffset = out.getPos();
      
      // Write empty root block
      DataOutputStream dos = blockWriter.startWriting(BlockType.ROOT_INDEX);
      dos.writeInt(0); // Zero entries
      blockWriter.writeHeaderAndData(out);
      
      // Update metrics
      totalUncompressedSize += blockWriter.getOnDiskSizeWithHeader();
      numLevels = 1;
      
      LOG.info("Wrote empty section index at offset {}", rootOffset);
      
      return rootOffset;
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
      for (int blockIndex = 0; blockIndex < intermediateBlocks.size(); blockIndex++) {
        SectionIndexBlock block = intermediateBlocks.get(blockIndex);
        long blockOffset = out.getPos();
        DataOutputStream dos = blockWriter.startWriting(BlockType.INTERMEDIATE_INDEX);

        int entryCount = block.getEntryCount();
        dos.writeInt(entryCount);

        // Entries in this intermediate block correspond to leaf blocks in range
        // [startIndex, startIndex + entryCount)
        int startIndex = blockIndex * maxChunkSize;
        for (int i = 0; i < entryCount; i++) {
          int leafIndex = startIndex + i;
          SectionIndexBlock leafBlock = leafBlocks.get(leafIndex);
          SectionIndexEntry firstEntry = leafBlock.getFirstEntry();

          byte[] prefix = firstEntry.getTenantPrefix();
          dos.writeInt(prefix.length);
          dos.write(prefix);
          dos.writeLong(leafBlock.getBlockOffset());
          dos.writeInt(leafBlock.getBlockSize());
        }

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
    
    /** List of all section entries loaded from the index */
    private final List<SectionIndexEntry> sections = new ArrayList<>();
    /** Number of levels in the loaded index */
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
     * Load a (potentially multi-level) section index from the given root index block.
     * This API requires the number of index levels (from the trailer) and an FS reader
     * for fetching intermediate/leaf blocks when needed.
     *
     * @param rootBlock the ROOT_INDEX block where the section index starts
     * @param levels the number of index levels; 1 for single-level, >=2 for multi-level
     * @param fsReader the filesystem block reader to fetch child index blocks
     */
    public void loadSectionIndex(HFileBlock rootBlock, int levels, HFileBlock.FSReader fsReader)
        throws IOException {
      if (rootBlock.getBlockType() != BlockType.ROOT_INDEX) {
        throw new IOException("Block is not a ROOT_INDEX for section index: "
            + rootBlock.getBlockType());
      }
      if (levels < 1) {
        throw new IOException("Invalid index level count: " + levels);
      }
      sections.clear();
      this.numLevels = levels;

      if (levels == 1) {
        // Single-level index: entries are directly in the root
        loadSectionIndex(rootBlock);
        return;
      }

      if (fsReader == null) {
        throw new IOException("FSReader is required to read multi-level section index");
      }

      // Multi-level: root contains pointers to next-level blocks.
      DataInputStream in = rootBlock.getByteStream();
      int fanout = in.readInt();
      if (fanout < 0) {
        throw new IOException("Negative root entry count in section index: " + fanout);
      }
      for (int i = 0; i < fanout; i++) {
        // Root entry: first leaf entry (prefix, offset, size) + child pointer (offset, size)
        int prefixLength = in.readInt();
        byte[] prefix = new byte[prefixLength];
        in.readFully(prefix);
        in.readLong(); // first entry offset (ignored)
        in.readInt();  // first entry size (ignored)
        long childBlockOffset = in.readLong();
        int childBlockSize = in.readInt();

        readChildIndexSubtree(childBlockOffset, childBlockSize, levels - 1, fsReader);
      }

      LOG.debug("Loaded multi-level section index: levels={}, sections={}", this.numLevels, sections.size());
    }

    /**
     * Recursively read intermediate/leaf index blocks and collect section entries.
     */
    private void readChildIndexSubtree(long blockOffset, int blockSize, int levelsRemaining,
        HFileBlock.FSReader fsReader) throws IOException {
      HFileBlock child = fsReader.readBlockData(blockOffset, blockSize, true, true, true);
      try {
        if (levelsRemaining == 1) {
          // Leaf level: contains actual section entries
          if (child.getBlockType() != BlockType.LEAF_INDEX) {
            LOG.warn("Expected LEAF_INDEX at leaf level but found {}", child.getBlockType());
          }
          readLeafBlock(child);
          return;
        }

        // Intermediate level: each entry points to a child block
        if (child.getBlockType() != BlockType.INTERMEDIATE_INDEX) {
          LOG.warn("Expected INTERMEDIATE_INDEX at level {} but found {}", levelsRemaining,
              child.getBlockType());
        }
        DataInputStream in = child.getByteStream();
        int entryCount = in.readInt();
        if (entryCount < 0) {
          throw new IOException("Negative intermediate entry count in section index: " + entryCount);
        }
        for (int i = 0; i < entryCount; i++) {
          int prefixLength = in.readInt();
          byte[] prefix = new byte[prefixLength];
          in.readFully(prefix);
          long nextOffset = in.readLong();
          int nextSize = in.readInt();
          readChildIndexSubtree(nextOffset, nextSize, levelsRemaining - 1, fsReader);
        }
      } finally {
        // Release as these are non-root, transient blocks
        try {
          child.release();
        } catch (Throwable t) {
          // ignore
        }
      }
    }

    /**
     * Parse a leaf index block and append all section entries.
     */
    private void readLeafBlock(HFileBlock leafBlock) throws IOException {
      DataInputStream in = leafBlock.getByteStream();
      int num = in.readInt();
      if (num < 0) {
        throw new IOException("Negative leaf entry count in section index: " + num);
      }
      for (int i = 0; i < num; i++) {
        int prefixLength = in.readInt();
        byte[] prefix = new byte[prefixLength];
        in.readFully(prefix);
        long offset = in.readLong();
        int size = in.readInt();
        sections.add(new SectionIndexEntry(prefix, offset, size));
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