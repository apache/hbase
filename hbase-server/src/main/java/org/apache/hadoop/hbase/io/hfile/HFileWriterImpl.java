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

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.CellComparator.MetaCellComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;
import org.apache.hadoop.hbase.io.hfile.HFileBlock.BlockWritable;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.io.Writable;

/**
 * Common functionality needed by all versions of {@link HFile} writers.
 */
@InterfaceAudience.Private
public class HFileWriterImpl implements HFile.Writer {
  private static final Log LOG = LogFactory.getLog(HFileWriterImpl.class);

  /** The Cell previously appended. Becomes the last cell in the file.*/
  protected Cell lastCell = null;

  /** FileSystem stream to write into. */
  protected FSDataOutputStream outputStream;

  /** True if we opened the <code>outputStream</code> (and so will close it). */
  protected final boolean closeOutputStream;

  /** A "file info" block: a key-value map of file-wide metadata. */
  protected FileInfo fileInfo = new HFile.FileInfo();

  /** Total # of key/value entries, i.e. how many times add() was called. */
  protected long entryCount = 0;

  /** Used for calculating the average key length. */
  protected long totalKeyLength = 0;

  /** Used for calculating the average value length. */
  protected long totalValueLength = 0;

  /** Total uncompressed bytes, maybe calculate a compression ratio later. */
  protected long totalUncompressedBytes = 0;

  /** Key comparator. Used to ensure we write in order. */
  protected final CellComparator comparator;

  /** Meta block names. */
  protected List<byte[]> metaNames = new ArrayList<byte[]>();

  /** {@link Writable}s representing meta block data. */
  protected List<Writable> metaData = new ArrayList<Writable>();

  /**
   * First cell in a block.
   * This reference should be short-lived since we write hfiles in a burst.
   */
  protected Cell firstCellInBlock = null;


  /** May be null if we were passed a stream. */
  protected final Path path;

  /** Cache configuration for caching data on write. */
  protected final CacheConfig cacheConf;

  /**
   * Name for this object used when logging or in toString. Is either
   * the result of a toString on stream or else name of passed file Path.
   */
  protected final String name;

  /**
   * The data block encoding which will be used.
   * {@link NoOpDataBlockEncoder#INSTANCE} if there is no encoding.
   */
  protected final HFileDataBlockEncoder blockEncoder;

  protected final HFileContext hFileContext;

  private int maxTagsLength = 0;

  /** KeyValue version in FileInfo */
  public static final byte [] KEY_VALUE_VERSION = Bytes.toBytes("KEY_VALUE_VERSION");

  /** Version for KeyValue which includes memstore timestamp */
  public static final int KEY_VALUE_VER_WITH_MEMSTORE = 1;

  /** Inline block writers for multi-level block index and compound Blooms. */
  private List<InlineBlockWriter> inlineBlockWriters = new ArrayList<InlineBlockWriter>();

  /** block writer */
  protected HFileBlock.Writer fsBlockWriter;

  private HFileBlockIndex.BlockIndexWriter dataBlockIndexWriter;
  private HFileBlockIndex.BlockIndexWriter metaBlockIndexWriter;

  /** The offset of the first data block or -1 if the file is empty. */
  private long firstDataBlockOffset = -1;

  /** The offset of the last data block or 0 if the file is empty. */
  protected long lastDataBlockOffset;

  /**
   * The last(stop) Cell of the previous data block.
   * This reference should be short-lived since we write hfiles in a burst.
   */
  private Cell lastCellOfPreviousBlock = null;

  /** Additional data items to be written to the "load-on-open" section. */
  private List<BlockWritable> additionalLoadOnOpenData = new ArrayList<BlockWritable>();

  protected long maxMemstoreTS = 0;

  public HFileWriterImpl(final Configuration conf, CacheConfig cacheConf, Path path,
      FSDataOutputStream outputStream,
      CellComparator comparator, HFileContext fileContext) {
    this.outputStream = outputStream;
    this.path = path;
    this.name = path != null ? path.getName() : outputStream.toString();
    this.hFileContext = fileContext;
    DataBlockEncoding encoding = hFileContext.getDataBlockEncoding();
    if (encoding != DataBlockEncoding.NONE) {
      this.blockEncoder = new HFileDataBlockEncoderImpl(encoding);
    } else {
      this.blockEncoder = NoOpDataBlockEncoder.INSTANCE;
    }
    this.comparator = comparator != null ? comparator
        : CellComparator.COMPARATOR;

    closeOutputStream = path != null;
    this.cacheConf = cacheConf;
    finishInit(conf);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Writer" + (path != null ? " for " + path : "") +
        " initialized with cacheConf: " + cacheConf +
        " comparator: " + comparator.getClass().getSimpleName() +
        " fileContext: " + fileContext);
    }
  }

  /**
   * Add to the file info. All added key/value pairs can be obtained using
   * {@link HFile.Reader#loadFileInfo()}.
   *
   * @param k Key
   * @param v Value
   * @throws IOException in case the key or the value are invalid
   */
  @Override
  public void appendFileInfo(final byte[] k, final byte[] v)
      throws IOException {
    fileInfo.append(k, v, true);
  }

  /**
   * Sets the file info offset in the trailer, finishes up populating fields in
   * the file info, and writes the file info into the given data output. The
   * reason the data output is not always {@link #outputStream} is that we store
   * file info as a block in version 2.
   *
   * @param trailer fixed file trailer
   * @param out the data output to write the file info to
   * @throws IOException
   */
  protected final void writeFileInfo(FixedFileTrailer trailer, DataOutputStream out)
  throws IOException {
    trailer.setFileInfoOffset(outputStream.getPos());
    finishFileInfo();
    fileInfo.write(out);
  }

  /**
   * Checks that the given Cell's key does not violate the key order.
   *
   * @param cell Cell whose key to check.
   * @return true if the key is duplicate
   * @throws IOException if the key or the key order is wrong
   */
  protected boolean checkKey(final Cell cell) throws IOException {
    boolean isDuplicateKey = false;

    if (cell == null) {
      throw new IOException("Key cannot be null or empty");
    }
    if (lastCell != null) {
      int keyComp = comparator.compareKeyIgnoresMvcc(lastCell, cell);

      if (keyComp > 0) {
        throw new IOException("Added a key not lexically larger than"
            + " previous. Current cell = " + cell + ", lastCell = " + lastCell);
      } else if (keyComp == 0) {
        isDuplicateKey = true;
      }
    }
    return isDuplicateKey;
  }

  /** Checks the given value for validity. */
  protected void checkValue(final byte[] value, final int offset,
      final int length) throws IOException {
    if (value == null) {
      throw new IOException("Value cannot be null");
    }
  }

  /**
   * @return Path or null if we were passed a stream rather than a Path.
   */
  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public String toString() {
    return "writer=" + (path != null ? path.toString() : null) + ", name="
        + name + ", compression=" + hFileContext.getCompression().getName();
  }

  public static Compression.Algorithm compressionByName(String algoName) {
    if (algoName == null)
      return HFile.DEFAULT_COMPRESSION_ALGORITHM;
    return Compression.getCompressionAlgorithmByName(algoName);
  }

  /** A helper method to create HFile output streams in constructors */
  protected static FSDataOutputStream createOutputStream(Configuration conf,
      FileSystem fs, Path path, InetSocketAddress[] favoredNodes) throws IOException {
    FsPermission perms = FSUtils.getFilePermissions(fs, conf,
        HConstants.DATA_FILE_UMASK_KEY);
    return FSUtils.create(fs, path, perms, favoredNodes);
  }

  /** Additional initialization steps */
  protected void finishInit(final Configuration conf) {
    if (fsBlockWriter != null) {
      throw new IllegalStateException("finishInit called twice");
    }

    fsBlockWriter = new HFileBlock.Writer(blockEncoder, hFileContext);

    // Data block index writer
    boolean cacheIndexesOnWrite = cacheConf.shouldCacheIndexesOnWrite();
    dataBlockIndexWriter = new HFileBlockIndex.BlockIndexWriter(fsBlockWriter,
        cacheIndexesOnWrite ? cacheConf : null,
        cacheIndexesOnWrite ? name : null);
    dataBlockIndexWriter.setMaxChunkSize(
        HFileBlockIndex.getMaxChunkSize(conf));
    inlineBlockWriters.add(dataBlockIndexWriter);

    // Meta data block index writer
    metaBlockIndexWriter = new HFileBlockIndex.BlockIndexWriter();
    if (LOG.isTraceEnabled()) LOG.trace("Initialized with " + cacheConf);
  }

  /**
   * At a block boundary, write all the inline blocks and opens new block.
   *
   * @throws IOException
   */
  protected void checkBlockBoundary() throws IOException {
    if (fsBlockWriter.blockSizeWritten() < hFileContext.getBlocksize()) return;
    finishBlock();
    writeInlineBlocks(false);
    newBlock();
  }

  /** Clean up the current data block */
  private void finishBlock() throws IOException {
    if (!fsBlockWriter.isWriting() || fsBlockWriter.blockSizeWritten() == 0) return;

    // Update the first data block offset for scanning.
    if (firstDataBlockOffset == -1) {
      firstDataBlockOffset = outputStream.getPos();
    }
    // Update the last data block offset
    lastDataBlockOffset = outputStream.getPos();
    fsBlockWriter.writeHeaderAndData(outputStream);
    int onDiskSize = fsBlockWriter.getOnDiskSizeWithHeader();
    Cell indexEntry =
      getMidpoint(this.comparator, lastCellOfPreviousBlock, firstCellInBlock);
    dataBlockIndexWriter.addEntry(CellUtil.getCellKeySerializedAsKeyValueKey(indexEntry),
      lastDataBlockOffset, onDiskSize);
    totalUncompressedBytes += fsBlockWriter.getUncompressedSizeWithHeader();
    if (cacheConf.shouldCacheDataOnWrite()) {
      doCacheOnWrite(lastDataBlockOffset);
    }
  }
  
  /**
   * Try to return a Cell that falls between <code>left</code> and
   * <code>right</code> but that is shorter; i.e. takes up less space. This
   * trick is used building HFile block index. Its an optimization. It does not
   * always work. In this case we'll just return the <code>right</code> cell.
   * 
   * @param comparator
   *          Comparator to use.
   * @param left
   * @param right
   * @return A cell that sorts between <code>left</code> and <code>right</code>.
   */
  public static Cell getMidpoint(final CellComparator comparator, final Cell left,
      final Cell right) {
    // TODO: Redo so only a single pass over the arrays rather than one to
    // compare and then a
    // second composing midpoint.
    if (right == null) {
      throw new IllegalArgumentException("right cell can not be null");
    }
    if (left == null) {
      return right;
    }
    // If Cells from meta table, don't mess around. meta table Cells have schema
    // (table,startrow,hash) so can't be treated as plain byte arrays. Just skip
    // out without
    // trying to do this optimization.
    if (comparator != null && comparator instanceof MetaCellComparator) {
      return right;
    }
    int diff = comparator.compareRows(left, right);
    if (diff > 0) {
      throw new IllegalArgumentException("Left row sorts after right row; left="
          + CellUtil.getCellKeyAsString(left) + ", right=" + CellUtil.getCellKeyAsString(right));
    }
    if (diff < 0) {
      // Left row is < right row.
      byte[] midRow = getMinimumMidpointArray(left.getRowArray(), left.getRowOffset(),
          left.getRowLength(), right.getRowArray(), right.getRowOffset(), right.getRowLength());
      // If midRow is null, just return 'right'. Can't do optimization.
      if (midRow == null)
        return right;
      return CellUtil.createCell(midRow);
    }
    // Rows are same. Compare on families.
    int lFamOffset = left.getFamilyOffset();
    int rFamOffset = right.getFamilyOffset();
    int lFamLength = left.getFamilyLength();
    int rFamLength = right.getFamilyLength();
    diff = CellComparator.compareFamilies(left, right);
    if (diff > 0) {
      throw new IllegalArgumentException("Left family sorts after right family; left="
          + CellUtil.getCellKeyAsString(left) + ", right=" + CellUtil.getCellKeyAsString(right));
    }
    if (diff < 0) {
      byte[] midRow = getMinimumMidpointArray(left.getFamilyArray(), lFamOffset,
          lFamLength, right.getFamilyArray(), rFamOffset,
          rFamLength);
      // If midRow is null, just return 'right'. Can't do optimization.
      if (midRow == null)
        return right;
      // Return new Cell where we use right row and then a mid sort family.
      return CellUtil.createCell(right.getRowArray(), right.getRowOffset(), right.getRowLength(),
          midRow, 0, midRow.length, HConstants.EMPTY_BYTE_ARRAY, 0,
          HConstants.EMPTY_BYTE_ARRAY.length);
    }
    // Families are same. Compare on qualifiers.
    diff = CellComparator.compareQualifiers(left, right);
    if (diff > 0) {
      throw new IllegalArgumentException("Left qualifier sorts after right qualifier; left="
          + CellUtil.getCellKeyAsString(left) + ", right=" + CellUtil.getCellKeyAsString(right));
    }
    if (diff < 0) {
      byte[] midRow = getMinimumMidpointArray(left.getQualifierArray(), left.getQualifierOffset(),
          left.getQualifierLength(), right.getQualifierArray(), right.getQualifierOffset(),
          right.getQualifierLength());
      // If midRow is null, just return 'right'. Can't do optimization.
      if (midRow == null)
        return right;
      // Return new Cell where we use right row and family and then a mid sort
      // qualifier.
      return CellUtil.createCell(right.getRowArray(), right.getRowOffset(), right.getRowLength(),
          right.getFamilyArray(), right.getFamilyOffset(), right.getFamilyLength(), midRow, 0,
          midRow.length);
    }
    // No opportunity for optimization. Just return right key.
    return right;
  }
  
  /**
   * @param leftArray
   * @param leftOffset
   * @param leftLength
   * @param rightArray
   * @param rightOffset
   * @param rightLength
   * @return Return a new array that is between left and right and minimally
   *         sized else just return null as indicator that we could not create a
   *         mid point.
   */
  private static byte[] getMinimumMidpointArray(final byte[] leftArray, final int leftOffset,
      final int leftLength, final byte[] rightArray, final int rightOffset, final int rightLength) {
    // rows are different
    int minLength = leftLength < rightLength ? leftLength : rightLength;
    int diffIdx = 0;
    while (diffIdx < minLength
        && leftArray[leftOffset + diffIdx] == rightArray[rightOffset + diffIdx]) {
      diffIdx++;
    }
    byte[] minimumMidpointArray = null;
    if (diffIdx >= minLength) {
      // leftKey's row is prefix of rightKey's.
      minimumMidpointArray = new byte[diffIdx + 1];
      System.arraycopy(rightArray, rightOffset, minimumMidpointArray, 0, diffIdx + 1);
    } else {
      int diffByte = leftArray[leftOffset + diffIdx];
      if ((0xff & diffByte) < 0xff && (diffByte + 1) < (rightArray[rightOffset + diffIdx] & 0xff)) {
        minimumMidpointArray = new byte[diffIdx + 1];
        System.arraycopy(leftArray, leftOffset, minimumMidpointArray, 0, diffIdx);
        minimumMidpointArray[diffIdx] = (byte) (diffByte + 1);
      } else {
        minimumMidpointArray = new byte[diffIdx + 1];
        System.arraycopy(rightArray, rightOffset, minimumMidpointArray, 0, diffIdx + 1);
      }
    }
    return minimumMidpointArray;
  }

  /** Gives inline block writers an opportunity to contribute blocks. */
  private void writeInlineBlocks(boolean closing) throws IOException {
    for (InlineBlockWriter ibw : inlineBlockWriters) {
      while (ibw.shouldWriteBlock(closing)) {
        long offset = outputStream.getPos();
        boolean cacheThisBlock = ibw.getCacheOnWrite();
        ibw.writeInlineBlock(fsBlockWriter.startWriting(
            ibw.getInlineBlockType()));
        fsBlockWriter.writeHeaderAndData(outputStream);
        ibw.blockWritten(offset, fsBlockWriter.getOnDiskSizeWithHeader(),
            fsBlockWriter.getUncompressedSizeWithoutHeader());
        totalUncompressedBytes += fsBlockWriter.getUncompressedSizeWithHeader();

        if (cacheThisBlock) {
          doCacheOnWrite(offset);
        }
      }
    }
  }

  /**
   * Caches the last written HFile block.
   * @param offset the offset of the block we want to cache. Used to determine
   *          the cache key.
   */
  private void doCacheOnWrite(long offset) {
    HFileBlock cacheFormatBlock = fsBlockWriter.getBlockForCaching(cacheConf);
    cacheConf.getBlockCache().cacheBlock(new BlockCacheKey(name, offset), cacheFormatBlock);
  }

  /**
   * Ready a new block for writing.
   *
   * @throws IOException
   */
  protected void newBlock() throws IOException {
    // This is where the next block begins.
    fsBlockWriter.startWriting(BlockType.DATA);
    firstCellInBlock = null;
    if (lastCell != null) {
      lastCellOfPreviousBlock = lastCell;
    }
  }

  /**
   * Add a meta block to the end of the file. Call before close(). Metadata
   * blocks are expensive. Fill one with a bunch of serialized data rather than
   * do a metadata block per metadata instance. If metadata is small, consider
   * adding to file info using {@link #appendFileInfo(byte[], byte[])}
   *
   * @param metaBlockName
   *          name of the block
   * @param content
   *          will call readFields to get data later (DO NOT REUSE)
   */
  @Override
  public void appendMetaBlock(String metaBlockName, Writable content) {
    byte[] key = Bytes.toBytes(metaBlockName);
    int i;
    for (i = 0; i < metaNames.size(); ++i) {
      // stop when the current key is greater than our own
      byte[] cur = metaNames.get(i);
      if (Bytes.BYTES_RAWCOMPARATOR.compare(cur, 0, cur.length, key, 0,
          key.length) > 0) {
        break;
      }
    }
    metaNames.add(i, key);
    metaData.add(i, content);
  }

  @Override
  public void close() throws IOException {
    if (outputStream == null) {
      return;
    }
    // Save data block encoder metadata in the file info.
    blockEncoder.saveMetadata(this);
    // Write out the end of the data blocks, then write meta data blocks.
    // followed by fileinfo, data block index and meta block index.

    finishBlock();
    writeInlineBlocks(true);

    FixedFileTrailer trailer = new FixedFileTrailer(getMajorVersion(), getMinorVersion());

    // Write out the metadata blocks if any.
    if (!metaNames.isEmpty()) {
      for (int i = 0; i < metaNames.size(); ++i) {
        // store the beginning offset
        long offset = outputStream.getPos();
        // write the metadata content
        DataOutputStream dos = fsBlockWriter.startWriting(BlockType.META);
        metaData.get(i).write(dos);

        fsBlockWriter.writeHeaderAndData(outputStream);
        totalUncompressedBytes += fsBlockWriter.getUncompressedSizeWithHeader();

        // Add the new meta block to the meta index.
        metaBlockIndexWriter.addEntry(metaNames.get(i), offset,
            fsBlockWriter.getOnDiskSizeWithHeader());
      }
    }

    // Load-on-open section.

    // Data block index.
    //
    // In version 2, this section of the file starts with the root level data
    // block index. We call a function that writes intermediate-level blocks
    // first, then root level, and returns the offset of the root level block
    // index.

    long rootIndexOffset = dataBlockIndexWriter.writeIndexBlocks(outputStream);
    trailer.setLoadOnOpenOffset(rootIndexOffset);

    // Meta block index.
    metaBlockIndexWriter.writeSingleLevelIndex(fsBlockWriter.startWriting(
        BlockType.ROOT_INDEX), "meta");
    fsBlockWriter.writeHeaderAndData(outputStream);
    totalUncompressedBytes += fsBlockWriter.getUncompressedSizeWithHeader();

    if (this.hFileContext.isIncludesMvcc()) {
      appendFileInfo(MAX_MEMSTORE_TS_KEY, Bytes.toBytes(maxMemstoreTS));
      appendFileInfo(KEY_VALUE_VERSION, Bytes.toBytes(KEY_VALUE_VER_WITH_MEMSTORE));
    }

    // File info
    writeFileInfo(trailer, fsBlockWriter.startWriting(BlockType.FILE_INFO));
    fsBlockWriter.writeHeaderAndData(outputStream);
    totalUncompressedBytes += fsBlockWriter.getUncompressedSizeWithHeader();

    // Load-on-open data supplied by higher levels, e.g. Bloom filters.
    for (BlockWritable w : additionalLoadOnOpenData){
      fsBlockWriter.writeBlock(w, outputStream);
      totalUncompressedBytes += fsBlockWriter.getUncompressedSizeWithHeader();
    }

    // Now finish off the trailer.
    trailer.setNumDataIndexLevels(dataBlockIndexWriter.getNumLevels());
    trailer.setUncompressedDataIndexSize(
        dataBlockIndexWriter.getTotalUncompressedSize());
    trailer.setFirstDataBlockOffset(firstDataBlockOffset);
    trailer.setLastDataBlockOffset(lastDataBlockOffset);
    trailer.setComparatorClass(comparator.getClass());
    trailer.setDataIndexCount(dataBlockIndexWriter.getNumRootEntries());


    finishClose(trailer);

    fsBlockWriter.release();
  }

  @Override
  public void addInlineBlockWriter(InlineBlockWriter ibw) {
    inlineBlockWriters.add(ibw);
  }

  @Override
  public void addGeneralBloomFilter(final BloomFilterWriter bfw) {
    this.addBloomFilter(bfw, BlockType.GENERAL_BLOOM_META);
  }

  @Override
  public void addDeleteFamilyBloomFilter(final BloomFilterWriter bfw) {
    this.addBloomFilter(bfw, BlockType.DELETE_FAMILY_BLOOM_META);
  }

  private void addBloomFilter(final BloomFilterWriter bfw,
      final BlockType blockType) {
    if (bfw.getKeyCount() <= 0)
      return;

    if (blockType != BlockType.GENERAL_BLOOM_META &&
        blockType != BlockType.DELETE_FAMILY_BLOOM_META) {
      throw new RuntimeException("Block Type: " + blockType.toString() +
          "is not supported");
    }
    additionalLoadOnOpenData.add(new BlockWritable() {
      @Override
      public BlockType getBlockType() {
        return blockType;
      }

      @Override
      public void writeToBlock(DataOutput out) throws IOException {
        bfw.getMetaWriter().write(out);
        Writable dataWriter = bfw.getDataWriter();
        if (dataWriter != null)
          dataWriter.write(out);
      }
    });
  }

  @Override
  public HFileContext getFileContext() {
    return hFileContext;
  }

  /**
   * Add key/value to file. Keys must be added in an order that agrees with the
   * Comparator passed on construction.
   *
   * @param cell
   *          Cell to add. Cannot be empty nor null.
   * @throws IOException
   */
  @Override
  public void append(final Cell cell) throws IOException {
    byte[] value = cell.getValueArray();
    int voffset = cell.getValueOffset();
    int vlength = cell.getValueLength();
    // checkKey uses comparator to check we are writing in order.
    boolean dupKey = checkKey(cell);
    checkValue(value, voffset, vlength);
    if (!dupKey) {
      checkBlockBoundary();
    }

    if (!fsBlockWriter.isWriting()) {
      newBlock();
    }

    fsBlockWriter.write(cell);

    totalKeyLength += CellUtil.estimatedSerializedSizeOfKey(cell);
    totalValueLength += vlength;

    // Are we the first key in this block?
    if (firstCellInBlock == null) {
      // If cell is big, block will be closed and this firstCellInBlock reference will only last
      // a short while.
      firstCellInBlock = cell;
    }

    // TODO: What if cell is 10MB and we write infrequently? We hold on to cell here indefinetly?
    lastCell = cell;
    entryCount++;
    this.maxMemstoreTS = Math.max(this.maxMemstoreTS, cell.getSequenceId());
    int tagsLength = cell.getTagsLength();
    if (tagsLength > this.maxTagsLength) {
      this.maxTagsLength = tagsLength;
    }
  }

  protected void finishFileInfo() throws IOException {
    if (lastCell != null) {
      // Make a copy. The copy is stuffed into our fileinfo map. Needs a clean
      // byte buffer. Won't take a tuple.
      byte [] lastKey = CellUtil.getCellKeySerializedAsKeyValueKey(this.lastCell);
      fileInfo.append(FileInfo.LASTKEY, lastKey, false);
    }

    // Average key length.
    int avgKeyLen =
        entryCount == 0 ? 0 : (int) (totalKeyLength / entryCount);
    fileInfo.append(FileInfo.AVG_KEY_LEN, Bytes.toBytes(avgKeyLen), false);
    fileInfo.append(FileInfo.CREATE_TIME_TS, Bytes.toBytes(hFileContext.getFileCreateTime()),
      false);

    // Average value length.
    int avgValueLen =
        entryCount == 0 ? 0 : (int) (totalValueLength / entryCount);
    fileInfo.append(FileInfo.AVG_VALUE_LEN, Bytes.toBytes(avgValueLen), false);
    if (hFileContext.getDataBlockEncoding() == DataBlockEncoding.PREFIX_TREE) {
      // In case of Prefix Tree encoding, we always write tags information into HFiles even if all
      // KVs are having no tags.
      fileInfo.append(FileInfo.MAX_TAGS_LEN, Bytes.toBytes(this.maxTagsLength), false);
    } else if (hFileContext.isIncludesTags()) {
      // When tags are not being written in this file, MAX_TAGS_LEN is excluded
      // from the FileInfo
      fileInfo.append(FileInfo.MAX_TAGS_LEN, Bytes.toBytes(this.maxTagsLength), false);
      boolean tagsCompressed = (hFileContext.getDataBlockEncoding() != DataBlockEncoding.NONE)
        && hFileContext.isCompressTags();
      fileInfo.append(FileInfo.TAGS_COMPRESSED, Bytes.toBytes(tagsCompressed), false);
    }
  }

  protected int getMajorVersion() {
    return 3;
  }

  protected int getMinorVersion() {
    return HFileReaderImpl.MAX_MINOR_VERSION;
  }

  protected void finishClose(FixedFileTrailer trailer) throws IOException {
    // Write out encryption metadata before finalizing if we have a valid crypto context
    Encryption.Context cryptoContext = hFileContext.getEncryptionContext();
    if (cryptoContext != Encryption.Context.NONE) {
      // Wrap the context's key and write it as the encryption metadata, the wrapper includes
      // all information needed for decryption
      trailer.setEncryptionKey(EncryptionUtil.wrapKey(cryptoContext.getConf(),
        cryptoContext.getConf().get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY,
          User.getCurrent().getShortName()),
        cryptoContext.getKey()));
    }
    // Now we can finish the close
    trailer.setMetaIndexCount(metaNames.size());
    trailer.setTotalUncompressedBytes(totalUncompressedBytes+ trailer.getTrailerSize());
    trailer.setEntryCount(entryCount);
    trailer.setCompressionCodec(hFileContext.getCompression());

    trailer.serialize(outputStream);

    if (closeOutputStream) {
      outputStream.close();
      outputStream = null;
    }
  }
}