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

package org.apache.hadoop.hbase.codec.prefixtree.encode;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeBlockMeta;
import org.apache.hadoop.hbase.codec.prefixtree.encode.column.ColumnSectionWriter;
import org.apache.hadoop.hbase.codec.prefixtree.encode.other.CellTypeEncoder;
import org.apache.hadoop.hbase.codec.prefixtree.encode.other.ColumnNodeType;
import org.apache.hadoop.hbase.codec.prefixtree.encode.other.LongEncoder;
import org.apache.hadoop.hbase.codec.prefixtree.encode.row.RowSectionWriter;
import org.apache.hadoop.hbase.codec.prefixtree.encode.tokenize.Tokenizer;
import org.apache.hadoop.hbase.io.CellOutputStream;
import org.apache.hadoop.hbase.util.ArrayUtils;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.SimpleMutableByteRange;
import org.apache.hadoop.hbase.util.byterange.ByteRangeSet;
import org.apache.hadoop.hbase.util.byterange.impl.ByteRangeHashSet;
import org.apache.hadoop.hbase.util.byterange.impl.ByteRangeTreeSet;
import org.apache.hadoop.hbase.util.vint.UFIntTool;
import org.apache.hadoop.io.WritableUtils;
/**
 * This is the primary class for converting a CellOutputStream into an encoded byte[]. As Cells are
 * added they are completely copied into the various encoding structures. This is important because
 * usually the cells being fed in during compactions will be transient.<br>
 * <br>
 * Usage:<br>
 * 1) constructor<br>
 * 4) append cells in sorted order: write(Cell cell)<br>
 * 5) flush()<br>
 */
@InterfaceAudience.Private
public class PrefixTreeEncoder implements CellOutputStream {

  /**************** static ************************/

  protected static final Log LOG = LogFactory.getLog(PrefixTreeEncoder.class);

  //future-proof where HBase supports multiple families in a data block.
  public static final boolean MULITPLE_FAMILIES_POSSIBLE = false;

  private static final boolean USE_HASH_COLUMN_SORTER = true;
  private static final int INITIAL_PER_CELL_ARRAY_SIZES = 256;
  private static final int VALUE_BUFFER_INIT_SIZE = 64 * 1024;


  /**************** fields *************************/

  protected long numResets = 0L;

  protected OutputStream outputStream;

  /*
   * Cannot change during a single block's encoding. If false, then substitute incoming Cell's
   * mvccVersion with zero and write out the block as usual.
   */
  protected boolean includeMvccVersion;

  /*
   * reusable ByteRanges used for communicating with the sorters/compilers
   */
  protected ByteRange rowRange;
  protected ByteRange familyRange;
  protected ByteRange qualifierRange;
  protected ByteRange tagsRange;

  /*
   * incoming Cell fields are copied into these arrays
   */
  protected long[] timestamps;
  protected long[] mvccVersions;
  protected byte[] typeBytes;
  protected int[] valueOffsets;
  protected int[] tagsOffsets;
  protected byte[] values;
  protected byte[] tags;

  protected PrefixTreeBlockMeta blockMeta;

  /*
   * Sub-encoders for the simple long/byte fields of a Cell.  Add to these as each cell arrives and
   * compile before flushing.
   */
  protected LongEncoder timestampEncoder;
  protected LongEncoder mvccVersionEncoder;
  protected CellTypeEncoder cellTypeEncoder;

  /*
   * Structures used for collecting families and qualifiers, de-duplicating them, and sorting them
   * so they can be passed to the tokenizers. Unlike row keys where we can detect duplicates by
   * comparing only with the previous row key, families and qualifiers can arrive in unsorted order
   * in blocks spanning multiple rows. We must collect them all into a set to de-duplicate them.
   */
  protected ByteRangeSet familyDeduplicator;
  protected ByteRangeSet qualifierDeduplicator;
  protected ByteRangeSet tagsDeduplicator;
  /*
   * Feed sorted byte[]s into these tokenizers which will convert the byte[]s to an in-memory
   * trie structure with nodes connected by memory pointers (not serializable yet).
   */
  protected Tokenizer rowTokenizer;
  protected Tokenizer familyTokenizer;
  protected Tokenizer qualifierTokenizer;
  protected Tokenizer tagsTokenizer;

  /*
   * Writers take an in-memory trie, sort the nodes, calculate offsets and lengths, and write
   * all information to an output stream of bytes that can be stored on disk.
   */
  protected RowSectionWriter rowWriter;
  protected ColumnSectionWriter familyWriter;
  protected ColumnSectionWriter qualifierWriter;
  protected ColumnSectionWriter tagsWriter;

  /*
   * Integers used for counting cells and bytes.  We keep track of the size of the Cells as if they
   * were full KeyValues because some parts of HBase like to know the "unencoded size".
   */
  protected int totalCells = 0;
  protected int totalUnencodedBytes = 0;//numBytes if the cells were KeyValues
  protected int totalValueBytes = 0;
  protected int totalTagBytes = 0;
  protected int maxValueLength = 0;
  protected int maxTagLength = 0;
  protected int totalBytes = 0;//


  /***************** construct ***********************/

  public PrefixTreeEncoder(OutputStream outputStream, boolean includeMvccVersion) {
    // used during cell accumulation
    this.blockMeta = new PrefixTreeBlockMeta();
    this.rowRange = new SimpleMutableByteRange();
    this.familyRange = new SimpleMutableByteRange();
    this.qualifierRange = new SimpleMutableByteRange();
    this.timestamps = new long[INITIAL_PER_CELL_ARRAY_SIZES];
    this.mvccVersions = new long[INITIAL_PER_CELL_ARRAY_SIZES];
    this.typeBytes = new byte[INITIAL_PER_CELL_ARRAY_SIZES];
    this.valueOffsets = new int[INITIAL_PER_CELL_ARRAY_SIZES];
    this.values = new byte[VALUE_BUFFER_INIT_SIZE];

    // used during compilation
    this.familyDeduplicator = USE_HASH_COLUMN_SORTER ? new ByteRangeHashSet()
        : new ByteRangeTreeSet();
    this.qualifierDeduplicator = USE_HASH_COLUMN_SORTER ? new ByteRangeHashSet()
        : new ByteRangeTreeSet();
    this.timestampEncoder = new LongEncoder();
    this.mvccVersionEncoder = new LongEncoder();
    this.cellTypeEncoder = new CellTypeEncoder();
    this.rowTokenizer = new Tokenizer();
    this.familyTokenizer = new Tokenizer();
    this.qualifierTokenizer = new Tokenizer();
    this.rowWriter = new RowSectionWriter();
    this.familyWriter = new ColumnSectionWriter();
    this.qualifierWriter = new ColumnSectionWriter();
    initializeTagHelpers();

    reset(outputStream, includeMvccVersion);
  }

  public void reset(OutputStream outputStream, boolean includeMvccVersion) {
    ++numResets;
    this.includeMvccVersion = includeMvccVersion;
    this.outputStream = outputStream;
    valueOffsets[0] = 0;
    familyDeduplicator.reset();
    qualifierDeduplicator.reset();
    tagsDeduplicator.reset();
    tagsWriter.reset();
    tagsTokenizer.reset();
    rowTokenizer.reset();
    timestampEncoder.reset();
    mvccVersionEncoder.reset();
    cellTypeEncoder.reset();
    familyTokenizer.reset();
    qualifierTokenizer.reset();
    rowWriter.reset();
    familyWriter.reset();
    qualifierWriter.reset();

    totalCells = 0;
    totalUnencodedBytes = 0;
    totalValueBytes = 0;
    maxValueLength = 0;
    totalBytes = 0;
  }

  protected void initializeTagHelpers() {
    this.tagsRange = new SimpleMutableByteRange();
    this.tagsDeduplicator = USE_HASH_COLUMN_SORTER ? new ByteRangeHashSet()
    : new ByteRangeTreeSet();
    this.tagsTokenizer = new Tokenizer();
    this.tagsWriter = new ColumnSectionWriter();
  }

  /**
   * Check that the arrays used to hold cell fragments are large enough for the cell that is being
   * added. Since the PrefixTreeEncoder is cached between uses, these arrays may grow during the
   * first few block encodings but should stabilize quickly.
   */
  protected void ensurePerCellCapacities() {
    int currentCapacity = valueOffsets.length;
    int neededCapacity = totalCells + 2;// some things write one index ahead. +2 to be safe
    if (neededCapacity < currentCapacity) {
      return;
    }

    int padding = neededCapacity;//this will double the array size
    timestamps = ArrayUtils.growIfNecessary(timestamps, neededCapacity, padding);
    mvccVersions = ArrayUtils.growIfNecessary(mvccVersions, neededCapacity, padding);
    typeBytes = ArrayUtils.growIfNecessary(typeBytes, neededCapacity, padding);
    valueOffsets = ArrayUtils.growIfNecessary(valueOffsets, neededCapacity, padding);
  }

  /******************** CellOutputStream methods *************************/

  /**
   * Note: Unused until support is added to the scanner/heap
   * <p/>
   * The following method are optimized versions of write(Cell cell). The result should be
   * identical, however the implementation may be able to execute them much more efficiently because
   * it does not need to compare the unchanged fields with the previous cell's.
   * <p/>
   * Consider the benefits during compaction when paired with a CellScanner that is also aware of
   * row boundaries. The CellScanner can easily use these methods instead of blindly passing Cells
   * to the write(Cell cell) method.
   * <p/>
   * The savings of skipping duplicate row detection are significant with long row keys. A
   * DataBlockEncoder may store a row key once in combination with a count of how many cells are in
   * the row. With a 100 byte row key, we can replace 100 byte comparisons with a single increment
   * of the counter, and that is for every cell in the row.
   */

  /**
   * Add a Cell to the output stream but repeat the previous row. 
   */
  //@Override
  public void writeWithRepeatRow(Cell cell) {
    ensurePerCellCapacities();//can we optimize away some of this?

    //save a relatively expensive row comparison, incrementing the row's counter instead
    rowTokenizer.incrementNumOccurrencesOfLatestValue();
    addFamilyPart(cell);
    addQualifierPart(cell);
    addAfterRowFamilyQualifier(cell);
  }


  @Override
  public void write(Cell cell) {
    ensurePerCellCapacities();

    rowTokenizer.addSorted(CellUtil.fillRowRange(cell, rowRange));
    addFamilyPart(cell);
    addQualifierPart(cell);
    addTagPart(cell);
    addAfterRowFamilyQualifier(cell);
  }


  private void addTagPart(Cell cell) {
    CellUtil.fillTagRange(cell, tagsRange);
    tagsDeduplicator.add(tagsRange);
  }

  /***************** internal add methods ************************/

  private void addAfterRowFamilyQualifier(Cell cell){
    // timestamps
    timestamps[totalCells] = cell.getTimestamp();
    timestampEncoder.add(cell.getTimestamp());

    // memstore timestamps
    if (includeMvccVersion) {
      mvccVersions[totalCells] = cell.getSequenceId();
      mvccVersionEncoder.add(cell.getSequenceId());
      totalUnencodedBytes += WritableUtils.getVIntSize(cell.getSequenceId());
    }else{
      //must overwrite in case there was a previous version in this array slot
      mvccVersions[totalCells] = 0L;
      if(totalCells == 0){//only need to do this for the first cell added
        mvccVersionEncoder.add(0L);
      }
      //totalUncompressedBytes += 0;//mvccVersion takes zero bytes when disabled
    }

    // types
    typeBytes[totalCells] = cell.getTypeByte();
    cellTypeEncoder.add(cell.getTypeByte());

    // values
    totalValueBytes += cell.getValueLength();
    // double the array each time we run out of space
    values = ArrayUtils.growIfNecessary(values, totalValueBytes, 2 * totalValueBytes);
    CellUtil.copyValueTo(cell, values, valueOffsets[totalCells]);
    if (cell.getValueLength() > maxValueLength) {
      maxValueLength = cell.getValueLength();
    }
    valueOffsets[totalCells + 1] = totalValueBytes;

    // general
    totalUnencodedBytes += KeyValueUtil.length(cell);
    ++totalCells;
  }

  private void addFamilyPart(Cell cell) {
    if (MULITPLE_FAMILIES_POSSIBLE || totalCells == 0) {
      CellUtil.fillFamilyRange(cell, familyRange);
      familyDeduplicator.add(familyRange);
    }
  }

  private void addQualifierPart(Cell cell) {
    CellUtil.fillQualifierRange(cell, qualifierRange);
    qualifierDeduplicator.add(qualifierRange);
  }


  /****************** compiling/flushing ********************/

  /**
   * Expensive method.  The second half of the encoding work happens here.
   *
   * Take all the separate accumulated data structures and turn them into a single stream of bytes
   * which is written to the outputStream.
   */
  @Override
  public void flush() throws IOException {
    compile();

    // do the actual flushing to the output stream.  Order matters.
    blockMeta.writeVariableBytesToOutputStream(outputStream);
    rowWriter.writeBytes(outputStream);
    familyWriter.writeBytes(outputStream);
    qualifierWriter.writeBytes(outputStream);
    tagsWriter.writeBytes(outputStream);
    timestampEncoder.writeBytes(outputStream);
    mvccVersionEncoder.writeBytes(outputStream);
    //CellType bytes are in the row nodes.  there is no additional type section
    outputStream.write(values, 0, totalValueBytes);
  }

  /**
   * Now that all the cells have been added, do the work to reduce them to a series of byte[]
   * fragments that are ready to be written to the output stream.
   */
  protected void compile(){
    blockMeta.setNumKeyValueBytes(totalUnencodedBytes);
    int lastValueOffset = valueOffsets[totalCells];
    blockMeta.setValueOffsetWidth(UFIntTool.numBytes(lastValueOffset));
    blockMeta.setValueLengthWidth(UFIntTool.numBytes(maxValueLength));
    blockMeta.setNumValueBytes(totalValueBytes);
    totalBytes += totalTagBytes + totalValueBytes;

    //these compile methods will add to totalBytes
    compileTypes();
    compileMvccVersions();
    compileTimestamps();
    compileTags();
    compileQualifiers();
    compileFamilies();
    compileRows();

    int numMetaBytes = blockMeta.calculateNumMetaBytes();
    blockMeta.setNumMetaBytes(numMetaBytes);
    totalBytes += numMetaBytes;
  }

  /**
   * <p>
   * The following "compile" methods do any intermediate work necessary to transform the cell
   * fragments collected during the writing phase into structures that are ready to write to the
   * outputStream.
   * </p>
   * The family and qualifier treatment is almost identical, as is timestamp and mvccVersion.
   */

  protected void compileTypes() {
    blockMeta.setAllSameType(cellTypeEncoder.areAllSameType());
    if(cellTypeEncoder.areAllSameType()){
      blockMeta.setAllTypes(cellTypeEncoder.getOnlyType());
    }
  }

  protected void compileMvccVersions() {
    mvccVersionEncoder.compile();
    blockMeta.setMvccVersionFields(mvccVersionEncoder);
    int numMvccVersionBytes = mvccVersionEncoder.getOutputArrayLength();
    totalBytes += numMvccVersionBytes;
  }

  protected void compileTimestamps() {
    timestampEncoder.compile();
    blockMeta.setTimestampFields(timestampEncoder);
    int numTimestampBytes = timestampEncoder.getOutputArrayLength();
    totalBytes += numTimestampBytes;
  }

  protected void compileQualifiers() {
    blockMeta.setNumUniqueQualifiers(qualifierDeduplicator.size());
    qualifierDeduplicator.compile();
    qualifierTokenizer.addAll(qualifierDeduplicator.getSortedRanges());
    qualifierWriter.reconstruct(blockMeta, qualifierTokenizer, ColumnNodeType.QUALIFIER);
    qualifierWriter.compile();
    int numQualifierBytes = qualifierWriter.getNumBytes();
    blockMeta.setNumQualifierBytes(numQualifierBytes);
    totalBytes += numQualifierBytes;
  }

  protected void compileFamilies() {
    blockMeta.setNumUniqueFamilies(familyDeduplicator.size());
    familyDeduplicator.compile();
    familyTokenizer.addAll(familyDeduplicator.getSortedRanges());
    familyWriter.reconstruct(blockMeta, familyTokenizer, ColumnNodeType.FAMILY);
    familyWriter.compile();
    int numFamilyBytes = familyWriter.getNumBytes();
    blockMeta.setNumFamilyBytes(numFamilyBytes);
    totalBytes += numFamilyBytes;
  }

  protected void compileTags() {
    blockMeta.setNumUniqueTags(tagsDeduplicator.size());
    tagsDeduplicator.compile();
    tagsTokenizer.addAll(tagsDeduplicator.getSortedRanges());
    tagsWriter.reconstruct(blockMeta, tagsTokenizer, ColumnNodeType.TAGS);
    tagsWriter.compile();
    int numTagBytes = tagsWriter.getNumBytes();
    blockMeta.setNumTagsBytes(numTagBytes);
    totalBytes += numTagBytes;
  }

  protected void compileRows() {
    rowWriter.reconstruct(this);
    rowWriter.compile();
    int numRowBytes = rowWriter.getNumBytes();
    blockMeta.setNumRowBytes(numRowBytes);
    blockMeta.setRowTreeDepth(rowTokenizer.getTreeDepth());
    totalBytes += numRowBytes;
  }

  /********************* convenience getters ********************************/

  public long getValueOffset(int index) {
    return valueOffsets[index];
  }

  public int getValueLength(int index) {
    return (int) (valueOffsets[index + 1] - valueOffsets[index]);
  }

  /************************* get/set *************************************/

  public PrefixTreeBlockMeta getBlockMeta() {
    return blockMeta;
  }

  public Tokenizer getRowTokenizer() {
    return rowTokenizer;
  }

  public LongEncoder getTimestampEncoder() {
    return timestampEncoder;
  }

  public int getTotalBytes() {
    return totalBytes;
  }

  public long[] getTimestamps() {
    return timestamps;
  }

  public long[] getMvccVersions() {
    return mvccVersions;
  }

  public byte[] getTypeBytes() {
    return typeBytes;
  }

  public LongEncoder getMvccVersionEncoder() {
    return mvccVersionEncoder;
  }

  public ByteRangeSet getFamilySorter() {
    return familyDeduplicator;
  }

  public ByteRangeSet getQualifierSorter() {
    return qualifierDeduplicator;
  }

  public ByteRangeSet getTagSorter() {
    return tagsDeduplicator;
  }

  public ColumnSectionWriter getFamilyWriter() {
    return familyWriter;
  }

  public ColumnSectionWriter getQualifierWriter() {
    return qualifierWriter;
  }

  public ColumnSectionWriter getTagWriter() {
    return tagsWriter;
  }

  public RowSectionWriter getRowWriter() {
    return rowWriter;
  }

  public ByteRange getValueByteRange() {
    return new SimpleMutableByteRange(values, 0, totalValueBytes);
  }

}
