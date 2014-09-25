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

package org.apache.hadoop.hbase.codec.prefixtree.encode.column;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeBlockMeta;
import org.apache.hadoop.hbase.codec.prefixtree.encode.other.ColumnNodeType;
import org.apache.hadoop.hbase.codec.prefixtree.encode.tokenize.Tokenizer;
import org.apache.hadoop.hbase.codec.prefixtree.encode.tokenize.TokenizerNode;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.apache.hadoop.hbase.util.vint.UFIntTool;

import com.google.common.collect.Lists;

/**
 * Takes the tokenized family or qualifier data and flattens it into a stream of bytes. The family
 * section is written after the row section, and qualifier section after family section.
 * <p/>
 * The family and qualifier tries, or "column tries", are structured differently than the row trie.
 * The trie cannot be reassembled without external data about the offsets of the leaf nodes, and
 * these external pointers are stored in the nubs and leaves of the row trie. For each cell in a
 * row, the row trie contains a list of offsets into the column sections (along with pointers to
 * timestamps and other per-cell fields). These offsets point to the last column node/token that
 * comprises the column name. To assemble the column name, the trie is traversed in reverse (right
 * to left), with the rightmost tokens pointing to the start of their "parent" node which is the
 * node to the left.
 * <p/>
 * This choice was made to reduce the size of the column trie by storing the minimum amount of
 * offset data. As a result, to find a specific qualifier within a row, you must do a binary search
 * of the column nodes, reassembling each one as you search. Future versions of the PrefixTree might
 * encode the columns in both a forward and reverse trie, which would convert binary searches into
 * more efficient trie searches which would be beneficial for wide rows.
 */
@InterfaceAudience.Private
public class ColumnSectionWriter {

  public static final int EXPECTED_NUBS_PLUS_LEAVES = 100;

  /****************** fields ****************************/

  private PrefixTreeBlockMeta blockMeta;

  private ColumnNodeType nodeType;
  private Tokenizer tokenizer;
  private int numBytes = 0;
  private ArrayList<TokenizerNode> nonLeaves;
  private ArrayList<TokenizerNode> leaves;
  private ArrayList<TokenizerNode> allNodes;
  private ArrayList<ColumnNodeWriter> columnNodeWriters;
  private List<Integer> outputArrayOffsets;


	/*********************** construct *********************/

  public ColumnSectionWriter() {
    this.nonLeaves = Lists.newArrayList();
    this.leaves = Lists.newArrayList();
    this.outputArrayOffsets = Lists.newArrayList();
  }

  public ColumnSectionWriter(PrefixTreeBlockMeta blockMeta, Tokenizer builder,
      ColumnNodeType nodeType) {
    this();// init collections
    reconstruct(blockMeta, builder, nodeType);
  }

  public void reconstruct(PrefixTreeBlockMeta blockMeta, Tokenizer builder,
      ColumnNodeType nodeType) {
    this.blockMeta = blockMeta;
    this.tokenizer = builder;
    this.nodeType = nodeType;
  }

  public void reset() {
    numBytes = 0;
    nonLeaves.clear();
    leaves.clear();
    outputArrayOffsets.clear();
  }


	/****************** methods *******************************/

  public ColumnSectionWriter compile() {
    if (this.nodeType == ColumnNodeType.FAMILY) {
      // do nothing. max family length fixed at Byte.MAX_VALUE
    } else if (this.nodeType == ColumnNodeType.QUALIFIER) {
      blockMeta.setMaxQualifierLength(tokenizer.getMaxElementLength());
    } else {
      blockMeta.setMaxTagsLength(tokenizer.getMaxElementLength());
    }
    compilerInternals();
    return this;
  }

  protected void compilerInternals() {
    tokenizer.setNodeFirstInsertionIndexes();
    tokenizer.appendNodes(nonLeaves, true, false);

    tokenizer.appendNodes(leaves, false, true);

    allNodes = Lists.newArrayListWithCapacity(nonLeaves.size() + leaves.size());
    allNodes.addAll(nonLeaves);
    allNodes.addAll(leaves);

    columnNodeWriters = Lists.newArrayListWithCapacity(CollectionUtils.nullSafeSize(allNodes));
    for (int i = 0; i < allNodes.size(); ++i) {
      TokenizerNode node = allNodes.get(i);
      columnNodeWriters.add(new ColumnNodeWriter(blockMeta, node, this.nodeType));
    }

    // leaf widths are known at this point, so add them up
    int totalBytesWithoutOffsets = 0;
    for (int i = allNodes.size() - 1; i >= 0; --i) {
      ColumnNodeWriter columnNodeWriter = columnNodeWriters.get(i);
      // leaves store all but their first token byte
      totalBytesWithoutOffsets += columnNodeWriter.getWidthUsingPlaceholderForOffsetWidth(0);
    }

    // figure out how wide our offset FInts are
    int parentOffsetWidth = 0;
    while (true) {
      ++parentOffsetWidth;
      int numBytesFinder = totalBytesWithoutOffsets + parentOffsetWidth * allNodes.size();
      if (numBytesFinder < UFIntTool.maxValueForNumBytes(parentOffsetWidth)) {
        numBytes = numBytesFinder;
        break;
      }// it fits
    }
    if (this.nodeType == ColumnNodeType.FAMILY) {
      blockMeta.setFamilyOffsetWidth(parentOffsetWidth);
    } else if (this.nodeType == ColumnNodeType.QUALIFIER) {
      blockMeta.setQualifierOffsetWidth(parentOffsetWidth);
    } else {
      blockMeta.setTagsOffsetWidth(parentOffsetWidth);
    }

    int forwardIndex = 0;
    for (int i = 0; i < allNodes.size(); ++i) {
      TokenizerNode node = allNodes.get(i);
      ColumnNodeWriter columnNodeWriter = columnNodeWriters.get(i);
      int fullNodeWidth = columnNodeWriter
          .getWidthUsingPlaceholderForOffsetWidth(parentOffsetWidth);
      node.setOutputArrayOffset(forwardIndex);
      columnNodeWriter.setTokenBytes(node.getToken());
      if (node.isRoot()) {
        columnNodeWriter.setParentStartPosition(0);
      } else {
        columnNodeWriter.setParentStartPosition(node.getParent().getOutputArrayOffset());
      }
      forwardIndex += fullNodeWidth;
    }

    tokenizer.appendOutputArrayOffsets(outputArrayOffsets);
  }

  public void writeBytes(OutputStream os) throws IOException {
    for (ColumnNodeWriter columnNodeWriter : columnNodeWriters) {
      columnNodeWriter.writeBytes(os);
    }
  }


  /************* get/set **************************/

  public ArrayList<ColumnNodeWriter> getColumnNodeWriters() {
    return columnNodeWriters;
  }

  public int getNumBytes() {
    return numBytes;
  }

  public int getOutputArrayOffset(int sortedIndex) {
    return outputArrayOffsets.get(sortedIndex);
  }

  public ArrayList<TokenizerNode> getNonLeaves() {
    return nonLeaves;
  }

  public ArrayList<TokenizerNode> getLeaves() {
    return leaves;
  }

}
