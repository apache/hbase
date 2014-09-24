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

package org.apache.hadoop.hbase.codec.prefixtree.encode.row;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeBlockMeta;
import org.apache.hadoop.hbase.codec.prefixtree.encode.PrefixTreeEncoder;
import org.apache.hadoop.hbase.codec.prefixtree.encode.tokenize.TokenizerNode;
import org.apache.hadoop.hbase.util.vint.UFIntTool;

import com.google.common.collect.Lists;

/**
 * Most of the complexity of the PrefixTree is contained in the "row section". It contains the row
 * key trie structure used to search and recreate all the row keys. Each nub and leaf in this trie
 * also contains references to offsets in the other sections of the data block that enable the
 * decoder to match a row key with its qualifier, timestamp, type, value, etc.
 * <p>
 * The row section is a concatenated collection of {@link RowNodeWriter}s. See that class for the
 * internals of each row node.
 */
@InterfaceAudience.Private
public class RowSectionWriter {

  /***************** fields **************************/

  protected PrefixTreeEncoder prefixTreeEncoder;

  protected PrefixTreeBlockMeta blockMeta;

  protected int numBytes;

  protected ArrayList<TokenizerNode> nonLeaves;
  protected ArrayList<TokenizerNode> leaves;

  protected ArrayList<RowNodeWriter> leafWriters;
  protected ArrayList<RowNodeWriter> nonLeafWriters;

  protected int numLeafWriters;
  protected int numNonLeafWriters;


  /********************* construct **********************/

  public RowSectionWriter() {
    this.nonLeaves = Lists.newArrayList();
    this.leaves = Lists.newArrayList();
    this.leafWriters = Lists.newArrayList();
    this.nonLeafWriters = Lists.newArrayList();
  }

  public RowSectionWriter(PrefixTreeEncoder prefixTreeEncoder) {
    reconstruct(prefixTreeEncoder);
  }

  public void reconstruct(PrefixTreeEncoder prefixTreeEncoder) {
    this.prefixTreeEncoder = prefixTreeEncoder;
    this.blockMeta = prefixTreeEncoder.getBlockMeta();
    reset();
  }

  public void reset() {
    numBytes = 0;
    nonLeaves.clear();
    leaves.clear();
    numLeafWriters = 0;
    numNonLeafWriters = 0;
  }


  /****************** methods *******************************/

  public RowSectionWriter compile() {
    blockMeta.setMaxRowLength(prefixTreeEncoder.getRowTokenizer().getMaxElementLength());
    prefixTreeEncoder.getRowTokenizer().setNodeFirstInsertionIndexes();

    prefixTreeEncoder.getRowTokenizer().appendNodes(nonLeaves, true, false);
    prefixTreeEncoder.getRowTokenizer().appendNodes(leaves, false, true);

    // track the starting position of each node in final output
    int negativeIndex = 0;

    // create leaf writer nodes
    // leaf widths are known at this point, so add them up
    int totalLeafBytes = 0;
    for (int i = leaves.size() - 1; i >= 0; --i) {
      TokenizerNode leaf = leaves.get(i);
      RowNodeWriter leafWriter = initializeWriter(leafWriters, numLeafWriters, leaf);
      ++numLeafWriters;
      // leaves store all but their first token byte
      int leafNodeWidth = leafWriter.calculateWidthOverrideOffsetWidth(0);
      totalLeafBytes += leafNodeWidth;
      negativeIndex += leafNodeWidth;
      leaf.setNegativeIndex(negativeIndex);
    }

    int totalNonLeafBytesWithoutOffsets = 0;
    int totalChildPointers = 0;
    for (int i = nonLeaves.size() - 1; i >= 0; --i) {
      TokenizerNode nonLeaf = nonLeaves.get(i);
      RowNodeWriter nonLeafWriter = initializeWriter(nonLeafWriters, numNonLeafWriters, nonLeaf);
      ++numNonLeafWriters;
      totalNonLeafBytesWithoutOffsets += nonLeafWriter.calculateWidthOverrideOffsetWidth(0);
      totalChildPointers += nonLeaf.getNumChildren();
    }

    // figure out how wide our offset FInts are
    int offsetWidth = 0;
    while (true) {
      ++offsetWidth;
      int offsetBytes = totalChildPointers * offsetWidth;
      int totalRowBytes = totalNonLeafBytesWithoutOffsets + offsetBytes + totalLeafBytes;
      if (totalRowBytes < UFIntTool.maxValueForNumBytes(offsetWidth)) {
        // it fits
        numBytes = totalRowBytes;
        break;
      }
    }
    blockMeta.setNextNodeOffsetWidth(offsetWidth);

    // populate negativeIndexes
    for (int i = nonLeaves.size() - 1; i >= 0; --i) {
      TokenizerNode nonLeaf = nonLeaves.get(i);
      int writerIndex = nonLeaves.size() - i - 1;
      RowNodeWriter nonLeafWriter = nonLeafWriters.get(writerIndex);
      int nodeWidth = nonLeafWriter.calculateWidth();
      negativeIndex += nodeWidth;
      nonLeaf.setNegativeIndex(negativeIndex);
    }

    return this;
  }

  protected RowNodeWriter initializeWriter(List<RowNodeWriter> list, int index,
      TokenizerNode builderNode) {
    RowNodeWriter rowNodeWriter = null;
    //check if there is an existing node we can recycle
    if (index >= list.size()) {
      //there are not enough existing nodes, so add a new one which will be retrieved below
      list.add(new RowNodeWriter(prefixTreeEncoder, builderNode));
    }
    rowNodeWriter = list.get(index);
    rowNodeWriter.reset(builderNode);
    return rowNodeWriter;
  }


  public void writeBytes(OutputStream os) throws IOException {
    for (int i = numNonLeafWriters - 1; i >= 0; --i) {
      RowNodeWriter nonLeafWriter = nonLeafWriters.get(i);
      nonLeafWriter.write(os);
    }
    // duplicates above... written more for clarity right now
    for (int i = numLeafWriters - 1; i >= 0; --i) {
      RowNodeWriter leafWriter = leafWriters.get(i);
      leafWriter.write(os);
    }
  }


  /***************** static ******************************/

  protected static ArrayList<TokenizerNode> filterByLeafAndReverse(
      ArrayList<TokenizerNode> ins, boolean leaves) {
    ArrayList<TokenizerNode> outs = Lists.newArrayList();
    for (int i = ins.size() - 1; i >= 0; --i) {
      TokenizerNode n = ins.get(i);
      if (n.isLeaf() && leaves || (!n.isLeaf() && !leaves)) {
        outs.add(ins.get(i));
      }
    }
    return outs;
  }


  /************* get/set **************************/

  public int getNumBytes() {
    return numBytes;
  }

  public ArrayList<TokenizerNode> getNonLeaves() {
    return nonLeaves;
  }

  public ArrayList<TokenizerNode> getLeaves() {
    return leaves;
  }

  public ArrayList<RowNodeWriter> getNonLeafWriters() {
    return nonLeafWriters;
  }

  public ArrayList<RowNodeWriter> getLeafWriters() {
    return leafWriters;
  }

}
