/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.MultiThreadedAction.DefaultDataGenerator;

@InterfaceAudience.Private
public class LoadTestDataGeneratorWithTags extends DefaultDataGenerator {

  private int minNumTags, maxNumTags;
  private int minTagLength, maxTagLength;
  private Random random = new Random();

  public LoadTestDataGeneratorWithTags(int minValueSize, int maxValueSize, int minColumnsPerKey,
      int maxColumnsPerKey, byte[]... columnFamilies) {
    super(minValueSize, maxValueSize, minColumnsPerKey, maxColumnsPerKey, columnFamilies);
  }

  @Override
  public void initialize(String[] args) {
    super.initialize(args);
    if (args.length != 4) {
      throw new IllegalArgumentException("LoadTestDataGeneratorWithTags must have "
          + "4 initialization arguments. ie. minNumTags:maxNumTags:minTagLength:maxTagLength");
    }
    // 1st arg in args is the min number of tags to be used with every cell
    this.minNumTags = Integer.parseInt(args[0]);
    // 2nd arg in args is the max number of tags to be used with every cell
    this.maxNumTags = Integer.parseInt(args[1]);
    // 3rd arg in args is the min tag length
    this.minTagLength = Integer.parseInt(args[2]);
    // 4th arg in args is the max tag length
    this.maxTagLength = Integer.parseInt(args[3]);
  }

  @Override
  public Mutation beforeMutate(long rowkeyBase, Mutation m) throws IOException {
    if (m instanceof Put) {
      List<Cell> updatedCells = new ArrayList<Cell>();
      int numTags;
      if (minNumTags == maxNumTags) {
        numTags = minNumTags;
      } else {
        numTags = minNumTags + random.nextInt(maxNumTags - minNumTags);
      }
      List<Tag> tags;
      for (CellScanner cellScanner = m.cellScanner(); cellScanner.advance();) {
        Cell cell = cellScanner.current();
        byte[] tag = LoadTestTool.generateData(random,
            minTagLength + random.nextInt(maxTagLength - minTagLength));
        tags = new ArrayList<Tag>();
        for (int n = 0; n < numTags; n++) {
          tags.add(new ArrayBackedTag((byte) 127, tag));
        }
        Cell updatedCell = new KeyValue(cell.getRowArray(), cell.getRowOffset(),
            cell.getRowLength(), cell.getFamilyArray(), cell.getFamilyOffset(),
            cell.getFamilyLength(), cell.getQualifierArray(), cell.getQualifierOffset(),
            cell.getQualifierLength(), cell.getTimestamp(), Type.codeToType(cell.getTypeByte()),
            cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(), tags);
        updatedCells.add(updatedCell);
      }
      m.getFamilyCellMap().clear();
      // Clear and add new Cells to the Mutation.
      for (Cell cell : updatedCells) {
        ((Put) m).add(cell);
      }
    }
    return m;
  }
}
