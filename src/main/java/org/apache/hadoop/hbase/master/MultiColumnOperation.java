/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.master;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;

/** Instantiated to process a batch of table alterations */
class MultiColumnOperation extends ColumnOperation {
  private final List<ColumnOperation> operations;

  MultiColumnOperation(final HMaster master, final byte [] tableName,
      final List<HColumnDescriptor> columnAdditions, 
      final List<Pair<byte [], HColumnDescriptor>> columnModifications, 
      final List<byte []> columnDeletions) throws IOException {
    super(master, tableName);
    // convert the three separate lists to an internal list of sub-operations
    List<ColumnOperation> argsAsOperations = new ArrayList<ColumnOperation>();
    if (columnAdditions != null) {
      for (HColumnDescriptor newColumn : columnAdditions) {
        argsAsOperations.add(new AddColumn(master, tableName, newColumn));
      }
    }
    if (columnModifications != null) {
      for (Pair<byte [], HColumnDescriptor> modColumn : columnModifications) {
        argsAsOperations.add(new ModifyColumn(
              master, tableName, modColumn.getFirst(), modColumn.getSecond()));
      }
    }
    if (columnDeletions != null) {
      for (byte [] columnToReap : columnDeletions) {
        argsAsOperations.add(new DeleteColumn(master, tableName, columnToReap));
      }
    }
    this.operations = argsAsOperations;
  }

  @Override
  protected void postProcess(HRegionInfo hri) throws IOException {
    // just ask all of the sub-operations to post-process
    for (ColumnOperation op : operations) {
      op.postProcess(hri);
    }
  }

  @Override
  protected void updateTableDescriptor(HTableDescriptor desc) 
  throws IOException {
    // just ask all of the sub-operations to update the descriptor
    for (ColumnOperation op : operations) {
      op.updateTableDescriptor(desc);
    }
  }
}

