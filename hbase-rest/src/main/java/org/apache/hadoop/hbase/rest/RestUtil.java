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
package org.apache.hadoop.hbase.rest;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Message;
import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class RestUtil {

  private RestUtil() {
    // Do not instantiate
  }

  /**
   * Speed-optimized method to convert an HBase result to a RowModel. Avoids iterators and uses the
   * non-cloning constructors to minimize overhead, especially when using protobuf marshalling.
   * @param r non-empty Result object
   */
  public static RowModel createRowModelFromResult(Result r) {
    Cell firstCell = r.rawCells()[0];
    RowModel rowModel =
      new RowModel(firstCell.getRowArray(), firstCell.getRowOffset(), firstCell.getRowLength());
    int cellsLength = r.rawCells().length;
    for (int i = 0; i < cellsLength; i++) {
      rowModel.addCell(new CellModel(r.rawCells()[i]));
    }
    return rowModel;
  }

  /**
   * Merges the object from codedInput, then calls checkLastTagWas. This is based on
   * ProtobufUtil.mergeFrom, but we have already taken care of setSizeLimit() before calling, so
   * only the checkLastTagWas() call is retained.
   * @param builder    protobuf object builder
   * @param codedInput encoded object data
   */
  public static void mergeFrom(Message.Builder builder, CodedInputStream codedInput)
    throws IOException {
    builder.mergeFrom(codedInput);
    codedInput.checkLastTagWas(0);
  }
}
