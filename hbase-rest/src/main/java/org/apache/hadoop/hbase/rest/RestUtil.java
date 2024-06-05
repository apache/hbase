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
   * Copied from org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil to avoid shading conflicts
   * between hbase-shaded-client and hbase-rest in HBase 2.x. This version of protobuf's mergeFrom
   * avoids the hard-coded 64MB limit for decoding buffers when working with byte arrays
   * @param builder current message builder
   * @param b       byte array
   */
  public static void mergeFrom(Message.Builder builder, byte[] b) throws IOException {
    final CodedInputStream codedInput = CodedInputStream.newInstance(b);
    codedInput.setSizeLimit(b.length);
    builder.mergeFrom(codedInput);
    codedInput.checkLastTagWas(0);
  }
}
