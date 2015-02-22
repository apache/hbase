/**
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

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.hadoop.hbase.util.Bytes;


public class ProtobufStreamingUtil implements StreamingOutput {

  private static final Log LOG = LogFactory.getLog(ProtobufStreamingUtil.class);
  private String contentType;
  private ResultScanner resultScanner;
  private int limit;
  private int fetchSize;

  protected ProtobufStreamingUtil(ResultScanner scanner, String type, int limit, int fetchSize) {
    this.resultScanner = scanner;
    this.contentType = type;
    this.limit = limit;
    this.fetchSize = fetchSize;
    LOG.debug("Created ScanStreamingUtil with content type = " + this.contentType + " user limit : "
        + this.limit + " scan fetch size : " + this.fetchSize);
  }

  @Override
  public void write(OutputStream outStream) throws IOException, WebApplicationException {
    Result[] rowsToSend;
    if(limit < fetchSize){
      rowsToSend = this.resultScanner.next(limit);
      writeToStream(createModelFromResults(rowsToSend), this.contentType, outStream);
    } else {
      int count = limit;
      while (count > 0) {
        if (count < fetchSize) {
          rowsToSend = this.resultScanner.next(count);
        } else {
          rowsToSend = this.resultScanner.next(this.fetchSize);
        }
        if(rowsToSend.length == 0){
          break;
        }
        count = count - rowsToSend.length;
        writeToStream(createModelFromResults(rowsToSend), this.contentType, outStream);
      }
    }
  }

  private void writeToStream(CellSetModel model, String contentType, OutputStream outStream)
      throws IOException {
    byte[] objectBytes = model.createProtobufOutput();
    outStream.write(Bytes.toBytes((short)objectBytes.length));
    outStream.write(objectBytes);
    outStream.flush();
    LOG.trace("Wrote " + model.getRows().size() + " rows to stream successfully.");
  }

  private CellSetModel createModelFromResults(Result[] results) {
    CellSetModel cellSetModel = new CellSetModel();
    for (Result rs : results) {
      byte[] rowKey = rs.getRow();
      RowModel rModel = new RowModel(rowKey);
      List<Cell> kvs = rs.listCells();
      for (Cell kv : kvs) {
        rModel.addCell(new CellModel(CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv), kv
            .getTimestamp(), CellUtil.cloneValue(kv)));
      }
      cellSetModel.addRow(rModel);
    }
    return cellSetModel;
  }
}
