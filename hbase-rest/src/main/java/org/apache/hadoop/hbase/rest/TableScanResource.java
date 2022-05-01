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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.javax.ws.rs.GET;
import org.apache.hbase.thirdparty.javax.ws.rs.HeaderParam;
import org.apache.hbase.thirdparty.javax.ws.rs.Produces;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Response;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Response.ResponseBuilder;
import org.apache.hbase.thirdparty.javax.ws.rs.core.StreamingOutput;
import org.apache.hbase.thirdparty.javax.ws.rs.core.UriInfo;

@InterfaceAudience.Private
public class TableScanResource extends ResourceBase {
  private static final Logger LOG = LoggerFactory.getLogger(TableScanResource.class);

  TableResource tableResource;
  ResultScanner results;
  int userRequestedLimit;

  public TableScanResource(ResultScanner scanner, int userRequestedLimit) throws IOException {
    super();
    this.results = scanner;
    this.userRequestedLimit = userRequestedLimit;
  }

  @GET
  @Produces({ Constants.MIMETYPE_XML, Constants.MIMETYPE_JSON })
  public CellSetModelStream get(final @Context UriInfo uriInfo) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("GET " + uriInfo.getAbsolutePath());
    }
    servlet.getMetrics().incrementRequests(1);
    final int rowsToSend = userRequestedLimit;
    servlet.getMetrics().incrementSucessfulScanRequests(1);
    final Iterator<Result> itr = results.iterator();
    return new CellSetModelStream(new ArrayList<RowModel>() {
      @Override
      public Iterator<RowModel> iterator() {
        return new Iterator<RowModel>() {
          int count = rowsToSend;

          @Override
          public boolean hasNext() {
            return count > 0 && itr.hasNext();
          }

          @Override
          public RowModel next() {
            Result rs = itr.next();
            if ((rs == null) || (count <= 0)) {
              return null;
            }
            byte[] rowKey = rs.getRow();
            RowModel rModel = new RowModel(rowKey);
            List<Cell> kvs = rs.listCells();
            for (Cell kv : kvs) {
              rModel.addCell(new CellModel(CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv),
                kv.getTimestamp(), CellUtil.cloneValue(kv)));
            }
            count--;
            if (count == 0) {
              results.close();
            }
            return rModel;
          }
        };
      }
    });
  }

  @GET
  @Produces({ Constants.MIMETYPE_PROTOBUF, Constants.MIMETYPE_PROTOBUF_IETF })
  public Response getProtobuf(final @Context UriInfo uriInfo,
    final @HeaderParam("Accept") String contentType) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("GET " + uriInfo.getAbsolutePath() + " as " + MIMETYPE_BINARY);
    }
    servlet.getMetrics().incrementRequests(1);
    try {
      int fetchSize = this.servlet.getConfiguration().getInt(Constants.SCAN_FETCH_SIZE, 10);
      StreamingOutput stream =
        new ProtobufStreamingOutput(this.results, contentType, userRequestedLimit, fetchSize);
      servlet.getMetrics().incrementSucessfulScanRequests(1);
      ResponseBuilder response = Response.ok(stream);
      response.header("content-type", contentType);
      return response.build();
    } catch (Exception exp) {
      servlet.getMetrics().incrementFailedScanRequests(1);
      processException(exp);
      LOG.warn(exp.toString(), exp);
      return null;
    }
  }

  @XmlRootElement(name = "CellSet")
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class CellSetModelStream {
    // JAXB needs an arraylist for streaming
    @XmlElement(name = "Row")
    @JsonIgnore
    private ArrayList<RowModel> Row;

    public CellSetModelStream() {
    }

    public CellSetModelStream(final ArrayList<RowModel> rowList) {
      this.Row = rowList;
    }

    // jackson needs an iterator for streaming
    @JsonProperty("Row")
    public Iterator<RowModel> getIterator() {
      return Row.iterator();
    }
  }
}
