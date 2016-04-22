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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ColumnAggregationWithErrorsProtos;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ColumnAggregationWithErrorsProtos.ColumnAggregationWithErrorsSumRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ColumnAggregationWithErrorsProtos.ColumnAggregationWithErrorsSumResponse;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * Test coprocessor endpoint that always throws a {@link DoNotRetryIOException} for requests on
 * the last region in the table.  This allows tests to ensure correct error handling of
 * coprocessor endpoints throwing exceptions.
 */
public class ColumnAggregationEndpointWithErrors
    extends
    ColumnAggregationWithErrorsProtos.ColumnAggregationServiceWithErrors
implements Coprocessor, CoprocessorService  {
  private static final Log LOG = LogFactory.getLog(ColumnAggregationEndpointWithErrors.class);
  private RegionCoprocessorEnvironment env = null;
  @Override
  public Service getService() {
    return this;
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionCoprocessorEnvironment) {
      this.env = (RegionCoprocessorEnvironment)env;
      return;
    }
    throw new CoprocessorException("Must be loaded on a table region!");
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    // Nothing to do.
  }

  @Override
  public void sum(RpcController controller, ColumnAggregationWithErrorsSumRequest request, 
      RpcCallback<ColumnAggregationWithErrorsSumResponse> done) {
    // aggregate at each region
    Scan scan = new Scan();
    // Family is required in pb. Qualifier is not.
    byte[] family = request.getFamily().toByteArray();
    byte[] qualifier = request.hasQualifier() ? request.getQualifier().toByteArray() : null;
    if (request.hasQualifier()) {
      scan.addColumn(family, qualifier);
    } else {
      scan.addFamily(family);
    }
    int sumResult = 0;
    InternalScanner scanner = null;
    try {
      Region region = this.env.getRegion();
      // throw an exception for requests to the last region in the table, to test error handling
      if (Bytes.equals(region.getRegionInfo().getEndKey(), HConstants.EMPTY_END_ROW)) {
        throw new DoNotRetryIOException("An expected exception");
      }
      scanner = region.getScanner(scan);
      List<Cell> curVals = new ArrayList<Cell>();
      boolean hasMore = false;
      do {
        curVals.clear();
        hasMore = scanner.next(curVals);
        for (Cell kv : curVals) {
          if (CellUtil.matchingQualifier(kv, qualifier)) {
            sumResult += Bytes.toInt(kv.getValueArray(), kv.getValueOffset());
          }
        }
      } while (hasMore);
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
      // Set result to -1 to indicate error.
      sumResult = -1;
      LOG.info("Setting sum result to -1 to indicate error", e);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
          ResponseConverter.setControllerException(controller, e);
          sumResult = -1;
          LOG.info("Setting sum result to -1 to indicate error", e);
        }
      }
    }
    done.run(ColumnAggregationWithErrorsSumResponse.newBuilder().setSum(sumResult).build());
  }
}
