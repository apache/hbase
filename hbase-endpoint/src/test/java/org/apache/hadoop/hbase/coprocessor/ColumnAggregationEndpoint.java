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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;

import org.apache.hadoop.hbase.shaded.coprocessor.protobuf.generated.ColumnAggregationProtos.ColumnAggregationService;
import org.apache.hadoop.hbase.shaded.coprocessor.protobuf.generated.ColumnAggregationProtos.SumRequest;
import org.apache.hadoop.hbase.shaded.coprocessor.protobuf.generated.ColumnAggregationProtos.SumResponse;

/**
 * The aggregation implementation at a region.
 */
public class ColumnAggregationEndpoint extends ColumnAggregationService
        implements RegionCoprocessor {
  private static final Logger LOG = LoggerFactory.getLogger(ColumnAggregationEndpoint.class);
  private RegionCoprocessorEnvironment env = null;

  @Override
  public Iterable<Service> getServices() {
    return Collections.singleton(this);
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
  public void sum(RpcController controller, SumRequest request, RpcCallback<SumResponse> done) {
    // aggregate at each region
    Scan scan = new Scan();
    // Family is required in pb. Qualifier is not.
    byte [] family = request.getFamily().toByteArray();
    byte [] qualifier = request.hasQualifier()? request.getQualifier().toByteArray(): null;
    if (request.hasQualifier()) {
      scan.addColumn(family, qualifier);
    } else {
      scan.addFamily(family);
    }
    int sumResult = 0;
    InternalScanner scanner = null;
    try {
      scanner = this.env.getRegion().getScanner(scan);
      List<Cell> curVals = new ArrayList<>();
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
      CoprocessorRpcUtils.setControllerException(controller, e);
      // Set result to -1 to indicate error.
      sumResult = -1;
      LOG.info("Setting sum result to -1 to indicate error", e);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
          CoprocessorRpcUtils.setControllerException(controller, e);
          sumResult = -1;
          LOG.info("Setting sum result to -1 to indicate error", e);
        }
      }
    }
    LOG.info("Returning result " + sumResult);
    done.run(SumResponse.newBuilder().setSum(sumResult).build());
  }
}
