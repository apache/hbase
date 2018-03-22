/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor.example;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.example.generated.ExampleProtos;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Sample coprocessor endpoint exposing a Service interface for counting rows and key values.
 *
 * <p>
 * For the protocol buffer definition of the RowCountService, see the source file located under
 * hbase-examples/src/main/protobuf/Examples.proto.
 * </p>
 */
@InterfaceAudience.Private
public class RowCountEndpoint extends ExampleProtos.RowCountService implements RegionCoprocessor {
  private RegionCoprocessorEnvironment env;

  public RowCountEndpoint() {
  }

  /**
   * Just returns a reference to this object, which implements the RowCounterService interface.
   */
  @Override
  public Iterable<Service> getServices() {
    return Collections.singleton(this);
  }

  /**
   * Returns a count of the rows in the region where this coprocessor is loaded.
   */
  @Override
  public void getRowCount(RpcController controller, ExampleProtos.CountRequest request,
                          RpcCallback<ExampleProtos.CountResponse> done) {
    Scan scan = new Scan();
    scan.setFilter(new FirstKeyOnlyFilter());
    ExampleProtos.CountResponse response = null;
    InternalScanner scanner = null;
    try {
      scanner = env.getRegion().getScanner(scan);
      List<Cell> results = new ArrayList<>();
      boolean hasMore = false;
      byte[] lastRow = null;
      long count = 0;
      do {
        hasMore = scanner.next(results);
        for (Cell kv : results) {
          byte[] currentRow = CellUtil.cloneRow(kv);
          if (lastRow == null || !Bytes.equals(lastRow, currentRow)) {
            lastRow = currentRow;
            count++;
          }
        }
        results.clear();
      } while (hasMore);

      response = ExampleProtos.CountResponse.newBuilder()
          .setCount(count).build();
    } catch (IOException ioe) {
      CoprocessorRpcUtils.setControllerException(controller, ioe);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ignored) {}
      }
    }
    done.run(response);
  }

  /**
   * Returns a count of all KeyValues in the region where this coprocessor is loaded.
   */
  @Override
  public void getKeyValueCount(RpcController controller, ExampleProtos.CountRequest request,
                               RpcCallback<ExampleProtos.CountResponse> done) {
    ExampleProtos.CountResponse response = null;
    InternalScanner scanner = null;
    try {
      scanner = env.getRegion().getScanner(new Scan());
      List<Cell> results = new ArrayList<>();
      boolean hasMore = false;
      long count = 0;
      do {
        hasMore = scanner.next(results);
        for (Cell kv : results) {
          count++;
        }
        results.clear();
      } while (hasMore);

      response = ExampleProtos.CountResponse.newBuilder()
          .setCount(count).build();
    } catch (IOException ioe) {
      CoprocessorRpcUtils.setControllerException(controller, ioe);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ignored) {}
      }
    }
    done.run(response);
  }

  /**
   * Stores a reference to the coprocessor environment provided by the
   * {@link org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost} from the region where this
   * coprocessor is loaded.  Since this is a coprocessor endpoint, it always expects to be loaded
   * on a table region, so always expects this to be an instance of
   * {@link RegionCoprocessorEnvironment}.
   * @param env the environment provided by the coprocessor host
   * @throws IOException if the provided environment is not an instance of
   * {@code RegionCoprocessorEnvironment}
   */
  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionCoprocessorEnvironment) {
      this.env = (RegionCoprocessorEnvironment)env;
    } else {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    // nothing to do
  }
}
