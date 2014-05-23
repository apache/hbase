/**
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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;

import com.google.protobuf.ServiceException;

/**
 * A region server that will OOME.
 * Everytime {@link #put(regionName, Durability)} is called, we add
 * keep around a reference to the batch.  Use this class to test OOME extremes.
 * Needs to be started manually as in
 * <code>${HBASE_HOME}/bin/hbase ./bin/hbase org.apache.hadoop.hbase.OOMERegionServer start</code>.
 */
public class OOMERegionServer extends HRegionServer {
  private List<Put> retainer = new ArrayList<Put>();

  public OOMERegionServer(HBaseConfiguration conf, CoordinatedStateManager cp)
      throws IOException, InterruptedException {
    super(conf, cp);
  }

  public void put(byte [] regionName, Put put)
  throws IOException {
    try {
      MutateRequest request =
        RequestConverter.buildMutateRequest(regionName, put);
      rpcServices.mutate(null, request);
      for (int i = 0; i < 30; i++) {
        // Add the batch update 30 times to bring on the OOME faster.
        this.retainer.add(put);
      }
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  public static void main(String[] args) {
    new HRegionServerCommandLine(OOMERegionServer.class).doMain(args);
  }
}
