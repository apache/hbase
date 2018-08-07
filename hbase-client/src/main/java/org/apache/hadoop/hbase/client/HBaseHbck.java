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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableStateResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.HbckService.BlockingInterface;


/**
 * Use {@link ClusterConnection#getHbck()} to obtain an instance of {@link Hbck} instead of
 * constructing
 * an HBaseHbck directly. This will be mostly used by hbck tool.
 *
 * <p>Connection should be an <i>unmanaged</i> connection obtained via
 * {@link ConnectionFactory#createConnection(Configuration)}.</p>
 *
 * <p>An instance of this class is lightweight and not-thread safe. A new instance should be created
 * by each thread. Pooling or caching of the instance is not recommended.</p>
 *
 * @see ConnectionFactory
 * @see ClusterConnection
 * @see Hbck
 */
@InterfaceAudience.Private
public class HBaseHbck implements Hbck {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseHbck.class);

  private boolean aborted;
  private final BlockingInterface hbck;

  private RpcControllerFactory rpcControllerFactory;

  HBaseHbck(ClusterConnection connection, BlockingInterface hbck) throws IOException {
    this.hbck = hbck;
    this.rpcControllerFactory = connection.getRpcControllerFactory();
  }

  @Override
  public void close() throws IOException {
    // currently does nothing
  }

  @Override
  public void abort(String why, Throwable e) {
    this.aborted = true;
    // Currently does nothing but throw the passed message and exception
    throw new RuntimeException(why, e);
  }

  @Override
  public boolean isAborted() {
    return this.aborted;
  }

  /**
   * NOTE: This is a dangerous action, as existing running procedures for the table or regions
   * which belong to the table may get confused.
   */
  @Override
  public TableState setTableStateInMeta(TableState state) throws IOException {
    try {
      GetTableStateResponse response = hbck.setTableStateInMeta(
          rpcControllerFactory.newController(),
          RequestConverter.buildSetTableStateInMetaRequest(state));
      return TableState.convert(state.getTableName(), response.getTableState());
    } catch (ServiceException se) {
      LOG.debug("ServiceException while updating table state in meta. table={}, state={}",
          state.getTableName(), state.getState());
      throw new IOException(se);
    }
  }
}
