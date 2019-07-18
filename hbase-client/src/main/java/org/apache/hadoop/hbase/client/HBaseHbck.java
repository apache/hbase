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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableStateResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.HbckService.BlockingInterface;


/**
 * Use {@link Connection#getHbck()} to obtain an instance of {@link Hbck} instead of
 * constructing an HBaseHbck directly.
 *
 * <p>Connection should be an <i>unmanaged</i> connection obtained via
 * {@link ConnectionFactory#createConnection(Configuration)}.</p>
 *
 * <p>NOTE: The methods in here can do damage to a cluster if applied in the wrong sequence or at
 * the wrong time. Use with caution. For experts only. These methods are only for the
 * extreme case where the cluster has been damaged or has achieved an inconsistent state because
 * of some unforeseen circumstance or bug and requires manual intervention.
 *
 * <p>An instance of this class is lightweight and not-thread safe. A new instance should be created
 * by each thread. Pooling or caching of the instance is not recommended.</p>
 *
 * @see ConnectionFactory
 * @see Hbck
 */
@InterfaceAudience.Private
public class HBaseHbck implements Hbck {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseHbck.class);

  private boolean aborted;
  private final BlockingInterface hbck;

  private RpcControllerFactory rpcControllerFactory;

  HBaseHbck(BlockingInterface hbck, RpcControllerFactory rpcControllerFactory) {
    this.hbck = hbck;
    this.rpcControllerFactory = rpcControllerFactory;
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

  @Override
  public TableState setTableStateInMeta(TableState state) throws IOException {
    try {
      GetTableStateResponse response = hbck.setTableStateInMeta(
          rpcControllerFactory.newController(),
          RequestConverter.buildSetTableStateInMetaRequest(state));
      return TableState.convert(state.getTableName(), response.getTableState());
    } catch (ServiceException se) {
      LOG.debug("table={}, state={}", state.getTableName(), state.getState(), se);
      throw new IOException(se);
    }
  }

  @Override
  public List<Long> assigns(List<String> encodedRegionNames, boolean override)
      throws IOException {
    try {
      MasterProtos.AssignsResponse response =
          this.hbck.assigns(rpcControllerFactory.newController(),
              RequestConverter.toAssignRegionsRequest(encodedRegionNames, override));
      return response.getPidList();
    } catch (ServiceException se) {
      LOG.debug(toCommaDelimitedString(encodedRegionNames), se);
      throw new IOException(se);
    }
  }

  @Override
  public List<Long> unassigns(List<String> encodedRegionNames, boolean override)
      throws IOException {
    try {
      MasterProtos.UnassignsResponse response =
          this.hbck.unassigns(rpcControllerFactory.newController(),
              RequestConverter.toUnassignRegionsRequest(encodedRegionNames, override));
      return response.getPidList();
    } catch (ServiceException se) {
      LOG.debug(toCommaDelimitedString(encodedRegionNames), se);
      throw new IOException(se);
    }
  }

  private static String toCommaDelimitedString(List<String> list) {
    return list.stream().collect(Collectors.joining(", "));
  }

  @Override
  public List<Boolean> bypassProcedure(List<Long> pids, long waitTime, boolean override,
      boolean recursive)
      throws IOException {
    MasterProtos.BypassProcedureResponse response = ProtobufUtil.call(
        new Callable<MasterProtos.BypassProcedureResponse>() {
          @Override
          public MasterProtos.BypassProcedureResponse call() throws Exception {
            try {
              return hbck.bypassProcedure(rpcControllerFactory.newController(),
                  MasterProtos.BypassProcedureRequest.newBuilder().addAllProcId(pids).
                      setWaitTime(waitTime).setOverride(override).setRecursive(recursive).build());
            } catch (Throwable t) {
              LOG.error(pids.stream().map(i -> i.toString()).
                  collect(Collectors.joining(", ")), t);
              throw t;
            }
          }
        });
    return response.getBypassedList();
  }

  @Override
  public List<Long> scheduleServerCrashProcedures(List<ServerName> serverNames)
      throws IOException {
    try {
      MasterProtos.ScheduleServerCrashProcedureResponse response =
          this.hbck.scheduleServerCrashProcedure(rpcControllerFactory.newController(),
            RequestConverter.toScheduleServerCrashProcedureRequest(serverNames));
      return response.getPidList();
    } catch (ServiceException se) {
      LOG.debug(toCommaDelimitedString(
        serverNames.stream().map(serverName -> ProtobufUtil.toServerName(serverName).toString())
            .collect(Collectors.toList())),
        se);
      throw new IOException(se);
    }
  }
}
