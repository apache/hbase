/*
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

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LossyCounting;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * A coprocessor that collects metrics from meta table.
 * <p>
 * These metrics will be available through the regular Hadoop metrics2 sinks (ganglia, opentsdb,
 * etc) as well as JMX output.
 * </p>
 * @see MetaTableMetrics
 */

@InterfaceAudience.Private
public class MetaTableMetrics implements RegionCoprocessor {

  private ExampleRegionObserverMeta observer;
  private MetricRegistry registry;
  private LossyCounting<String> clientMetricsLossyCounting, regionMetricsLossyCounting;
  private boolean active = false;
  private Set<String> metrics = ConcurrentHashMap.newKeySet();

  enum MetaTableOps {
    GET, PUT, DELETE,
  }

  private ImmutableMap<Class<? extends Row>, MetaTableOps> opsNameMap =
      ImmutableMap.<Class<? extends Row>, MetaTableOps>builder()
              .put(Put.class, MetaTableOps.PUT)
              .put(Get.class, MetaTableOps.GET)
              .put(Delete.class, MetaTableOps.DELETE)
              .build();

  class ExampleRegionObserverMeta implements RegionCoprocessor, RegionObserver {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get,
        List<Cell> results) throws IOException {
      registerAndMarkMetrics(e, get);
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit,
        Durability durability) throws IOException {
      registerAndMarkMetrics(e, put);
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete,
        WALEdit edit, Durability durability) {
      registerAndMarkMetrics(e, delete);
    }

    private void registerAndMarkMetrics(ObserverContext<RegionCoprocessorEnvironment> e, Row row){
      if (!active || !isMetaTableOp(e)) {
        return;
      }
      tableMetricRegisterAndMark(row);
      clientMetricRegisterAndMark();
      regionMetricRegisterAndMark(row);
      opMetricRegisterAndMark(row);
      opWithClientMetricRegisterAndMark(row);
    }

    /**
     * Get table name from Ops such as: get, put, delete.
     * @param op such as get, put or delete.
     */
    private String getTableNameFromOp(Row op) {
      final String tableRowKey = Bytes.toString(op.getRow());
      if (StringUtils.isEmpty(tableRowKey)) {
        return null;
      }
      final String[] splits = tableRowKey.split(",");
      return splits.length > 0 ? splits[0] : null;
    }

    /**
     * Get regionId from Ops such as: get, put, delete.
     * @param op  such as get, put or delete.
     */
    private String getRegionIdFromOp(Row op) {
      final String tableRowKey = Bytes.toString(op.getRow());
      if (StringUtils.isEmpty(tableRowKey)) {
        return null;
      }
      final String[] splits = tableRowKey.split(",");
      return splits.length > 2 ? splits[2] : null;
    }

    private boolean isMetaTableOp(ObserverContext<RegionCoprocessorEnvironment> e) {
      return TableName.META_TABLE_NAME
          .equals(e.getEnvironment().getRegionInfo().getTable());
    }

    private void clientMetricRegisterAndMark() {
      // Mark client metric
      String clientIP = RpcServer.getRemoteIp() != null ? RpcServer.getRemoteIp().toString() : null;
      if (clientIP == null || clientIP.isEmpty()) {
        return;
      }
      String clientRequestMeter = clientRequestMeterName(clientIP);
      clientMetricsLossyCounting.add(clientRequestMeter);
      registerAndMarkMeter(clientRequestMeter);
    }

    private void tableMetricRegisterAndMark(Row op) {
      // Mark table metric
      String tableName = getTableNameFromOp(op);
      if (tableName == null || tableName.isEmpty()) {
        return;
      }
      String tableRequestMeter = tableMeterName(tableName);
      registerAndMarkMeter(tableRequestMeter);
    }

    private void regionMetricRegisterAndMark(Row op) {
      // Mark region metric
      String regionId = getRegionIdFromOp(op);
      if (regionId == null || regionId.isEmpty()) {
        return;
      }
      String regionRequestMeter = regionMeterName(regionId);
      regionMetricsLossyCounting.add(regionRequestMeter);
      registerAndMarkMeter(regionRequestMeter);
    }

    private void opMetricRegisterAndMark(Row op) {
      // Mark access type ["get", "put", "delete"] metric
      String opMeterName = opMeterName(op);
      if (opMeterName == null || opMeterName.isEmpty()) {
        return;
      }
      registerAndMarkMeter(opMeterName);
    }

    private void opWithClientMetricRegisterAndMark(Object op) {
      // // Mark client + access type metric
      String opWithClientMeterName = opWithClientMeterName(op);
      if (opWithClientMeterName == null || opWithClientMeterName.isEmpty()) {
        return;
      }
      registerAndMarkMeter(opWithClientMeterName);
    }

    // Helper function to register and mark meter if not present
    private void registerAndMarkMeter(String requestMeter) {
      if (requestMeter.isEmpty()) {
        return;
      }
      if(!registry.get(requestMeter).isPresent()){
        metrics.add(requestMeter);
      }
      registry.meter(requestMeter).mark();
    }

    private String opWithClientMeterName(Object op) {
      // Extract meter name containing the client IP
      String clientIP = RpcServer.getRemoteIp() != null ? RpcServer.getRemoteIp().toString() : "";
      if (clientIP.isEmpty()) {
        return "";
      }
      MetaTableOps ops = opsNameMap.get(op.getClass());
      String opWithClientMeterName = "";
      switch (ops) {
        case GET:
          opWithClientMeterName = String.format("MetaTable_client_%s_get_request", clientIP);
          break;
        case PUT:
          opWithClientMeterName = String.format("MetaTable_client_%s_put_request", clientIP);
          break;
        case DELETE:
          opWithClientMeterName = String.format("MetaTable_client_%s_delete_request", clientIP);
          break;
        default:
          break;
      }
      return opWithClientMeterName;
    }

    private String opMeterName(Object op) {
      // Extract meter name containing the access type
      MetaTableOps ops = opsNameMap.get(op.getClass());
      String opMeterName = "";
      switch (ops) {
        case GET:
          opMeterName = "MetaTable_get_request";
          break;
        case PUT:
          opMeterName = "MetaTable_put_request";
          break;
        case DELETE:
          opMeterName = "MetaTable_delete_request";
          break;
        default:
          break;
      }
      return opMeterName;
    }

    private String tableMeterName(String tableName) {
      // Extract meter name containing the table name
      return String.format("MetaTable_table_%s_request", tableName);
    }

    private String clientRequestMeterName(String clientIP) {
      // Extract meter name containing the client IP
      if (clientIP.isEmpty()) {
        return "";
      }
      return String.format("MetaTable_client_%s_lossy_request", clientIP);
    }

    private String regionMeterName(String regionId) {
      // Extract meter name containing the region ID
      return String.format("MetaTable_region_%s_lossy_request", regionId);
    }
  }

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(observer);
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    observer = new ExampleRegionObserverMeta();
    if (env instanceof RegionCoprocessorEnvironment
        && ((RegionCoprocessorEnvironment) env).getRegionInfo().getTable() != null
        && ((RegionCoprocessorEnvironment) env).getRegionInfo().getTable()
          .equals(TableName.META_TABLE_NAME)) {
      RegionCoprocessorEnvironment regionCoprocessorEnv = (RegionCoprocessorEnvironment) env;
      registry = regionCoprocessorEnv.getMetricRegistryForRegionServer();
      LossyCounting.LossyCountingListener<String> listener = key -> {
        registry.remove(key);
        metrics.remove(key);
      };
      final Configuration conf = regionCoprocessorEnv.getConfiguration();
      clientMetricsLossyCounting = new LossyCounting<>("clientMetaMetrics", conf, listener);
      regionMetricsLossyCounting = new LossyCounting<>("regionMetaMetrics", conf, listener);
      // only be active mode when this region holds meta table.
      active = true;
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    // since meta region can move around, clear stale metrics when stop.
    for(String metric:metrics){
      registry.remove(metric);
    }
  }
}
