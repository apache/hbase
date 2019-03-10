/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.metrics.Meter;
import org.apache.hadoop.hbase.metrics.Metric;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
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
  private Map<String, Optional<Metric>> requestsMap;
  private MetricRegistry registry;
  private LossyCounting clientMetricsLossyCounting, regionMetricsLossyCounting;
  private boolean active = false;

  enum MetaTableOps {
    GET, PUT, DELETE;
  }

  private ImmutableMap<Class, MetaTableOps> opsNameMap =
      ImmutableMap.<Class, MetaTableOps>builder()
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
        WALEdit edit, Durability durability) throws IOException {
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

    private void markMeterIfPresent(String requestMeter) {
      if (requestMeter.isEmpty()) {
        return;
      }

      Optional<Metric> optionalMetric = requestsMap.get(requestMeter);
      if (optionalMetric != null && optionalMetric.isPresent()) {
        Meter metric = (Meter) optionalMetric.get();
        metric.mark();
      }
    }

    private void registerMeterIfNotPresent(String requestMeter) {
      if (requestMeter.isEmpty()) {
        return;
      }
      if (!requestsMap.containsKey(requestMeter)) {
        registry.meter(requestMeter);
        requestsMap.put(requestMeter, registry.get(requestMeter));
      }
    }

    /**
     * Registers and counts lossyCount for Meters that kept by lossy counting.
     * By using lossy count to maintain meters, at most 7 / e meters will be kept  (e is error rate)
     * e.g. when e is 0.02 by default, at most 350 Clients request metrics will be kept
     *      also, all kept elements have frequency higher than e * N. (N is total count)
     * @param requestMeter meter to be registered
     * @param lossyCounting lossyCounting object for one type of meters.
     */
    private void registerLossyCountingMeterIfNotPresent(String requestMeter,
        LossyCounting lossyCounting) {

      if (requestMeter.isEmpty()) {
        return;
      }
      synchronized (lossyCounting) {
        Set<String> metersToBeRemoved = lossyCounting.addByOne(requestMeter);

        boolean isNewMeter = !requestsMap.containsKey(requestMeter);
        boolean requestMeterRemoved = metersToBeRemoved.contains(requestMeter);
        if (isNewMeter) {
          if (requestMeterRemoved) {
            // if the new metric is swept off by lossyCounting then don't add in the map
            metersToBeRemoved.remove(requestMeter);
          } else {
            // else register the new metric and add in the map
            registry.meter(requestMeter);
            requestsMap.put(requestMeter, registry.get(requestMeter));
          }
        }

        for (String meter : metersToBeRemoved) {
          //cleanup requestsMap according to the swept data from lossy count;
          requestsMap.remove(meter);
          registry.remove(meter);
        }
      }
    }

    /**
     * Get table name from Ops such as: get, put, delete.
     * @param op such as get, put or delete.
     */
    private String getTableNameFromOp(Row op) {
      String tableName = null;
      String tableRowKey = new String(((Row) op).getRow(), StandardCharsets.UTF_8);
      if (tableRowKey.isEmpty()) {
        return null;
      }
      tableName = tableRowKey.split(",").length > 0 ? tableRowKey.split(",")[0] : null;
      return tableName;
    }

    /**
     * Get regionId from Ops such as: get, put, delete.
     * @param op  such as get, put or delete.
     */
    private String getRegionIdFromOp(Row op) {
      String regionId = null;
      String tableRowKey = new String(((Row) op).getRow(), StandardCharsets.UTF_8);
      if (tableRowKey.isEmpty()) {
        return null;
      }
      regionId = tableRowKey.split(",").length > 2 ? tableRowKey.split(",")[2] : null;
      return regionId;
    }

    private boolean isMetaTableOp(ObserverContext<RegionCoprocessorEnvironment> e) {
      return TableName.META_TABLE_NAME
          .equals(e.getEnvironment().getRegionInfo().getTable());
    }

    private void clientMetricRegisterAndMark() {
      // Mark client metric
      String clientIP = RpcServer.getRemoteIp() != null ? RpcServer.getRemoteIp().toString() : "";
      if (clientIP == null || clientIP.isEmpty()) {
        return;
      }
      String clientRequestMeter = clientRequestMeterName(clientIP);
      registerLossyCountingMeterIfNotPresent(clientRequestMeter, clientMetricsLossyCounting);
      markMeterIfPresent(clientRequestMeter);
    }

    private void tableMetricRegisterAndMark(Row op) {
      // Mark table metric
      String tableName = getTableNameFromOp(op);
      if (tableName == null || tableName.isEmpty()) {
        return;
      }
      String tableRequestMeter = tableMeterName(tableName);
      registerAndMarkMeterIfNotPresent(tableRequestMeter);
    }

    private void regionMetricRegisterAndMark(Row op) {
      // Mark region metric
      String regionId = getRegionIdFromOp(op);
      if (regionId == null || regionId.isEmpty()) {
        return;
      }
      String regionRequestMeter = regionMeterName(regionId);
      registerLossyCountingMeterIfNotPresent(regionRequestMeter, regionMetricsLossyCounting);
      markMeterIfPresent(regionRequestMeter);
    }

    private void opMetricRegisterAndMark(Row op) {
      // Mark access type ["get", "put", "delete"] metric
      String opMeterName = opMeterName(op);
      if (opMeterName == null || opMeterName.isEmpty()) {
        return;
      }
      registerAndMarkMeterIfNotPresent(opMeterName);
    }

    private void opWithClientMetricRegisterAndMark(Object op) {
      // // Mark client + access type metric
      String opWithClientMeterName = opWithClientMeterName(op);
      if (opWithClientMeterName == null || opWithClientMeterName.isEmpty()) {
        return;
      }
      registerAndMarkMeterIfNotPresent(opWithClientMeterName);
    }

    // Helper function to register and mark meter if not present
    private void registerAndMarkMeterIfNotPresent(String name) {
      registerMeterIfNotPresent(name);
      markMeterIfPresent(name);
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
      requestsMap = new ConcurrentHashMap<>();
      clientMetricsLossyCounting = new LossyCounting("clientMetaMetrics");
      regionMetricsLossyCounting = new LossyCounting("regionMetaMetrics");
      // only be active mode when this region holds meta table.
      active = true;
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    // since meta region can move around, clear stale metrics when stop.
    if (requestsMap != null) {
      for (String meterName : requestsMap.keySet()) {
        registry.remove(meterName);
      }
    }
  }
}
