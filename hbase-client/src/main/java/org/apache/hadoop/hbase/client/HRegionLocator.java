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

import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.REGION_NAMES_KEY;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.CatalogReplicaMode;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.trace.TableSpanBuilder;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

/**
 * An implementation of {@link RegionLocator}. Used to view region location information for a single
 * HBase table. Lightweight. Get as needed and just close when done. Instances of this class SHOULD
 * NOT be constructed directly. Obtain an instance via {@link Connection}. See
 * {@link ConnectionFactory} class comment for an example of how.
 * <p/>
 * This class is thread safe
 */
@InterfaceAudience.Private
public class HRegionLocator implements RegionLocator {

  private final TableName tableName;
  private final ConnectionImplementation connection;

  public HRegionLocator(TableName tableName, ConnectionImplementation connection) {
    this.connection = connection;
    this.tableName = tableName;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    // This method is required by the RegionLocator interface. This implementation does not have any
    // persistent state, so there is no need to do anything here.
  }

  @Override
  public HRegionLocation getRegionLocation(byte[] row, int replicaId, boolean reload)
    throws IOException {
    final Supplier<Span> supplier = new TableSpanBuilder(connection)
      .setName("HRegionLocator.getRegionLocation").setTableName(tableName);
    return tracedLocationFuture(() -> connection
      .locateRegion(tableName, row, !reload, true, replicaId).getRegionLocation(replicaId),
      AsyncRegionLocator::getRegionNames, supplier);
  }

  @Override
  public List<HRegionLocation> getRegionLocations(byte[] row, boolean reload) throws IOException {
    final Supplier<Span> supplier = new TableSpanBuilder(connection)
      .setName("HRegionLocator.getRegionLocations").setTableName(tableName);
    final RegionLocations locs = tracedLocationFuture(
      () -> connection.locateRegion(tableName, row, !reload, true, RegionInfo.DEFAULT_REPLICA_ID),
      AsyncRegionLocator::getRegionNames, supplier);
    return Arrays.asList(locs.getRegionLocations());
  }

  @Override
  public List<HRegionLocation> getAllRegionLocations() throws IOException {
    final Supplier<Span> supplier = new TableSpanBuilder(connection)
      .setName("HRegionLocator.getAllRegionLocations").setTableName(tableName);
    return tracedLocationFuture(() -> {
      ArrayList<HRegionLocation> regions = new ArrayList<>();
      for (RegionLocations locations : listRegionLocations()) {
        for (HRegionLocation location : locations.getRegionLocations()) {
          regions.add(location);
        }
        RegionLocations cleaned = locations.removeElementsWithNullLocation();
        // above can return null if all locations had null location
        if (cleaned != null) {
          connection.cacheLocation(tableName, cleaned);
        }
      }
      return regions;
    }, HRegionLocator::getRegionNames, supplier);
  }

  private static List<String> getRegionNames(List<HRegionLocation> locations) {
    if (CollectionUtils.isEmpty(locations)) {
      return Collections.emptyList();
    }
    return locations.stream().filter(Objects::nonNull).map(AsyncRegionLocator::getRegionNames)
      .filter(Objects::nonNull).flatMap(List::stream).collect(Collectors.toList());
  }

  @Override
  public void clearRegionLocationCache() {
    final Supplier<Span> supplier = new TableSpanBuilder(connection)
      .setName("HRegionLocator.clearRegionLocationCache").setTableName(tableName);
    TraceUtil.trace(() -> connection.clearRegionCache(tableName), supplier);
  }

  @Override
  public TableName getName() {
    return this.tableName;
  }

  @SuppressWarnings("MixedMutabilityReturnType")
  private List<RegionLocations> listRegionLocations() throws IOException {
    if (TableName.isMetaTableName(tableName)) {
      return Collections
        .singletonList(connection.locateRegion(tableName, HConstants.EMPTY_START_ROW, false, true));
    }
    final List<RegionLocations> regions = new ArrayList<>();
    MetaTableAccessor.Visitor visitor = new MetaTableAccessor.TableVisitorBase(tableName) {
      @Override
      public boolean visitInternal(Result result) throws IOException {
        RegionLocations locations = MetaTableAccessor.getRegionLocations(result);
        if (locations == null) {
          return true;
        }
        regions.add(locations);
        return true;
      }
    };
    CatalogReplicaMode metaReplicaMode = CatalogReplicaMode.fromString(connection.getConfiguration()
      .get(LOCATOR_META_REPLICAS_MODE, CatalogReplicaMode.NONE.toString()));
    MetaTableAccessor.scanMetaForTableRegions(connection, visitor, tableName, metaReplicaMode);
    return regions;
  }

  private <R, T extends Throwable> R tracedLocationFuture(TraceUtil.ThrowingCallable<R, T> action,
    Function<R, List<String>> getRegionNames, Supplier<Span> spanSupplier) throws T {
    final Span span = spanSupplier.get();
    try (Scope ignored = span.makeCurrent()) {
      final R result = action.call();
      final List<String> regionNames = getRegionNames.apply(result);
      if (!CollectionUtils.isEmpty(regionNames)) {
        span.setAttribute(REGION_NAMES_KEY, regionNames);
      }
      span.setStatus(StatusCode.OK);
      span.end();
      return result;
    } catch (Throwable e) {
      TraceUtil.setError(span, e);
      span.end();
      throw e;
    }
  }
}
