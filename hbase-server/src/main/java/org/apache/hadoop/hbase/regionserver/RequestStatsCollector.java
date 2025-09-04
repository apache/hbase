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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.regionserver.http.RSRequestServlet.N_A;
import static org.apache.hadoop.hbase.regionserver.http.RSRequestServlet.STATUS_RUNNING;
import static org.apache.hadoop.hbase.regionserver.http.RSRequestServlet.STATUS_STOPPED;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Policy;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

/**
 * A service to collect and report request statistics on a RegionServer. It maintains an in-memory
 * cache of request statistics, and provides methods to start, stop, clear, and query the
 * statistics. It also supports exporting the statistics in JSON format and querying using SQL.
 */
@InterfaceAudience.Private
public class RequestStatsCollector {
  public enum OrderBy {
    COUNTS("counts", RequestStat::countsAsDouble),
    RESPONSE_TIME_SUM("responseTimeSum", RequestStat::responseTimeSumMsAsDouble),
    RESPONSE_TIME_AVG("responseTimeAvg", RequestStat::responseTimeAvgMs),
    RESPONSE_SIZE_SUM("responseSizeSum", RequestStat::responseSizeSumBytesAsDouble),
    RESPONSE_SIZE_AVG("responseSizeAvg", RequestStat::responseSizeAvgBytes),
    REQUEST_SIZE_SUM("requestSizeSum", RequestStat::requestSizeSumBytesAsDouble),
    REQUEST_SIZE_AVG("requestSizeAvg", RequestStat::requestSizeAvgBytes);

    private final String value;
    private final Function<RequestStat, Double> sortValueFun;

    OrderBy(String value, Function<RequestStat, Double> sortValueFun) {
      this.value = value;
      this.sortValueFun = sortValueFun;
    }

    public static OrderBy fromValue(String value) {
      for (OrderBy orderBy : OrderBy.values()) {
        if (orderBy.value.equals(value)) {
          return orderBy;
        }
      }
      throw new IllegalArgumentException("Unknown value: " + value);
    }
  }

  // noinspection FieldMayBeFinal
  private class Info {
    public String status = getStatus();
    public long startTime = getStartTime();
    public long stopTime = getStopTime();
    public String maxSize = getMaxSizeStr();
    public String size = getSizeStr();
    public long entryCount = getEntryCount();
  }

  public static final int COLLECTION_SIZE_PERCENT_MIN = 1;
  public static final int COLLECTION_SIZE_PERCENT_MAX = 5;
  public static final int SAMPLING_RATE_MIN = 1;
  public static final int SAMPLING_RATE_MAX = 100;
  public static final int TOP_N_MIN = 1;
  public static final int TOP_N_MAX = 10000;

  static final long MIN_SIZE_BYTES = 1024 * 1024;

  // Parallel processing, but not too many threads
  private static final int QUERY_PARALLELISM_CPU_DIVISOR = 8;
  private static final int PARALLELISM =
    Math.max(Runtime.getRuntime().availableProcessors() / QUERY_PARALLELISM_CPU_DIVISOR, 1);

  private static final String RANGE_SEPARATOR = " ~ ";
  private static final String DB_SCHEMA = "stats";
  private static final String DB_TABLE = "stats";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private Connection conn;
  private CalciteConnection calciteConnection;

  private long startTime = 0L;
  private long stopTime = 0L;
  private int collectionSizePercent = 1;
  private int samplingRate = 100;

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean queryInProgress = new AtomicBoolean(false);

  private Cache<Request, RequestStat> requestStats = null;
  private Map<ByteBuffer, TableName> regionTableMap = null;

  public boolean isStarted() {
    return started.get();
  }

  public String getStatus() {
    return isStarted() ? STATUS_RUNNING : STATUS_STOPPED;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getStopTime() {
    return stopTime;
  }

  public int getCollectionSizePercent() {
    return collectionSizePercent;
  }

  public int getSamplingRate() {
    return samplingRate;
  }

  public void start(int collectionSizePercent, int samplingRate) {
    if (
      collectionSizePercent < COLLECTION_SIZE_PERCENT_MIN
        || collectionSizePercent > COLLECTION_SIZE_PERCENT_MAX
    ) {
      throw new IllegalArgumentException("collectionSizePercent must be >= "
        + COLLECTION_SIZE_PERCENT_MIN + " and <= " + COLLECTION_SIZE_PERCENT_MAX);
    }

    this.collectionSizePercent = collectionSizePercent;

    long maxMemory = Runtime.getRuntime().maxMemory();
    long maxBytes = Math.max((long) (maxMemory * collectionSizePercent / 100.0), MIN_SIZE_BYTES);

    startInternal(maxBytes, samplingRate);
  }

  // For internal use and testing only
  void startInternal(long maxBytes, int samplingRate) {
    if (started.compareAndSet(false, true)) {
      try {
        long maxMemory = Runtime.getRuntime().maxMemory();
        long maxBytesLimit = (long) (maxMemory * COLLECTION_SIZE_PERCENT_MAX / 100.0);
        if (maxBytes <= 0 || maxBytes > maxBytesLimit) {
          throw new IllegalArgumentException("maxBytes must be > 0 and <= " + maxBytesLimit);
        }

        if (samplingRate < SAMPLING_RATE_MIN || samplingRate > SAMPLING_RATE_MAX) {
          throw new IllegalArgumentException(
            "samplingRate must be >= " + SAMPLING_RATE_MIN + " and <= " + SAMPLING_RATE_MAX);
        }

        requestStats = Caffeine.newBuilder().maximumWeight(maxBytes)
          .weigher((Request request, RequestStat stat) -> (int) Math
            .min(request.heapSize() + stat.heapSize(), Integer.MAX_VALUE))
          .build();

        this.samplingRate = samplingRate;
        regionTableMap = new ConcurrentHashMap<>();
        startTime = EnvironmentEdgeManager.currentTime();
        stopTime = 0;
      } catch (Exception e) {
        started.set(false);
        throw e;
      }
    }
  }

  public void stop() {
    if (!started.get()) {
      return;
    }

    started.set(false);
    stopTime = EnvironmentEdgeManager.currentTime();
  }

  public void clear() {
    if (started.get()) {
      throw new IllegalStateException("RequestStatService is still running");
    }

    startTime = 0;
    stopTime = 0;
    requestStats = null;
    regionTableMap = null;
  }

  /**
   * For internal use and testing only. Add a new request stat. If the request already exists, merge
   * the new stat with the existing one.
   * @param rowKey            The row key
   * @param regionNameAsBytes The region name
   * @param tableName         The table name
   * @param type              The operation type
   * @param client            The client address and port
   * @param count             The number of requests
   * @param requestSizeBytes  The request size in bytes
   * @param responseTimeMs    The response time in milliseconds
   * @param responseSizeBytes The response size in bytes
   */
  void add(byte[] rowKey, byte[] regionNameAsBytes, TableName tableName, Region.Operation type,
    String client, long count, long requestSizeBytes, long responseTimeMs, long responseSizeBytes) {
    if (!started.get() || requestStats == null || regionTableMap == null) {
      return;
    }

    if (samplingRate < 100) {
      int rand = ThreadLocalRandom.current().nextInt(100);
      if (rand >= samplingRate) {
        return;
      }
    }

    // The Request object omits the table name to reduce redundancy and save memory.
    // Instead, a separate map maintains the mapping from region names to table names,
    // which is also used to track region splits and merges during request stats collection.
    regionTableMap.putIfAbsent(ByteBuffer.wrap(regionNameAsBytes), tableName);

    requestStats.asMap()
      .computeIfAbsent(new Request(rowKey, regionNameAsBytes, type, client), k -> new RequestStat())
      .add(count, requestSizeBytes, responseTimeMs, responseSizeBytes);
  }

  public void process(Message message, String client, long requestSize, long responseTime,
    long responseSize) {
    if (message instanceof ClientProtos.MultiRequest multiRequest) {
      multiRequest.getRegionActionList().forEach(regionAction -> {
        regionAction.getActionList().forEach(action -> {
          byte[] rowKey = null;
          Region.Operation operation = null;
          if (action.hasMutation()) {
            rowKey = action.getMutation().getRow().toByteArray();
            operation =
              Region.Operation.valueOf("BATCH_" + action.getMutation().getMutateType().name());
          } else if (action.hasGet()) {
            rowKey = action.getGet().getRow().toByteArray();
            operation = Region.Operation.BATCH_GET;
          }

          if (rowKey == null) {
            return;
          }

          byte[] regionFullName = regionAction.getRegion().getValue().toByteArray();
          TableName tableName = RegionInfo.getTable(regionFullName);
          byte[] encodedRegionName = Bytes.toBytes(RegionInfo.encodeRegionName(regionFullName));
          add(rowKey, encodedRegionName, tableName, operation, client, 1, requestSize, responseTime,
            responseSize);
        });
      });

      return;
    }

    byte[] rowKey = null;
    byte[] regionFullName = null;
    Region.Operation operation = null;
    if (message instanceof ClientProtos.MutateRequest mutateRequest) {
      if (mutateRequest.hasMutation()) {
        rowKey = mutateRequest.getMutation().getRow().toByteArray();
        regionFullName = mutateRequest.getRegion().getValue().toByteArray();
        String type = mutateRequest.getMutation().getMutateType().name();
        operation =
          Region.Operation.valueOf(mutateRequest.hasCondition() ? "CHECK_AND_" + type : type);
      }
    } else if (message instanceof ClientProtos.ScanRequest scanRequest) {
      if (scanRequest.hasScan()) {
        byte[] startRow = scanRequest.getScan().getStartRow().toByteArray();
        byte[] stopRow = scanRequest.getScan().getStopRow().toByteArray();
        rowKey = toScanRangeBytes(startRow, stopRow);
        regionFullName = scanRequest.getRegion().getValue().toByteArray();
        operation = Region.Operation.SCAN;
      }
    } else if (message instanceof ClientProtos.GetRequest getRequest) {
      rowKey = getRequest.getGet().getRow().toByteArray();
      regionFullName = getRequest.getRegion().getValue().toByteArray();
      operation = Region.Operation.GET;
    }

    if (rowKey == null || regionFullName == null) {
      return;
    }

    TableName tableName = RegionInfo.getTable(regionFullName);
    byte[] encodedRegionName = Bytes.toBytes(RegionInfo.encodeRegionName(regionFullName));
    add(rowKey, encodedRegionName, tableName, operation, client, 1, requestSize, responseTime,
      responseSize);
  }

  // For testing only
  RequestStat get(Request request) {
    return requestStats.getIfPresent(request);
  }

  // For testing only
  Cache<Request, RequestStat> getRequestStats() {
    return requestStats;
  }

  public long getEntryCount() {
    if (requestStats == null) {
      return -1L;
    }

    return requestStats.estimatedSize();
  }

  public long getSize() {
    if (requestStats == null) {
      return -1L;
    }

    Optional<OptionalLong> result =
      requestStats.policy().eviction().map(Policy.Eviction::weightedSize);
    return result.orElse(OptionalLong.of(-1L)).orElse(-1L);
  }

  public String getSizeStr() {
    long size = getSize();
    if (size < 0) {
      return N_A;
    }
    return StringUtils.TraditionalBinaryPrefix.long2String(size, "B", 1);
  }

  public long getMaxSize() {
    if (requestStats == null) {
      return -1L;
    }

    Optional<Long> result = requestStats.policy().eviction().map(Policy.Eviction::getMaximum);
    return result.orElse(-1L);
  }

  public String getMaxSizeStr() {
    long maxSize = getMaxSize();
    if (maxSize < 0) {
      return N_A;
    }
    return StringUtils.TraditionalBinaryPrefix.long2String(maxSize, "B", 1);
  }

  // For testing only
  void setQueryInProgress(boolean queryInProgress) {
    this.queryInProgress.set(queryInProgress);
  }

  /**
   * Query the top N requests and order by the given sort function in descending order, and return
   * the result.
   * @param sortValueFun The function to get the value to sort.
   * @param topN         The number of top requests to return.
   * @return The top N requests ordered by the given sort function in descending order.
   */
  List<Map.Entry<Request, RequestStat>> query(Function<RequestStat, Double> sortValueFun,
    int topN) {
    // query is heavy operation, do not allow concurrent execution
    if (!queryInProgress.compareAndSet(false, true)) {
      throw new IllegalStateException("Another query is in progress");
    }

    if (topN < TOP_N_MIN || topN > TOP_N_MAX) {
      throw new IllegalArgumentException("topN must be >= " + TOP_N_MIN + " and <= " + TOP_N_MAX);
    }

    if (requestStats == null) {
      return Collections.emptyList();
    }

    try {
      Comparator<Map.Entry<Request, RequestStat>> comparator = Comparator
        .comparingDouble((Map.Entry<Request, RequestStat> e) -> sortValueFun.apply(e.getValue()))
        .thenComparing(e -> e.getValue().responseTimeSumMs())
        .thenComparing(e -> e.getValue().counts())
        .thenComparing(e -> e.getValue().responseSizeSumBytes())
        .thenComparing(e -> e.getValue().requestSizeSumBytes());

      PriorityQueue<Map.Entry<Request, RequestStat>> finalHeap;
      if (PARALLELISM == 1) {
        // No need to split and merge
        PriorityQueue<Map.Entry<Request, RequestStat>> heap = new PriorityQueue<>(comparator);
        keepTopN(topN, requestStats.asMap().entrySet().stream().toList(), heap, comparator);
        finalHeap = heap;
      } else {
        // Split
        ForkJoinPool pool = new ForkJoinPool(PARALLELISM);
        List<PriorityQueue<Map.Entry<Request, RequestStat>>> partialResults;
        try {
          partialResults = pool.submit(() -> requestStats.asMap().entrySet().parallelStream()
            .collect(Collectors
              .groupingByConcurrent(e -> ThreadLocalRandom.current().nextInt(PARALLELISM)))
            .values().parallelStream().map(chunk -> {
              PriorityQueue<Map.Entry<Request, RequestStat>> partialHeap =
                new PriorityQueue<>(comparator);
              keepTopN(topN, chunk, partialHeap, comparator);
              return partialHeap;
            }).toList()).get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        } finally {
          pool.shutdown();
        }

        // Merge partial results
        finalHeap = new PriorityQueue<>(comparator);
        for (PriorityQueue<Map.Entry<Request, RequestStat>> heap : partialResults) {
          keepTopN(topN, heap.stream().toList(), finalHeap, comparator);
        }
      }

      // Final result in descending order
      List<Map.Entry<Request, RequestStat>> result = new ArrayList<>(finalHeap);
      result.sort(comparator.reversed());
      return result;
    } finally {
      queryInProgress.set(false);
    }
  }

  // Min-heap to keep top N
  private static void keepTopN(int topN, List<Map.Entry<Request, RequestStat>> entryList,
    PriorityQueue<Map.Entry<Request, RequestStat>> minHeap,
    Comparator<Map.Entry<Request, RequestStat>> comparator) {
    for (Map.Entry<Request, RequestStat> entry : entryList) {
      if (minHeap.size() < topN) {
        minHeap.offer(entry);
      } else if (comparator.compare(entry, minHeap.peek()) > 0) {
        minHeap.poll();
        minHeap.offer(entry);
      }
    }
  }

  public String queryAndBuildResultJson(int topN, OrderBy orderBy) {
    long startTimestamp = EnvironmentEdgeManager.currentTime();

    try {
      class Stat {
        public String rowKey;
        public String region;
        public String table;
        public String operation;
        public String client;
        public long counts;
        public long requestSizeSumBytes;
        public double requestSizeAvgBytes;
        public long responseTimeSumMs;
        public double responseTimeAvgMs;
        public long responseSizeSumBytes;
        public double responseSizeAvgBytes;
      }

      // noinspection unused
      class QueryResult {
        public Info info;
        public java.util.List<Stat> stats;
        public long executionTimeMs;
        public boolean isSql;
      }

      List<Map.Entry<Request, RequestStat>> dataToReport = query(orderBy.sortValueFun, topN);
      List<Stat> stats = dataToReport.stream().map(entry -> {
        Request req = entry.getKey();
        RequestStat stat = entry.getValue();
        Stat r = new Stat();
        r.rowKey = Bytes.toStringBinary(req.rowKey());
        r.region = Bytes.toString(req.region());
        r.table = regionTableMap.get(ByteBuffer.wrap(req.region())).toString();
        r.operation = req.operation().toString();
        r.client = req.client();
        r.counts = stat.counts();
        r.requestSizeSumBytes = stat.requestSizeSumBytes();
        r.requestSizeAvgBytes = stat.requestSizeAvgBytes();
        r.responseTimeSumMs = stat.responseTimeSumMs();
        r.responseTimeAvgMs = stat.responseTimeAvgMs();
        r.responseSizeSumBytes = stat.responseSizeSumBytes();
        r.responseSizeAvgBytes = stat.responseSizeAvgBytes();
        return r;
      }).toList();

      QueryResult result = new QueryResult();
      result.info = new Info();
      result.stats = stats;
      result.executionTimeMs = EnvironmentEdgeManager.currentTime() - startTimestamp;
      result.isSql = false;

      return MAPPER.writeValueAsString(result);
    } catch (Exception e) {
      throw new RuntimeException("Failed to build JSON result", e);
    }
  }

  public String buildInfoJson() {
    try {
      Info info = new Info();
      return MAPPER.writeValueAsString(info);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize Info to JSON", e);
    }
  }

  private static byte[] toScanRangeBytes(byte[] startKey, byte[] endKey) {
    String scanRangeStr =
      Bytes.toStringBinary(startKey) + RANGE_SEPARATOR + Bytes.toStringBinary(endKey);
    return (scanRangeStr.equals(RANGE_SEPARATOR) ? "" : scanRangeStr).getBytes();
  }

  public static class StatTuple {
    public final String rowKey;
    public final String region;
    public final String table;
    public final String operation;
    public final String client;
    public final long counts;
    public final long requestSizeSumBytes;
    public final long responseTimeSumMs;
    public final long responseSizeSumBytes;

    public StatTuple(String rowKey, String region, String table, String operation, String client,
      long counts, long requestSizeSumBytes, long responseTimeSumMs, long responseSizeSumBytes) {
      this.rowKey = rowKey;
      this.region = region;
      this.table = table;
      this.operation = operation;
      this.client = client;
      this.counts = counts;
      this.requestSizeSumBytes = requestSizeSumBytes;
      this.responseTimeSumMs = responseTimeSumMs;
      this.responseSizeSumBytes = responseSizeSumBytes;
    }
  }

  public static class StatsSchema {
    public final StatTuple[] stats;

    public StatsSchema(ConcurrentMap<Request, RequestStat> map,
      Map<ByteBuffer, TableName> regionTableMap) {
      stats = map.entrySet().stream().map(e -> {
        Request r = e.getKey();
        RequestStat s = e.getValue();
        return new StatTuple(Bytes.toStringBinary(r.rowKey()), Bytes.toStringBinary(r.region()),
          regionTableMap.get(ByteBuffer.wrap(r.region())).getNameAsString(), r.operation().name(),
          r.client(), s.counts(), s.requestSizeSumBytes(), s.responseTimeSumMs(),
          s.responseSizeSumBytes());
      }).toArray(StatTuple[]::new);
    }
  }

  public String sql(String sql, int limit) throws SQLException, ClassNotFoundException {
    // query is heavy operation, do not allow concurrent execution
    if (!queryInProgress.compareAndSet(false, true)) {
      throw new IllegalStateException("Another query is in progress");
    }

    // noinspection unused
    class SqlResponse {
      public Info info;
      public java.util.List<java.util.Map<String, Object>> stats;
      public long executionTimeMs;
      public boolean isSql;
    }

    try {
      long startTimestamp = EnvironmentEdgeManager.currentTime();

      SqlResponse response = new SqlResponse();
      response.info = new Info();
      response.stats = new ArrayList<>();
      if (requestStats != null) {
        initCalcite();
        try (Statement stmt = calciteConnection.createStatement()) {
          // noinspection SqlNoDataSourceInspection,SqlSourceToSinkFlow
          ResultSet rs = stmt.executeQuery(sql + " LIMIT " + limit);
          ResultSetMetaData meta = rs.getMetaData();
          while (rs.next()) {
            Map<String, Object> row = new java.util.LinkedHashMap<>();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
              row.put(meta.getColumnName(i), rs.getObject(i));
            }
            response.stats.add(row);
          }
        }
      }
      response.executionTimeMs = EnvironmentEdgeManager.currentTime() - startTimestamp;
      response.isSql = true;
      try {
        return MAPPER.writeValueAsString(response);
      } catch (Exception e) {
        throw new RuntimeException("Failed to serialize SQL response", e);
      }
    } finally {
      queryInProgress.set(false);
    }
  }

  private void initCalcite() throws SQLException, ClassNotFoundException {
    if (conn == null) {
      Class.forName("org.apache.calcite.jdbc.Driver");
      Properties info = new Properties();
      info.setProperty("lex", "JAVA");
      conn = DriverManager.getConnection("jdbc:calcite:", info);
    }
    if (calciteConnection == null) {
      calciteConnection = conn.unwrap(CalciteConnection.class);
      calciteConnection.setSchema(DB_SCHEMA);
    }
    calciteConnection.getRootSchema().add(DB_TABLE,
      new ReflectiveSchema(new StatsSchema(requestStats.asMap(), regionTableMap)));
  }
}
