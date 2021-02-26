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
package org.apache.hadoop.hbase.master.webapp;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.StreamSupport;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterators;
import org.apache.hbase.thirdparty.io.netty.handler.codec.http.QueryStringEncoder;

/**
 * <p>
 * Support class for the "Meta Entries" section in {@code resources/hbase-webapps/master/table.jsp}.
 * </p>
 * <p>
 * <b>Interface</b>. This class's intended consumer is {@code table.jsp}. As such, it's primary
 * interface is the active {@link HttpServletRequest}, from which it uses the {@code scan_*}
 * request parameters. This class supports paging through an optionally filtered view of the
 * contents of {@code hbase:meta}. Those filters and the pagination offset are specified via these
 * request parameters. It provides helper methods for constructing pagination links.
 * <ul>
 *   <li>{@value #NAME_PARAM} - the name of the table requested. The only table of our concern here
 *   is {@code hbase:meta}; any other value is effectively ignored by the giant conditional in the
 *   jsp.</li>
 *   <li>{@value #SCAN_LIMIT_PARAM} - specifies a limit on the number of region (replicas) rendered
 *   on the by the table in a single request -- a limit on page size. This corresponds to the
 *   number of {@link RegionReplicaInfo} objects produced by {@link Results#iterator()}. When a
 *   value for {@code scan_limit} is invalid or not specified, the default value of
 *   {@value #SCAN_LIMIT_DEFAULT} is used. In order to avoid excessive resource consumption, a
 *   maximum value of {@value #SCAN_LIMIT_MAX} is enforced.</li>
 *   <li>{@value #SCAN_REGION_STATE_PARAM} - an optional filter on {@link RegionState}.</li>
 *   <li>{@value #SCAN_START_PARAM} - specifies the rowkey at which a scan should start. For usage
 *   details, see the below section on <b>Pagination</b>.</li>
 *   <li>{@value #SCAN_TABLE_PARAM} - specifies a filter on the values returned, limiting them to
 *   regions from a specified table. This parameter is implemented as a prefix filter on the
 *   {@link Scan}, so in effect it can be used for simple namespace and multi-table matches.</li>
 * </ul>
 * </p>
 * <p>
 * <b>Pagination</b>. A single page of results are made available via {@link #getResults()} / an
 * instance of {@link Results}. Callers use its {@link Iterator} consume the page of
 * {@link RegionReplicaInfo} instances, each of which represents a region or region replica. Helper
 * methods are provided for building page navigation controls preserving the user's selected filter
 * set: {@link #buildFirstPageUrl()}, {@link #buildNextPageUrl(byte[])}. Pagination is implemented
 * using a simple offset + limit system. Offset is provided by the {@value #SCAN_START_PARAM},
 * limit via {@value #SCAN_LIMIT_PARAM}. Under the hood, the {@link Scan} is constructed with
 * {@link Scan#setMaxResultSize(long)} set to ({@value SCAN_LIMIT_PARAM} +1), while the
 * {@link Results} {@link Iterator} honors {@value #SCAN_LIMIT_PARAM}. The +1 allows the caller to
 * know if a "next page" is available via {@link Results#hasMoreResults()}. Note that this
 * pagination strategy is incomplete when it comes to region replicas and can potentially omit
 * rendering replicas that fall between the last rowkey offset and {@code replicaCount % page size}.
 * </p>
 * <p>
 * <b>Error Messages</b>. Any time there's an error parsing user input, a message will be populated
 * in {@link #getErrorMessages()}. Any fields which produce an error will have their filter values
 * set to the default, except for a value of {@value  #SCAN_LIMIT_PARAM} that exceeds
 * {@value #SCAN_LIMIT_MAX}, in which case {@value #SCAN_LIMIT_MAX} is used.
 * </p>
 */
@InterfaceAudience.Private
public class MetaBrowser {
  public static final String NAME_PARAM = "name";
  public static final String SCAN_LIMIT_PARAM = "scan_limit";
  public static final String SCAN_REGION_STATE_PARAM = "scan_region_state";
  public static final String SCAN_START_PARAM = "scan_start";
  public static final String SCAN_TABLE_PARAM = "scan_table";

  public static final int SCAN_LIMIT_DEFAULT = 10;
  public static final int SCAN_LIMIT_MAX = 10_000;

  private final AsyncConnection connection;
  private final HttpServletRequest request;
  private final List<String> errorMessages;
  private final String name;
  private final Integer scanLimit;
  private final RegionState.State scanRegionState;
  private final byte[] scanStart;
  private final TableName scanTable;

  public MetaBrowser(final AsyncConnection connection, final HttpServletRequest request) {
    this.connection = connection;
    this.request = request;
    this.errorMessages = new LinkedList<>();
    this.name = resolveName(request);
    this.scanLimit = resolveScanLimit(request);
    this.scanRegionState = resolveScanRegionState(request);
    this.scanStart = resolveScanStart(request);
    this.scanTable = resolveScanTable(request);
  }

  public List<String> getErrorMessages() {
    return errorMessages;
  }

  public String getName() {
    return name;
  }

  public Integer getScanLimit() {
    return scanLimit;
  }

  public byte[] getScanStart() {
    return scanStart;
  }

  public RegionState.State getScanRegionState() {
    return scanRegionState;
  }

  public TableName getScanTable() {
    return scanTable;
  }

  public Results getResults() {
    final AsyncTable<AdvancedScanResultConsumer> asyncTable =
      connection.getTable(TableName.META_TABLE_NAME);
    return new Results(asyncTable.getScanner(buildScan()));
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("scanStart", scanStart)
      .append("scanLimit", scanLimit)
      .append("scanTable", scanTable)
      .append("scanRegionState", scanRegionState)
      .toString();
  }

  private static String resolveName(final HttpServletRequest request) {
    return resolveRequestParameter(request, NAME_PARAM);
  }

  private Integer resolveScanLimit(final HttpServletRequest request) {
    final String requestValueStr = resolveRequestParameter(request, SCAN_LIMIT_PARAM);
    if (StringUtils.isBlank(requestValueStr)) {
      return null;
    }

    final Integer requestValue = tryParseInt(requestValueStr);
    if (requestValue == null) {
      errorMessages.add(buildScanLimitMalformedErrorMessage(requestValueStr));
      return null;
    }
    if (requestValue <= 0) {
      errorMessages.add(buildScanLimitLTEQZero(requestValue));
      return SCAN_LIMIT_DEFAULT;
    }

    final int truncatedValue = Math.min(requestValue, SCAN_LIMIT_MAX);
    if (requestValue != truncatedValue) {
      errorMessages.add(buildScanLimitExceededErrorMessage(requestValue));
    }
    return truncatedValue;
  }

  private RegionState.State resolveScanRegionState(final HttpServletRequest request) {
    final String requestValueStr = resolveRequestParameter(request, SCAN_REGION_STATE_PARAM);
    if (requestValueStr == null) {
      return null;
    }
    final RegionState.State requestValue = tryValueOf(RegionState.State.class, requestValueStr);
    if (requestValue == null) {
      errorMessages.add(buildScanRegionStateMalformedErrorMessage(requestValueStr));
      return null;
    }
    return requestValue;
  }

  private static byte[] resolveScanStart(final HttpServletRequest request) {
    // TODO: handle replicas that fall between the last rowkey and pagination limit.
    final String requestValue = resolveRequestParameter(request, SCAN_START_PARAM);
    if (requestValue == null) {
      return null;
    }
    return Bytes.toBytesBinary(requestValue);
  }

  private static TableName resolveScanTable(final HttpServletRequest request) {
    final String requestValue = resolveRequestParameter(request, SCAN_TABLE_PARAM);
    if (requestValue == null) {
      return null;
    }
    return TableName.valueOf(requestValue);
  }

  private static String resolveRequestParameter(final HttpServletRequest request,
    final String param) {
    if (request == null) {
      return null;
    }
    final String requestValueStrEnc = request.getParameter(param);
    if (StringUtils.isBlank(requestValueStrEnc)) {
      return null;
    }
    return urlDecode(requestValueStrEnc);
  }

  private static Filter buildTableFilter(final TableName tableName) {
    return new PrefixFilter(tableName.toBytes());
  }

  private static Filter buildScanRegionStateFilter(final RegionState.State state) {
    return new SingleColumnValueFilter(
      HConstants.CATALOG_FAMILY,
      HConstants.STATE_QUALIFIER,
      CompareOperator.EQUAL,
      // use the same serialization strategy as found in MetaTableAccessor#addRegionStateToPut
      Bytes.toBytes(state.name()));
  }

  private Filter buildScanFilter() {
    if (scanTable == null && scanRegionState == null) {
      return null;
    }

    final List<Filter> filters = new ArrayList<>(2);
    if (scanTable != null) {
      filters.add(buildTableFilter(scanTable));
    }
    if (scanRegionState != null) {
      filters.add(buildScanRegionStateFilter(scanRegionState));
    }
    if (filters.size() == 1) {
      return filters.get(0);
    }
    return new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
  }

  private Scan buildScan() {
    final Scan metaScan = new Scan()
      .addFamily(HConstants.CATALOG_FAMILY)
      .readVersions(1)
      .setLimit((scanLimit != null ? scanLimit : SCAN_LIMIT_DEFAULT) + 1);
    if (scanStart != null) {
      metaScan.withStartRow(scanStart, false);
    }
    final Filter filter = buildScanFilter();
    if (filter != null) {
      metaScan.setFilter(filter);
    }
    return metaScan;
  }

  /**
   * Adds {@code value} to {@code encoder} under {@code paramName} when {@code value} is non-null.
   */
  private void addParam(final QueryStringEncoder encoder, final String paramName,
    final Object value) {
    if (value != null) {
      encoder.addParam(paramName, value.toString());
    }
  }

  private QueryStringEncoder buildFirstPageEncoder() {
    final QueryStringEncoder encoder =
      new QueryStringEncoder(request.getRequestURI());
    addParam(encoder, NAME_PARAM, name);
    addParam(encoder, SCAN_LIMIT_PARAM, scanLimit);
    addParam(encoder, SCAN_REGION_STATE_PARAM, scanRegionState);
    addParam(encoder, SCAN_TABLE_PARAM, scanTable);
    return encoder;
  }

  public String buildFirstPageUrl() {
    return buildFirstPageEncoder().toString();
  }

  static String buildStartParamFrom(final byte[] lastRow) {
    if (lastRow == null) {
      return null;
    }
    return urlEncode(Bytes.toStringBinary(lastRow));
  }

  public String buildNextPageUrl(final byte[] lastRow) {
    final QueryStringEncoder encoder = buildFirstPageEncoder();
    final String startRow = buildStartParamFrom(lastRow);
    addParam(encoder, SCAN_START_PARAM, startRow);
    return encoder.toString();
  }

  private static String urlEncode(final String val) {
    if (StringUtils.isEmpty(val)) {
      return null;
    }
    try {
      return URLEncoder.encode(val, StandardCharsets.UTF_8.toString());
    } catch (UnsupportedEncodingException e) {
      return null;
    }
  }

  private static String urlDecode(final String val) {
    if (StringUtils.isEmpty(val)) {
      return null;
    }
    try {
      return URLDecoder.decode(val, StandardCharsets.UTF_8.toString());
    } catch (UnsupportedEncodingException e) {
      return null;
    }
  }

  private static Integer tryParseInt(final String val) {
    if (StringUtils.isEmpty(val)) {
      return null;
    }
    try {
      return Integer.parseInt(val);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static <T extends Enum<T>> T tryValueOf(final Class<T> clazz,
    final String value) {
    if (clazz == null || value == null) {
      return null;
    }
    try {
      return Enum.valueOf(clazz, value);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static String buildScanLimitExceededErrorMessage(final int requestValue) {
    return String.format(
      "Requested SCAN_LIMIT value %d exceeds maximum value %d.", requestValue, SCAN_LIMIT_MAX);
  }

  private static String buildScanLimitMalformedErrorMessage(final String requestValue) {
    return String.format(
      "Requested SCAN_LIMIT value '%s' cannot be parsed as an integer.", requestValue);
  }

  private static String buildScanLimitLTEQZero(final int requestValue) {
    return String.format("Requested SCAN_LIMIT value %d is <= 0.", requestValue);
  }

  private static String buildScanRegionStateMalformedErrorMessage(final String requestValue) {
    return String.format(
      "Requested SCAN_REGION_STATE value '%s' cannot be parsed as a RegionState.", requestValue);
  }

  /**
   * Encapsulates the results produced by this {@link MetaBrowser} instance.
   */
  public final class Results implements AutoCloseable, Iterable<RegionReplicaInfo> {

    private final ResultScanner resultScanner;
    private final Iterator<RegionReplicaInfo> sourceIterator;

    private Results(final ResultScanner resultScanner) {
      this.resultScanner = resultScanner;
      this.sourceIterator =  StreamSupport.stream(resultScanner.spliterator(), false)
        .map(RegionReplicaInfo::from)
        .flatMap(Collection::stream)
        .iterator();
    }

    /**
     * @return {@code true} when the underlying {@link ResultScanner} is not yet exhausted,
     *   {@code false} otherwise.
     */
    public boolean hasMoreResults() {
      return sourceIterator.hasNext();
    }

    @Override
    public void close() {
      if (resultScanner != null) {
        resultScanner.close();
      }
    }

    @Override public Iterator<RegionReplicaInfo> iterator() {
      return Iterators.limit(sourceIterator, scanLimit != null ? scanLimit : SCAN_LIMIT_DEFAULT);
    }
  }
}
