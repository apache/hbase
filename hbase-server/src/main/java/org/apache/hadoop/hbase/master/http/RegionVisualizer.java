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
package org.apache.hadoop.hbase.master.http;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.gson.FieldNamingPolicy;
import org.apache.hbase.thirdparty.com.google.gson.Gson;
import org.apache.hbase.thirdparty.com.google.gson.GsonBuilder;
import org.apache.hbase.thirdparty.com.google.gson.JsonElement;
import org.apache.hbase.thirdparty.com.google.gson.JsonObject;
import org.apache.hbase.thirdparty.com.google.gson.JsonPrimitive;
import org.apache.hbase.thirdparty.com.google.gson.JsonSerializationContext;
import org.apache.hbase.thirdparty.com.google.gson.JsonSerializer;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;

/**
 * Support class for the "Region Visualizer" rendered out of
 * {@code src/main/jamon/org/apache/hadoop/hbase/tmpl/master/RegionVisualizerTmpl.jamon}
 */
@InterfaceAudience.Private
public class RegionVisualizer extends AbstractHBaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(RegionVisualizer.class);

  private final Admin admin;
  private final AsyncAdmin asyncAdmin;
  private final Gson gson;

  public RegionVisualizer() {
    admin = null;
    asyncAdmin = null;
    gson = null;
  }

  public RegionVisualizer(final Admin admin) {
    this.admin = admin;
    this.asyncAdmin = null;
    this.gson = buildGson();
  }

  public RegionVisualizer(final AsyncAdmin asyncAdmin) {
    this.admin = null;
    this.asyncAdmin = asyncAdmin;
    this.gson = buildGson();
  }

  public String renderRegionDetails() {
    return gson.toJson(clusterStatusToRegionDetails());
  }

  public static void main(final String[] argv) {
    new RegionVisualizer().doStaticMain(argv);
  }

  static Gson buildGson() {
    return new GsonBuilder()
      .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
      .enableComplexMapKeySerialization()
      .registerTypeAdapter(byte[].class, new ByteArraySerializer())
      .registerTypeAdapter(Size.class, new SizeAsBytesSerializer())
      .registerTypeAdapter(RegionDetails.class, new RegionDetailsSerializer())
      .create();
  }

  private ClusterMetrics getClusterMetrics()
    throws ExecutionException, InterruptedException, IOException {
    if (admin != null) {
      return admin.getClusterMetrics();
    }
    if (asyncAdmin != null) {
      return asyncAdmin.getClusterMetrics().get();
    }
    throw new RuntimeException("should not happen");
  }

  private List<RegionDetails> clusterStatusToRegionDetails() {
    final ClusterMetrics clusterMetrics;
    try {
      clusterMetrics = getClusterMetrics();
    } catch (Exception e) {
      LOG.warn("Failed to retrieve cluster metrics.", e);
      return Collections.emptyList();
    }

    return clusterMetrics.getLiveServerMetrics().entrySet().stream()
      .flatMap(serverEntry -> {
        final ServerName serverName = serverEntry.getKey();
        final ServerMetrics serverMetrics = serverEntry.getValue();
        return serverMetrics.getRegionMetrics().values().stream()
          .map(regionMetrics -> {
            final TableName tableName = RegionInfo.getTable(regionMetrics.getRegionName());
            return new RegionDetails(serverName, tableName, regionMetrics);
          });
      })
      .collect(Collectors.toList());
  }

  @Override protected void addOptions() {

  }

  @Override protected void processOptions(CommandLine cmd) {

  }

  @Override protected int doWork() throws Exception {
    final Configuration conf = HBaseConfiguration.create(getConf());
    final AsyncConnection conn = ConnectionFactory.createAsyncConnection(conf).get();
    final RegionVisualizer viz = new RegionVisualizer(conn.getAdmin());
    System.out.println(viz.renderRegionDetails());
    return 0;
  }

  private static final class ByteArraySerializer implements JsonSerializer<byte[]> {

    @Override
    public JsonElement serialize(byte[] src, Type typeOfSrc, JsonSerializationContext context) {
      return new JsonPrimitive(Bytes.toString(src));
    }
  }

  /**
   * Simplify representation of a {@link Size} instance by converting to bytes.
   */
  private static final class SizeAsBytesSerializer implements JsonSerializer<Size> {

    @Override
    public JsonElement serialize(Size src, Type typeOfSrc, JsonSerializationContext context) {
      return new JsonPrimitive(src.get(Size.Unit.BYTE));
    }
  }

  /**
   * "Flatten" the serialized representation of a {@link RegionDetails}.
   */
  private static final class RegionDetailsSerializer implements JsonSerializer<RegionDetails> {
    @Override
    public JsonElement serialize(RegionDetails src, Type typeOfSrc,
      JsonSerializationContext context) {
      final JsonObject jobj = (JsonObject) context.serialize(src.getRegionMetrics());
      jobj.addProperty("server_name", src.getServerName().toShortString());
      jobj.addProperty("table_name", src.getTableName().getNameAsString());
      return jobj;
    }
  }

  /**
   * POJO carrying detailed information about a region for use in visualizations. Intended to be
   * serialized to JSON and consumed from JavaScript.
   */
  public static final class RegionDetails {
    private final ServerName serverName;
    private final TableName tableName;
    private final RegionMetrics regionMetrics;

    RegionDetails(
      final ServerName serverName,
      final TableName tableName,
      final RegionMetrics regionMetrics
    ) {
      this.serverName = serverName;
      this.tableName = tableName;
      this.regionMetrics = regionMetrics;
    }

    public ServerName getServerName() {
      return serverName;
    }

    public TableName getTableName() {
      return tableName;
    }

    public RegionMetrics getRegionMetrics() {
      return regionMetrics;
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("serverName", serverName)
        .append("tableName", tableName)
        .append("regionMetrics", regionMetrics)
        .toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      RegionDetails that = (RegionDetails) o;

      return new EqualsBuilder()
        .append(serverName, that.serverName)
        .append(tableName, that.tableName)
        .append(regionMetrics, that.regionMetrics)
        .isEquals();
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder(17, 37)
        .append(serverName)
        .append(tableName)
        .append(regionMetrics)
        .toHashCode();
    }
  }
}
