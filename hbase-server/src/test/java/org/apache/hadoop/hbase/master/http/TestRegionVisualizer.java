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

import static org.junit.Assert.assertEquals;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.RegionMetricsBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.http.RegionVisualizer.RegionDetails;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hbase.thirdparty.com.google.gson.Gson;
import org.apache.hbase.thirdparty.com.google.gson.JsonObject;

@Category({ MasterTests.class, SmallTests.class})
public class TestRegionVisualizer {

  @ClassRule
  public static final HBaseClassTestRule testRule =
    HBaseClassTestRule.forClass(TestRegionVisualizer.class);

  private static final Random rand = new Random();
  private static List<Method> regionMetricsBuilderLongValueSetters;

  @BeforeClass
  public static void beforeClass() {
    regionMetricsBuilderLongValueSetters =
      Arrays.stream(RegionMetricsBuilder.class.getDeclaredMethods())
        .filter(method -> method.getName().startsWith("set"))
        .filter(method -> method.getParameterTypes().length == 1)
        .filter(method -> Objects.equals(method.getParameterTypes()[0], long.class))
        .collect(Collectors.toList());
  }

  @Test
  public void testRegionDetailsJsonSerialization() throws Exception {
    final ServerName serverName =
      ServerName.valueOf("example.org", 1234, System.currentTimeMillis());
    final TableName tableName = TableName.valueOf("foo", "bar");
    final RegionDetails regionDetails =
      new RegionDetails(serverName, tableName, buildRegionMetrics(tableName));

    final Gson gson = RegionVisualizer.buildGson();
    final JsonObject result = gson.fromJson(gson.toJson(regionDetails), JsonObject.class);
    Assert.assertNotNull(result);
    assertEquals(serverName.toShortString(), result.get("server_name").getAsString());
    assertEquals(tableName.getNameAsString(), result.get("table_name").getAsString());
  }

  /**
   * Build a {@link RegionMetrics} object for {@code tableName}. Populates a couple random fields
   * with non-empty values.
   */
  final RegionMetrics buildRegionMetrics(final TableName tableName) throws Exception {
    final List<Method> setters = new ArrayList<>(regionMetricsBuilderLongValueSetters);
    Collections.shuffle(setters, rand);

    final RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName).build();
    final RegionMetricsBuilder builder =
      RegionMetricsBuilder.newBuilder(regionInfo.getRegionName());
    for (final Method setter : setters.subList(0, 3)) {
      setter.invoke(builder, rand.nextLong());
    }
    return builder.build();
  }
}
