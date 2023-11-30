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
package org.apache.hadoop.hbase.util.bulkdatagenerator;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

public class BulkDataGeneratorMapper
  extends Mapper<Text, NullWritable, ImmutableBytesWritable, KeyValue> {

  /** Counter enumeration to count number of rows generated. */
  public static enum Counters {
    ROWS_GENERATED
  }

  public static final String SPLIT_COUNT_KEY =
    BulkDataGeneratorMapper.class.getName() + "split.count";

  private static final String ORG_ID = "00D000000000062";
  private static final int MAX_EVENT_ID = Integer.MAX_VALUE;
  private static final int MAX_VEHICLE_ID = 100;
  private static final int MAX_SPEED_KPH = 140;
  private static final int NUM_LOCATIONS = 10;
  private static int splitCount = 1;
  private static final Random random = new Random(System.currentTimeMillis());
  private static final Map<String, Pair<BigDecimal, BigDecimal>> LOCATIONS =
    Maps.newHashMapWithExpectedSize(NUM_LOCATIONS);
  private static final List<String> LOCATION_KEYS = Lists.newArrayListWithCapacity(NUM_LOCATIONS);
  static {
    LOCATIONS.put("Belém", new Pair<>(BigDecimal.valueOf(-01.45), BigDecimal.valueOf(-48.48)));
    LOCATIONS.put("Brasília", new Pair<>(BigDecimal.valueOf(-15.78), BigDecimal.valueOf(-47.92)));
    LOCATIONS.put("Campinas", new Pair<>(BigDecimal.valueOf(-22.90), BigDecimal.valueOf(-47.05)));
    LOCATIONS.put("Cuiaba", new Pair<>(BigDecimal.valueOf(-07.25), BigDecimal.valueOf(-58.42)));
    LOCATIONS.put("Manaus", new Pair<>(BigDecimal.valueOf(-03.10), BigDecimal.valueOf(-60.00)));
    LOCATIONS.put("Porto Velho",
      new Pair<>(BigDecimal.valueOf(-08.75), BigDecimal.valueOf(-63.90)));
    LOCATIONS.put("Recife", new Pair<>(BigDecimal.valueOf(-08.10), BigDecimal.valueOf(-34.88)));
    LOCATIONS.put("Rio de Janeiro",
      new Pair<>(BigDecimal.valueOf(-22.90), BigDecimal.valueOf(-43.23)));
    LOCATIONS.put("Santarém", new Pair<>(BigDecimal.valueOf(-02.43), BigDecimal.valueOf(-54.68)));
    LOCATIONS.put("São Paulo", new Pair<>(BigDecimal.valueOf(-23.53), BigDecimal.valueOf(-46.62)));
    LOCATION_KEYS.addAll(LOCATIONS.keySet());
  }

  final static byte[] COLUMN_FAMILY_BYTES = Utility.COLUMN_FAMILY.getBytes();

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration c = context.getConfiguration();
    splitCount = c.getInt(SPLIT_COUNT_KEY, 1);
  }

  /**
   * Generates a single record based on value set to the key by
   * {@link BulkDataGeneratorRecordReader#getCurrentKey()}.
   * {@link Utility.TableColumnNames#TOOL_EVENT_ID} is first part of row key. Keeping first
   * {@link Utility#SPLIT_PREFIX_LENGTH} characters as index of the record to be generated ensures
   * that records are equally distributed across all regions of the table since region boundaries
   * are generated in similar fashion. Check {@link Utility#createTable(Admin, String, int, Map)}
   * method for region split info.
   * @param key     - The key having index of next record to be generated
   * @param value   - Value associated with the key (not used)
   * @param context - Context of the mapper container
   */
  @Override
  protected void map(Text key, NullWritable value, Context context)
    throws IOException, InterruptedException {

    int recordIndex = Integer.parseInt(key.toString());

    // <6-characters-region-boundary-prefix>_<15-random-chars>_<record-index-for-this-mapper-task>
    final String toolEventId =
      String.format("%0" + Utility.SPLIT_PREFIX_LENGTH + "d", recordIndex % (splitCount + 1)) + "_"
        + EnvironmentEdgeManager.currentTime() + (1e14 + (random.nextFloat() * 9e13)) + "_"
        + recordIndex;
    final String eventId = String.valueOf(Math.abs(random.nextInt(MAX_EVENT_ID)));
    final String vechileId = String.valueOf(Math.abs(random.nextInt(MAX_VEHICLE_ID)));
    final String speed = String.valueOf(Math.abs(random.nextInt(MAX_SPEED_KPH)));
    final String location = LOCATION_KEYS.get(random.nextInt(NUM_LOCATIONS));
    final Pair<BigDecimal, BigDecimal> coordinates = LOCATIONS.get(location);
    final BigDecimal latitude = coordinates.getFirst();
    final BigDecimal longitude = coordinates.getSecond();

    final ImmutableBytesWritable hKey =
      new ImmutableBytesWritable(String.format("%s:%s", toolEventId, ORG_ID).getBytes());
    addKeyValue(context, hKey, Utility.TableColumnNames.ORG_ID, ORG_ID);
    addKeyValue(context, hKey, Utility.TableColumnNames.TOOL_EVENT_ID, toolEventId);
    addKeyValue(context, hKey, Utility.TableColumnNames.EVENT_ID, eventId);
    addKeyValue(context, hKey, Utility.TableColumnNames.VEHICLE_ID, vechileId);
    addKeyValue(context, hKey, Utility.TableColumnNames.SPEED, speed);
    addKeyValue(context, hKey, Utility.TableColumnNames.LATITUDE, latitude.toString());
    addKeyValue(context, hKey, Utility.TableColumnNames.LONGITUDE, longitude.toString());
    addKeyValue(context, hKey, Utility.TableColumnNames.LOCATION, location);
    addKeyValue(context, hKey, Utility.TableColumnNames.TIMESTAMP,
      String.valueOf(EnvironmentEdgeManager.currentTime()));

    context.getCounter(Counters.ROWS_GENERATED).increment(1);
  }

  private void addKeyValue(final Context context, ImmutableBytesWritable key,
    final Utility.TableColumnNames columnName, final String value)
    throws IOException, InterruptedException {
    KeyValue kv =
      new KeyValue(key.get(), COLUMN_FAMILY_BYTES, columnName.getColumnName(), value.getBytes());
    context.write(key, kv);
  }
}
