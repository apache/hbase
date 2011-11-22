/*
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.regionserver.metrics;

import static org.junit.Assert.*;

import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.ClassSize;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONStringer;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestSchemaConfigured {
  private static final Log LOG = LogFactory.getLog(TestSchemaConfigured.class);
  private final String TABLE_NAME = "myTable";
  private final String CF_NAME = "myColumnFamily";

  private static final Path TMP_HFILE_PATH = new Path(
      "/hbase/myTable/myRegion/" + HRegion.REGION_TEMP_SUBDIR + "/hfilename");

  /** Test if toString generates real JSON */
  @Test
  public void testToString() throws JSONException {
    SchemaConfigured sc = new SchemaConfigured(null, TABLE_NAME, CF_NAME);
    JSONStringer json = new JSONStringer();
    json.object();
    json.key("tableName");
    json.value(TABLE_NAME);
    json.key("cfName");
    json.value(CF_NAME);
    json.endObject();
    assertEquals(json.toString(), sc.schemaConfAsJSON());
  }

  /** Don't allow requesting metrics before setting table/CF name */
  @Test
  public void testDelayedInitialization() {
    SchemaConfigured unconfigured = new SchemaConfigured();
    try {
      unconfigured.getSchemaMetrics();
      fail(IllegalStateException.class.getSimpleName() + " expected");
    } catch (IllegalStateException ex) {
      assertTrue("Unexpected exception message: " + ex.getMessage(),
          Pattern.matches(".* metrics requested before .* initialization.*",
          ex.getMessage()));
      LOG.debug("Expected exception: " + ex.getMessage());
    }

    SchemaMetrics.setUseTableNameInTest(false);
    SchemaConfigured other = new SchemaConfigured(null, TABLE_NAME, CF_NAME);
    other.passSchemaMetricsTo(unconfigured);
    unconfigured.getSchemaMetrics();  // now this should succeed
  }

  /** Don't allow setting table/CF name twice */
  @Test
  public void testInitializingTwice() {
    Configuration conf = HBaseConfiguration.create();
    for (int i = 0; i < 4; ++i) {
      SchemaConfigured sc = new SchemaConfigured(conf, TABLE_NAME, CF_NAME);
      SchemaConfigured target =
          new SchemaConfigured(conf, TABLE_NAME + (i % 2 == 1 ? "1" : ""),
              CF_NAME + ((i & 2) != 0 ? "1" : ""));
      if (i == 0) {
        sc.passSchemaMetricsTo(target);  // No exception expected.
        continue;
      }

      String testDesc =
          "Trying to re-configure " + target.schemaConfAsJSON() + " with "
              + sc.schemaConfAsJSON();
      try {
        sc.passSchemaMetricsTo(target);
        fail(IllegalArgumentException.class.getSimpleName() + " expected");
      } catch (IllegalArgumentException ex) {
        final String errorMsg = testDesc + ". Unexpected exception message: " +
            ex.getMessage();
        final String exceptionRegex = "Trying to change table .* CF .*";
        assertTrue(errorMsg, Pattern.matches(exceptionRegex, ex.getMessage()));
        LOG.debug("Expected exception: " + ex.getMessage());
      }
    }
  }

  @Test(expected=IllegalStateException.class)
  public void testConfigureWithUnconfigured() {
    SchemaConfigured unconfigured = new SchemaConfigured();
    SchemaConfigured target = new SchemaConfigured();
    unconfigured.passSchemaMetricsTo(target);
  }

  public void testConfigurePartiallyDefined() {
    final SchemaConfigured sc = new SchemaConfigured(null, "t1", "cf1");
    final SchemaConfigured target1 = new SchemaConfigured(null, "t2", null);
    sc.passSchemaMetricsTo(target1);
    assertEquals("t2", target1.getColumnFamilyName());
    assertEquals("cf1", target1.getColumnFamilyName());

    final SchemaConfigured target2 = new SchemaConfigured(null, null, "cf2");
    sc.passSchemaMetricsTo(target2);
    assertEquals("t1", target2.getColumnFamilyName());
    assertEquals("cf2", target2.getColumnFamilyName());

    final SchemaConfigured target3 = new SchemaConfigured(null, null, null);
    sc.passSchemaMetricsTo(target3);
    assertEquals("t1", target2.getColumnFamilyName());
    assertEquals("cf1", target2.getColumnFamilyName());
  }

  @Test(expected=IllegalArgumentException.class)
  public void testConflictingConf() {
    SchemaConfigured sc = new SchemaConfigured(null, "t1", "cf1");
    SchemaConfigured target = new SchemaConfigured(null, "t2", "cf1");
   target.passSchemaMetricsTo(sc);
  }

  /**
   * When the "column family" deduced from the path is ".tmp" (this happens
   * for files written on compaction) we allow re-setting the CF to another
   * value.
   */
  @Test
  public void testTmpPath() {
    SchemaConfigured sc = new SchemaConfigured(null, "myTable", "myCF");
    SchemaConfigured target = new SchemaConfigured(TMP_HFILE_PATH);
    sc.passSchemaMetricsTo(target);
  }

  /**
   * Even if CF is initially undefined (".tmp"), we don't allow to change
   * table name.
   */
  @Test(expected=IllegalArgumentException.class)
  public void testTmpPathButInvalidTable() {
    SchemaConfigured sc = new SchemaConfigured(null, "anotherTable", "myCF");
    SchemaConfigured target = new SchemaConfigured(TMP_HFILE_PATH);
    sc.passSchemaMetricsTo(target);
  }

  @Test
  public void testSchemaConfigurationHook() {
    SchemaConfigured sc = new SchemaConfigured(null, "myTable", "myCF");
    final StringBuilder newCF = new StringBuilder();
    final StringBuilder newTable = new StringBuilder();
    SchemaConfigured target = new SchemaConfigured() {
      @Override
      protected void schemaConfigurationChanged() {
        newCF.append(getColumnFamilyName());
        newTable.append(getTableName());
      }
    };
    sc.passSchemaMetricsTo(target);
    assertEquals("myTable", newTable.toString());
    assertEquals("myCF", newCF.toString());
  }

  @Test
  public void testResetSchemaMetricsConf() {
    SchemaConfigured target = new SchemaConfigured(null, "t1", "cf1");
    SchemaConfigured.resetSchemaMetricsConf(target);
    new SchemaConfigured(null, "t2", "cf2").passSchemaMetricsTo(target);
    assertEquals("t2", target.getTableName());
    assertEquals("cf2", target.getColumnFamilyName());
  }

  @Test
  public void testPathTooShort() {
    // This has too few path components (four, the first one is empty).
    SchemaConfigured sc1 = new SchemaConfigured(new Path("/a/b/c/d"));
    assertEquals(SchemaMetrics.UNKNOWN, sc1.getTableName());
    assertEquals(SchemaMetrics.UNKNOWN, sc1.getColumnFamilyName());

    SchemaConfigured sc2 = new SchemaConfigured(new Path("a/b/c/d"));
    assertEquals(SchemaMetrics.UNKNOWN, sc2.getTableName());
    assertEquals(SchemaMetrics.UNKNOWN, sc2.getColumnFamilyName());

    SchemaConfigured sc3 = new SchemaConfigured(
        new Path("/hbase/tableName/regionId/cfName/hfileName"));
    assertEquals("tableName", sc3.getTableName());
    assertEquals("cfName", sc3.getColumnFamilyName());

    SchemaConfigured sc4 = new SchemaConfigured(
        new Path("hbase/tableName/regionId/cfName/hfileName"));
    assertEquals("tableName", sc4.getTableName());
    assertEquals("cfName", sc4.getColumnFamilyName());
  }

}
