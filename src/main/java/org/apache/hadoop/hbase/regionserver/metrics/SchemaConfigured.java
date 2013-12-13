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

package org.apache.hadoop.hbase.regionserver.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics.SchemaAware;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * A base class for objects that are associated with a particular table and
 * column family. Provides a way to obtain the schema metrics object.
 * <p>
 * Due to the variety of things that can be associated with a table/CF, there
 * are many ways to initialize this base class, either in the constructor, or
 * from another similar object. For example, an HFile reader configures HFile
 * blocks it reads with its own table/CF name.
 */
public class SchemaConfigured implements HeapSize, SchemaAware {
  private static final Log LOG = LogFactory.getLog(SchemaConfigured.class);

  // These are not final because we set them at runtime e.g. for HFile blocks.
  private String cfName;
  private String tableName;

  /**
   * Schema metrics. Can only be initialized when we know our column family
   * name, table name, and have had a chance to take a look at the
   * configuration (in {@link SchemaMetrics#configureGlobally(Configuration))
   * so we know whether we are using per-table metrics. Therefore, initialized
   * lazily. We don't make this volatile because even if a thread sees a stale
   * value of null, it will be re-initialized to the same value that other
   * threads see.
   */
  private SchemaMetrics schemaMetrics;

  static {
    if (ClassSize.OBJECT <= 0 || ClassSize.REFERENCE <= 0) {
      throw new AssertionError("Class sizes are not initialized");
    }
  }

  /**
   * Estimated heap size of this object. We don't count table name and column
   * family name characters because these strings are shared among many
   * objects. We need unaligned size to reuse this in subclasses.
   */
  public static final int SCHEMA_CONFIGURED_UNALIGNED_HEAP_SIZE =
      ClassSize.OBJECT + 3 * ClassSize.REFERENCE;

  private static final int SCHEMA_CONFIGURED_ALIGNED_HEAP_SIZE =
      ClassSize.align(SCHEMA_CONFIGURED_UNALIGNED_HEAP_SIZE);

  /** A helper constructor that configures the "use table name" flag. */
  private SchemaConfigured(Configuration conf) {
    SchemaMetrics.configureGlobally(conf);
    // Even though we now know if table-level metrics are used, we can't
    // initialize schemaMetrics yet, because CF and table name are only known
    // to the calling constructor.
  }

  /**
   * Creates an instance corresponding to an unknown table and column family.
   * Used in unit tests. 
   */
  public static SchemaConfigured createUnknown() {
    return new SchemaConfigured(null, SchemaMetrics.UNKNOWN,
        SchemaMetrics.UNKNOWN);
  }

  /**
   * Default constructor. Only use when column/family name are not known at
   * construction (i.e. for HFile blocks).
   */
  public SchemaConfigured() {
  }

  /**
   * Initialize table and column family name from an HFile path. If
   * configuration is null,
   * {@link SchemaMetrics#configureGlobally(Configuration)} should have been
   * called already.
   */
  public SchemaConfigured(Configuration conf, Path path) {
    this(conf);

    if (path != null) {
      String splits[] = path.toString().split("/");
      int numPathLevels = splits.length;
      if (numPathLevels > 0 && splits[0].isEmpty()) {
        // This path starts with an '/'.
        --numPathLevels;
      }
      if (numPathLevels < HFile.MIN_NUM_HFILE_PATH_LEVELS) {
        LOG.warn("Could not determine table and column family of the HFile "
            + "path " + path + ". Expecting at least "
            + HFile.MIN_NUM_HFILE_PATH_LEVELS + " path components.");
        path = null;
      } else {
        cfName = splits[splits.length - 2];
        if (cfName.equals(HRegion.REGION_TEMP_SUBDIR)) {
          // This is probably a compaction or flush output file. We will set
          // the real CF name later.
          cfName = null;
        }
        tableName = splits[splits.length - 4];
        return;
      }
    }

    // This might also happen if we were passed an incorrect path.
    cfName = SchemaMetrics.UNKNOWN;
    tableName = SchemaMetrics.UNKNOWN;
  }

  /**
   * Used when we know an HFile path to deduce table and CF name from, but do
   * not have a configuration.
   * @param path an HFile path
   */
  public SchemaConfigured(Path path) {
    this(null, path);
  }

  /**
   * Used when we know table and column family name. If configuration is null,
   * {@link SchemaMetrics#configureGlobally(Configuration)} should have been
   * called already.
   */
  public SchemaConfigured(Configuration conf, String tableName, String cfName)
  {
    this(conf);
    this.tableName = tableName;
    this.cfName = cfName;
  }

  public SchemaConfigured(SchemaAware that) {
    tableName = that.getTableName();
    cfName = that.getColumnFamilyName();
    schemaMetrics = that.getSchemaMetrics();
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public String getColumnFamilyName() {
    return cfName;
  }

  @Override
  public SchemaMetrics getSchemaMetrics() {
    if (schemaMetrics == null) {
      if (tableName == null || cfName == null) {
        throw new IllegalStateException("Schema metrics requested before " +
            "table/CF name initialization: " + schemaConfAsJSON());
      }
      schemaMetrics = SchemaMetrics.getInstance(tableName, cfName);
    }
    return schemaMetrics;
  }

  /**
   * Configures the given object (e.g. an HFile block) with the current table
   * and column family name, and the associated collection of metrics. Please
   * note that this method configures the <b>other</b> object, not <b>this</b>
   * object.
   */
  public void passSchemaMetricsTo(SchemaConfigured target) {
    if (isNull()) {
      resetSchemaMetricsConf(target);
      return;
    }

    if (!isSchemaConfigured()) {
      // Cannot configure another object if we are not configured ourselves.
      throw new IllegalStateException("Table name/CF not initialized: " +
          schemaConfAsJSON());
    }

    if (conflictingWith(target)) {
      // Make sure we don't try to change table or CF name.
      throw new IllegalArgumentException("Trying to change table name to \"" +
          tableName + "\", CF name to \"" + cfName + "\" from " +
          target.schemaConfAsJSON());
    }

    target.tableName = tableName;
    target.cfName = cfName;
    target.schemaMetrics = schemaMetrics;
    target.schemaConfigurationChanged();
  }

  /**
   * Reset schema metrics configuration in this particular instance. Used when
   * legitimately need to re-initialize the object with another table/CF.
   * This is a static method because its use is discouraged and reserved for
   * when it is really necessary (e.g. writing HFiles in a temp direcdtory
   * on compaction).
   */
  public static void resetSchemaMetricsConf(SchemaConfigured target) {
    target.tableName = null;
    target.cfName = null;
    target.schemaMetrics = null;
    target.schemaConfigurationChanged();
  }

  @Override
  public long heapSize() {
    return SCHEMA_CONFIGURED_ALIGNED_HEAP_SIZE;
  }

  public String schemaConfAsJSON() {
    return "{\"tableName\":\"" + tableName + "\",\"cfName\":\"" + cfName
        + "\"}";
  }

  protected boolean isSchemaConfigured() {
    return tableName != null && cfName != null;
  }

  private boolean isNull() {
    return tableName == null && cfName == null && schemaMetrics == null;
  }

  /**
   * Determines if the current object's table/CF settings are not in conflict
   * with the other object's table and CF. If the other object's table/CF are
   * undefined, they are not considered to be in conflict. Used to sanity-check
   * configuring the other object with this object's table/CF.
   */
  boolean conflictingWith(SchemaConfigured other) {
    return (other.tableName != null && !tableName.equals(other.tableName)) ||
        (other.cfName != null && !cfName.equals(other.cfName));
  }

  /**
   * A hook method called when schema configuration changes. Can be used to
   * update schema-aware member fields.
   */
  protected void schemaConfigurationChanged() {
  }

}
