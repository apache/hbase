/*
 * Copyright 2011 The Apache Software Foundation
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

package org.apache.hadoop.hbase.regionserver.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics.SchemaAware;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * A base class for objects that are associated with a particular table and
 * column family. Provides a way to obtain the schema metrics object.
 */
public class SchemaConfigured implements HeapSize, SchemaAware {
  private static final Log LOG = LogFactory.getLog(SchemaMetrics.class);

  // These are not final because we set them at runtime e.g. for HFile blocks.
  private String cfName = SchemaMetrics.UNKNOWN;
  private String tableName = SchemaMetrics.UNKNOWN;
  private SchemaMetrics schemaMetrics;

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
      if (splits.length < HFile.MIN_NUM_HFILE_PATH_LEVELS) {
        LOG.warn("Could not determine table and column family of the HFile " +
            "path " + path);
      } else {
        cfName = splits[splits.length - 2].intern();
        tableName = splits[splits.length - 4].intern();
      }
    }

    initializeMetrics();
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
    this.tableName = tableName.intern();
    this.cfName = cfName.intern();
    initializeMetrics();
  }

  public SchemaConfigured(SchemaAware that) {
    tableName = that.getTableName();
    cfName = that.getColumnFamilyName();
    schemaMetrics = that.getSchemaMetrics();
    if (schemaMetrics == null) {
      initializeMetrics();
    }
  }

  private void initializeMetrics() {
    schemaMetrics = SchemaMetrics.getInstance(tableName, cfName);
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
    return schemaMetrics;
  }

  /**
   * Configures the given object (most commonly an HFile block) with the
   * current table and column family name, and the associated collection of
   * metrics.
   */
  public void passSchemaMetricsTo(SchemaConfigured that) {
    SchemaConfigured upcast = that;  // need this to assign private fields
    upcast.tableName = tableName;
    upcast.cfName = cfName;
    upcast.schemaMetrics = schemaMetrics;
  }

  @Override
  public long heapSize() {
    return SCHEMA_CONFIGURED_ALIGNED_HEAP_SIZE;
  }

}
