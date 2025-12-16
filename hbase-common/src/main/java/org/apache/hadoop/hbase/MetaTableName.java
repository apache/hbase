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
package org.apache.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@InterfaceAudience.Public
public class MetaTableName {
  private static final Logger LOG = LoggerFactory.getLogger(MetaTableName.class);
  private static volatile TableName instance;

  private MetaTableName() {
  }

  /**
   * Get the singleton instance of the meta table name.
   * @return The meta table name instance
   */
  public static TableName getInstance() {
    if (instance == null) {
      synchronized (MetaTableName.class) {
        if (instance == null) {
          instance = initializeHbaseMetaTableName(HBaseConfiguration.create());
          LOG.info("Meta table name initialized: {}", instance.getName());
        }
      }
    }
    return instance;
  }

  /**
   * Initialize the meta table name from the given configuration.
   * 
   * @param conf The configuration to use
   * @return The initialized meta table name
   */
  private static TableName initializeHbaseMetaTableName(Configuration conf) {
    TableName metaTableName = TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "meta");
    LOG.info("Meta table suffix value: {}", metaTableName);
      return metaTableName;
  }
}

