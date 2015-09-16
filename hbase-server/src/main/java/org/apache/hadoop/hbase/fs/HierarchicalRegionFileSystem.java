/**
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

package org.apache.hadoop.hbase.fs;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;

@InterfaceAudience.Private
public class HierarchicalRegionFileSystem extends LegacyRegionFileSystem {
  private static final Log LOG = LogFactory.getLog(HierarchicalRegionFileSystem.class);

  private static final String OLD_REGION_NAME_PADDING = "abcdef1234abcdef1234abcdef1234ab";

  /** Number of characters for DIR name, 4 characters for 16^4 = 65536 buckets. */
  public static final int HUMONGOUS_DIR_NAME_SIZE = 4;

  public HierarchicalRegionFileSystem(Configuration conf, HRegionInfo hri) {
    super(conf, hri);
  }

  // ==========================================================================
  //  PROTECTED FS-Layout Internals
  // ==========================================================================
  @Override
  protected Path getRegionDir(Path baseDir) {
    return getHumongousRegionDir(baseDir, getTable(), getRegionInfo().getEncodedName());
  }

  private Path getHumongousRegionDir(Path baseDir, TableName tableName, String name) {
    if (name.length() != HRegionInfo.MD5_HEX_LENGTH) {
      String table = tableName.getQualifierAsString();
      String namespace = tableName.getNamespaceAsString();

      // Meta and old root table use the old encoded name format still
      if (!namespace.equals(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR)) {
        throw new IllegalArgumentException("The region with encoded name " + name
          + " is not a humongous region, cannot get humongous region dir from it.");
      }
      if (!table.equals(TableName.META_TABLE_NAME.getQualifierAsString()) &&
          !table.equals(TableName.OLD_ROOT_TABLE_NAME.getQualifierAsString())) {
        throw new IllegalArgumentException("The region with encoded name " + name
          + " is not a humongous region, cannot get humongous region dir from it.");
      }

      // Add padding to guarantee we will have enough characters
      return new Path(new Path(baseDir, makeBucketName(name, OLD_REGION_NAME_PADDING)), name);
    }
    return new Path(new Path(baseDir, makeBucketName(name, null)), name);
  }

  private String makeBucketName(String regionName, String padding) {
    if (padding != null) {
      regionName = regionName + padding;
    }
    return regionName.substring(HRegionInfo.MD5_HEX_LENGTH - HUMONGOUS_DIR_NAME_SIZE);
  }
}
