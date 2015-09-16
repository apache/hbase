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
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.FSUtils;

public abstract class HMasterFileSystem {
  public abstract Collection<String> getNamespaces() throws IOException;
  public abstract Collection<TableName> getTables(String namespace) throws IOException;
  public abstract Collection<HRegionInfo> getRegions(TableName tableName) throws IOException;

  public Collection<TableName> getTables() throws IOException {
    ArrayList<TableName> tables = new ArrayList<TableName>();
    for (String ns: getNamespaces()) {
      tables.addAll(getTables(ns));
    }
    return tables;
  }

  public static HMasterFileSystem open(final Configuration conf) {
    return open(conf, FSUtils.getRootDir(conf));
  }

  public static HMasterFileSystem open(Configuration conf, Path rootDir) {
    String fsType = conf.get("hbase.fs.layout.type").toLowerCase();
    switch (fsType) {
      case "legacy":
        return new LegacyMasterFileSystem(conf, rootDir);
      case "hierarchical":
        return new HierarchicalMasterFileSystem(conf, rootDir);
      default:
        throw new IOException("Invalid filesystem type " + fsType);
    }
  }
}
