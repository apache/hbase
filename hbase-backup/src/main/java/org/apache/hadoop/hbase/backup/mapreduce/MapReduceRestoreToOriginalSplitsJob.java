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
package org.apache.hadoop.hbase.backup.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.RestoreJob;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSVisitor;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@InterfaceAudience.Private
public class MapReduceRestoreToOriginalSplitsJob implements RestoreJob {
  private Configuration conf;

  @Override
  public void run(Path[] dirPaths, TableName[] fromTables, Path restoreRootDir,
    TableName[] toTables, boolean fullBackupRestore) throws IOException {
    Configuration conf = getConf();

    // We are using the files from the snapshot. We should copy them rather than move them over
    conf.setBoolean(BulkLoadHFiles.ALWAYS_COPY_FILES, true);

    FileSystem fs = FileSystem.get(conf);
    Map<byte[], List<Path>> family2Files = buildFamily2Files(fs, dirPaths, fullBackupRestore);

    BulkLoadHFiles bulkLoad = BulkLoadHFiles.create(conf);
    for (int i = 0; i < fromTables.length; i++) {
      bulkLoad.bulkLoad(toTables[i], family2Files);
    }
  }

  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  private static Map<byte[], List<Path>> buildFamily2Files(FileSystem fs, Path[] dirs,
    boolean isFullBackup) throws IOException {
    if (isFullBackup) {
      return buildFullBackupFamily2Files(fs, dirs);
    }

    Map<byte[], List<Path>> family2Files = new HashMap<>();

    for (Path dir : dirs) {
      byte[] familyName = Bytes.toBytes(dir.getParent().getName());
      if (family2Files.containsKey(familyName)) {
        family2Files.get(familyName).add(dir);
      } else {
        family2Files.put(familyName, Lists.newArrayList(dir));
      }
    }

    return family2Files;
  }

  private static Map<byte[], List<Path>> buildFullBackupFamily2Files(FileSystem fs, Path[] dirs)
    throws IOException {
    Map<byte[], List<Path>> family2Files = new HashMap<>();
    for (Path regionPath : dirs) {
      FSVisitor.visitRegionStoreFiles(fs, regionPath, (region, family, name) -> {
        Path path = new Path(regionPath, new Path(family, name));
        byte[] familyName = Bytes.toBytes(family);
        if (family2Files.containsKey(familyName)) {
          family2Files.get(familyName).add(path);
        } else {
          family2Files.put(familyName, Lists.newArrayList(path));
        }
      });
    }
    return family2Files;
  }

}
