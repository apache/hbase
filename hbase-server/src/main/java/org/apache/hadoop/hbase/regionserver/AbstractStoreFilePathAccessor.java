/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Joiner;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

@InterfaceAudience.Private
public abstract class AbstractStoreFilePathAccessor implements StoreFilePathAccessor {

  public static final String STOREFILE_INCLUDED_STR = "included";

  protected static final String LIST_SEPARATOR = ";";
  protected final Configuration conf;

  public AbstractStoreFilePathAccessor(Configuration conf) {
    this.conf = conf;
  }

  abstract String getSeparator();

  abstract List<Path> getStoreFilePaths(final String tableName, final String regionName,
    final String storeName, final String columnName) throws IOException;

  @Override
  public abstract void writeStoreFilePaths(final String tableName, final String regionName,
    final String storeName, StoreFilePathUpdate storeFilePathUpdate)
    throws IOException;

  @Override
  public List<Path> getIncludedStoreFilePaths(final String tableName, final String regionName,
    final String storeName) throws IOException {
    return getStoreFilePaths(tableName, regionName, storeName, STOREFILE_INCLUDED_STR);
  }

  protected static byte[] storeFileListToByteArray(List<Path> storeFilePaths) {
    return Bytes.toBytes(Joiner.on(LIST_SEPARATOR).join(storeFilePaths));
  }

  protected static List<Path> byteToStoreFileList(byte[] data) {
    List<Path> paths = new ArrayList<>();
    if (data != null && data.length != 0) {
      String pathString = Bytes.toString(data);
      String[] pathStrings = pathString.split(LIST_SEPARATOR);
      for (String path : pathStrings) {
        paths.add(new Path(path));
      }
    }
    return paths;
  }

  /**
   * Get a rowkey in the order of regionName-storeName-tablename
   *
   * @param tableName
   * @param regionName
   * @param storeName
   * @return a joint rowkey in the form of regionName-storeName-tablename
   */
  protected String getKey(final String tableName, final String regionName, final String storeName) {
    return Joiner.on(getSeparator()).join(regionName, storeName, tableName);
  }

  protected void validate(final String tableName, final String regionName,
    final String storeName, final String columnName) {
    validate(tableName, regionName, storeName);
    Preconditions.checkArgument(StringUtils.isNotBlank(columnName),
      "column name cannot be null or empty");
  }

  protected void validate(final String tableName, final String regionName,
    final String storeName) {
    Preconditions
      .checkArgument(StringUtils.isNotBlank(tableName), "table name cannot be null or empty");
    Preconditions
      .checkArgument(StringUtils.isNotBlank(regionName), "region name cannot be null or empty");
    Preconditions
      .checkArgument(StringUtils.isNotBlank(storeName), "store name cannot be null or empty");
  }

  protected void validate(final String tableName, final String regionName, final String storeName,
    final StoreFilePathUpdate storeFilePathUpdate) {
    validate(tableName, regionName, storeName);
    Preconditions.checkArgument(storeFilePathUpdate.hasStoreFilesUpdate(),
      "Must have storefile or compacted files to be updated");
  }
}
