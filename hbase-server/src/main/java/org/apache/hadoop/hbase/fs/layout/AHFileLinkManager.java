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
package org.apache.hadoop.hbase.fs.layout;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.util.Pair;

public abstract class AHFileLinkManager {
  public abstract HFileLink buildFromHFileLinkPattern(Configuration conf, Path hFileLinkPattern) throws IOException;
  
  public abstract HFileLink buildFromHFileLinkPattern(final Path rootDir, final Path archiveDir, final Path hFileLinkPattern);

  public abstract Path createPath(final TableName table, final String region, final String family, final String hfile);

  public abstract HFileLink build(final Configuration conf, final TableName table, final String region, final String family, final String hfile)
      throws IOException;

  public abstract boolean isHFileLink(final Path path);

  public abstract boolean isHFileLink(String fileName);

  public abstract String getReferencedHFileName(final String fileName);

  public abstract Path getHFileLinkPatternRelativePath(Path path);

  public abstract String getReferencedRegionName(final String fileName);
  
  public abstract TableName getReferencedTableName(final String fileName);
  
  public abstract String createHFileLinkName(final HRegionInfo hfileRegionInfo, final String hfileName);

  public abstract String createHFileLinkName(final TableName tableName, final String regionName, final String hfileName);

  public abstract boolean create(final Configuration conf, final FileSystem fs, final Path dstFamilyPath, final HRegionInfo hfileRegionInfo, final String hfileName)
      throws IOException;

  public abstract boolean createFromHFileLink(final Configuration conf, final FileSystem fs, final Path dstFamilyPath, final String hfileLinkName) throws IOException;

  public abstract String createBackReferenceName(final String tableNameStr, final String regionName);

  public abstract Path getHFileFromBackReference(final Path rootDir, final Path linkRefPath);

  public abstract Pair<TableName, String> parseBackReferenceName(String name);

  public abstract Path getHFileFromBackReference(final Configuration conf, final Path linkRefPath) throws IOException;

  public AHFileLinkManager() {
    super();
  }
}
