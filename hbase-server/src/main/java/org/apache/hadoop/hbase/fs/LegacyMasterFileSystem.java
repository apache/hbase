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
import java.util.Collections;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

@InterfaceAudience.Private
public class LegacyMasterFileSystem extends HMasterFileSystem {
  private final Path activeNsDir;
  private final Path rootDir;

  private final FileSystem fs;

  public LegacyMasterFileSystem(Configuration conf, Path rootDir) {
    this.fs = rootDir.getFileSystem(conf);
    this.rootDir = rootDir;
    this.activeNsDir = new Path(rootDir, HConstants.BASE_NAMESPACE_DIR);
  }

  public Collection<String> getNamespaces() throws IOException {
    return getNamespaces(activeNsDir);
  }

  protected Collection<String> getNamespaces(Path baseDir) throws IOException {
    FileStatus[] activeNsDirs = fs.globStatus(new Path(baseDir, "*"));
    if (activeNsDirs == null || activeNsDirs.length == 0) {
      return Collections.emptyList();
    }
    ArrayList<String> namespaces = new ArrayList<String>(activeNsDirs.length);
    for (int i = 0; i < activeNsDirs.length; ++i) {
      namespaces.add(activeNsDirs[i].getPath().getName());
    }
    return namespaces;
  }

  public Collection<TableName> getTables(String namespace) throws IOException {
    Path baseDir = new Path(activeNsDir, namespace);
    FileStatus[] dirs = fs.listStatus(baseDir, new UserTableDirFilter(fs));
    if (dirs == null || dirs.length == 0) {
      return Collections.emptyList();
    }

    ArrayList<TableName> tables = new ArrayList<TableName>(dirs.length);
    for (int i = 0; i < dirs.length; ++i) {
      tables.add(TableName.valueOf(namespace, dirs[i].getPath().getName()));
    }
    return tables;
  }

  public Collection<HRegionInfo> getRegions(TableName tableName) throws IOException {
    return null;
  }

  protected Collection<HRegionInfo> getRegions(Path baseDir) throws IOException {
    FileStatus[] dirs = fs.listStatus(baseDir, new RegionDirFilter(fs));
    if (dirs == null || dirs.length == 0) {
      return Collections.emptyList();
    }

    ArrayList<HRegionInfo> hriList = new ArrayList<HRegionInfo>(dirs.length);
    for (int i = 0; i < dirs.length; ++i) {
      // TODO: Load HRI
    }
    return hriList;
  }

  /**
   * A {@link PathFilter} that returns usertable directories. To get all directories use the
   * {@link BlackListDirFilter} with a <tt>null</tt> blacklist
   */
  private static class UserTableDirFilter extends FSUtils.BlackListDirFilter {
    public UserTableDirFilter(FileSystem fs) {
      super(fs, HConstants.HBASE_NON_TABLE_DIRS);
    }

    protected boolean isValidName(final String name) {
      if (!super.isValidName(name))
        return false;

      try {
        TableName.isLegalTableQualifierName(Bytes.toBytes(name));
      } catch (IllegalArgumentException e) {
        LOG.info("INVALID NAME " + name);
        return false;
      }
      return true;
    }
  }

  /**
   * Filter for all dirs that don't start with '.'
   */
  private static class RegionDirFilter implements PathFilter {
    // This pattern will accept 0.90+ style hex region dirs and older numeric region dir names.
    final public static Pattern regionDirPattern = Pattern.compile("^[0-9a-f]*$");
    final FileSystem fs;

    public RegionDirFilter(FileSystem fs) {
      this.fs = fs;
    }

    @Override
    public boolean accept(Path rd) {
      if (!regionDirPattern.matcher(rd.getName()).matches()) {
        return false;
      }

      try {
        return fs.getFileStatus(rd).isDirectory();
      } catch (IOException ioe) {
        // Maybe the file was moved or the fs was disconnected.
        LOG.warn("Skipping file " + rd +" due to IOException", ioe);
        return false;
      }
    }
  }
}
