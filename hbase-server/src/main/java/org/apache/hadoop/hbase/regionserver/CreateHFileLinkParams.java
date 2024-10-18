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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class CreateHFileLinkParams {

  private Configuration conf;
  private FileSystem fs;
  private Path dstFamilyPath;
  private String familyName;
  private String dstTableName;
  private String dstRegionName;
  private TableName linkedTable;
  private String linkedRegion;
  private String hfileName;
  private boolean createBackRef;

  public CreateHFileLinkParams() {

  }

  public Configuration getConf() {
    return conf;
  }

  public CreateHFileLinkParams conf(Configuration conf) {
    this.conf = conf;
    return this;
  }

  public FileSystem getFs() {
    return fs;
  }

  public CreateHFileLinkParams fileSystem(FileSystem fs) {
    this.fs = fs;
    return this;
  }

  public Path getDstFamilyPath() {
    return dstFamilyPath;
  }

  public CreateHFileLinkParams dstFamilyPath(Path dstFamilyPath) {
    this.dstFamilyPath = dstFamilyPath;
    return this;
  }

  public String getFamilyName() {
    return familyName;
  }

  public CreateHFileLinkParams familyName(String familyName) {
    this.familyName = familyName;
    return this;
  }

  public String getDstTableName() {
    return dstTableName;
  }

  public CreateHFileLinkParams dstTableName(String dstTableName) {
    this.dstTableName = dstTableName;
    return this;
  }

  public String getDstRegionName() {
    return dstRegionName;
  }

  public CreateHFileLinkParams dstRegionName(String dstRegionName) {
    this.dstRegionName = dstRegionName;
    return this;
  }

  public TableName getLinkedTable() {
    return linkedTable;
  }

  public CreateHFileLinkParams linkedTable(TableName linkedTable) {
    this.linkedTable = linkedTable;
    return this;
  }

  public String getLinkedRegion() {
    return linkedRegion;
  }

  public CreateHFileLinkParams linkedRegion(String linkedRegion) {
    this.linkedRegion = linkedRegion;
    return this;
  }

  public String getHfileName() {
    return hfileName;
  }

  public CreateHFileLinkParams hfileName(String hfileName) {
    this.hfileName = hfileName;
    return this;
  }

  public boolean isCreateBackRef() {
    return createBackRef;
  }

  public CreateHFileLinkParams createBackRef(boolean createBackRef) {
    this.createBackRef = createBackRef;
    return this;
  }

  public static CreateHFileLinkParams create(final Configuration conf, final FileSystem fs,
    final Path dstFamilyPath, final RegionInfo hfileRegionInfo, final String hfileName)
    throws IOException {
    return create(conf, fs, dstFamilyPath, hfileRegionInfo, hfileName, true);
  }

  public static CreateHFileLinkParams create(final Configuration conf, final FileSystem fs,
    final Path dstFamilyPath, final RegionInfo hfileRegionInfo, final String hfileName,
    final boolean createBackRef) throws IOException {
    TableName linkedTable = hfileRegionInfo.getTable();
    String linkedRegion = hfileRegionInfo.getEncodedName();
    return create(conf, fs, dstFamilyPath, linkedTable, linkedRegion, hfileName, createBackRef);
  }

  public static CreateHFileLinkParams create(final Configuration conf, final FileSystem fs,
    final Path dstFamilyPath, final TableName linkedTable, final String linkedRegion,
    final String hfileName) throws IOException {
    return create(conf, fs, dstFamilyPath, linkedTable, linkedRegion, hfileName, true);
  }

  public static CreateHFileLinkParams create(final Configuration conf, final FileSystem fs,
    final Path dstFamilyPath, final TableName linkedTable, final String linkedRegion,
    final String hfileName, final boolean createBackRef) throws IOException {
    String familyName = dstFamilyPath.getName();
    String regionName = dstFamilyPath.getParent().getName();
    String tableName =
      CommonFSUtils.getTableName(dstFamilyPath.getParent().getParent()).getNameAsString();

    return create(conf, fs, dstFamilyPath, familyName, tableName, regionName, linkedTable,
      linkedRegion, hfileName, createBackRef);
  }

  public static CreateHFileLinkParams create(final Configuration conf, final FileSystem fs,
    final Path dstFamilyPath, final String familyName, final String dstTableName,
    final String dstRegionName, final TableName linkedTable, final String linkedRegion,
    final String hfileName, final boolean createBackRef) throws IOException {
    return new CreateHFileLinkParams().conf(conf).fileSystem(fs).linkedTable(linkedTable)
      .linkedRegion(linkedRegion).hfileName(hfileName).createBackRef(createBackRef)
      .dstFamilyPath(dstFamilyPath).dstTableName(dstTableName).dstRegionName(dstRegionName);
  }
}
