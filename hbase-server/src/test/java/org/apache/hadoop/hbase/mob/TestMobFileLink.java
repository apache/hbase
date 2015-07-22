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
package org.apache.hadoop.hbase.mob;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMobFileLink {

  @Test
  public void testMobFilePath() throws IOException {
    TableName tableName = TableName.valueOf("testMobFilePath");
    Configuration conf = HBaseConfiguration.create();
    FileSystem fs = FileSystem.get(conf);
    Path rootDir = FSUtils.getRootDir(conf);
    Path tableDir = FSUtils.getTableDir(rootDir, tableName);
    Path archiveDir = FSUtils.getTableDir(HFileArchiveUtil.getArchivePath(conf), tableName);
    String fileName = "mobFile";
    String encodedRegionName = MobUtils.getMobRegionInfo(tableName).getEncodedName();
    String columnFamily = "columnFamily";
    Path regionDir = new Path(tableDir, encodedRegionName);
    Path archivedRegionDir = new Path(archiveDir, encodedRegionName);
    Path expectedMobFilePath = new Path(MobUtils.getMobFamilyPath(conf, tableName, columnFamily),
      fileName).makeQualified(fs.getUri(), fs.getWorkingDirectory());
    Path expectedOriginPath = new Path(new Path(regionDir, columnFamily), fileName).makeQualified(
      fs.getUri(), fs.getWorkingDirectory());
    Path expectedArchivePath = new Path(new Path(archivedRegionDir, columnFamily), fileName)
      .makeQualified(fs.getUri(), fs.getWorkingDirectory());

    String hfileLinkName = tableName.getNameAsString() + "=" + encodedRegionName + "-" + fileName;
    Path hfileLinkPath = new Path(columnFamily, hfileLinkName);
    HFileLink hfileLink = HFileLink.buildFromHFileLinkPattern(conf, hfileLinkPath);
    Assert.assertEquals(expectedMobFilePath, hfileLink.getMobPath());
    Assert.assertEquals(expectedOriginPath, hfileLink.getOriginPath());
    Assert.assertEquals(expectedArchivePath, hfileLink.getArchivePath());
  }
}
