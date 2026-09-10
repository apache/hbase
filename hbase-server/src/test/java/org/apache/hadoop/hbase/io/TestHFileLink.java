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
package org.apache.hadoop.hbase.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.regex.Matcher;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Test that FileLink switches between alternate locations when the current location moves or gets
 * deleted.
 */
@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestHFileLink {

  @Test
  public void testValidLinkNames(TestInfo testInfo) {
    String validLinkNames[] = { "foo=fefefe-0123456", "ns=foo=abababa-fefefefe" };

    for (String name : validLinkNames) {
      assertTrue(name.matches(HFileLink.LINK_NAME_REGEX), "Failed validating:" + name);
    }

    for (String name : validLinkNames) {
      assertTrue(HFileLink.isHFileLink(name), "Failed validating:" + name);
    }

    String testName = testInfo.getTestMethod().get().getName() + "=fefefe-0123456";
    assertEquals(TableName.valueOf(testInfo.getTestMethod().get().getName()),
      HFileLink.getReferencedTableName(testName));
    assertEquals("fefefe", HFileLink.getReferencedRegionName(testName));
    assertEquals("0123456", HFileLink.getReferencedHFileName(testName));
    assertEquals(testName, HFileLink.createHFileLinkName(
      TableName.valueOf(testInfo.getTestMethod().get().getName()), "fefefe", "0123456"));

    testName = "ns=" + testInfo.getTestMethod().get().getName() + "=fefefe-0123456";
    assertEquals(TableName.valueOf("ns", testInfo.getTestMethod().get().getName()),
      HFileLink.getReferencedTableName(testName));
    assertEquals("fefefe", HFileLink.getReferencedRegionName(testName));
    assertEquals("0123456", HFileLink.getReferencedHFileName(testName));
    assertEquals(testName, HFileLink.createHFileLinkName(
      TableName.valueOf("ns", testInfo.getTestMethod().get().getName()), "fefefe", "0123456"));

    for (String name : validLinkNames) {
      Matcher m = HFileLink.LINK_NAME_PATTERN.matcher(name);
      assertTrue(m.matches());
      assertEquals(HFileLink.getReferencedTableName(name),
        TableName.valueOf(m.group(1), m.group(2)));
      assertEquals(HFileLink.getReferencedRegionName(name), m.group(3));
      assertEquals(HFileLink.getReferencedHFileName(name), m.group(4));
    }
  }

  @Test
  public void testBackReference(TestInfo testInfo) {
    Path rootDir = new Path("/root");
    Path archiveDir = new Path(rootDir, ".archive");
    String storeFileName = "121212";
    String linkDir = FileLink.BACK_REFERENCES_DIRECTORY_PREFIX + storeFileName;
    String encodedRegion = "FEFE";
    String cf = "cf1";

    TableName refTables[] = { TableName.valueOf(testInfo.getTestMethod().get().getName()),
      TableName.valueOf("ns", testInfo.getTestMethod().get().getName()) };

    for (TableName refTable : refTables) {
      Path refTableDir = CommonFSUtils.getTableDir(archiveDir, refTable);
      Path refRegionDir = HRegion.getRegionDir(refTableDir, encodedRegion);
      Path refDir = new Path(refRegionDir, cf);
      Path refLinkDir = new Path(refDir, linkDir);
      String refStoreFileName = refTable.getNameAsString().replace(TableName.NAMESPACE_DELIM, '=')
        + "=" + encodedRegion + "-" + storeFileName;

      TableName tableNames[] = { TableName.valueOf(testInfo.getTestMethod().get().getName() + "1"),
        TableName.valueOf("ns", testInfo.getTestMethod().get().getName() + "2"),
        TableName.valueOf(testInfo.getTestMethod().get().getName() + ":"
          + testInfo.getTestMethod().get().getName()) };

      for (TableName tableName : tableNames) {
        Path tableDir = CommonFSUtils.getTableDir(rootDir, tableName);
        Path regionDir = HRegion.getRegionDir(tableDir, encodedRegion);
        Path cfDir = new Path(regionDir, cf);

        assertEquals(
          encodedRegion + "." + tableName.getNameAsString().replace(TableName.NAMESPACE_DELIM, '='),
          HFileLink.createBackReferenceName(CommonFSUtils.getTableName(tableDir).getNameAsString(),
            encodedRegion));

        Pair<TableName, String> parsedRef = HFileLink.parseBackReferenceName(encodedRegion + "."
          + tableName.getNameAsString().replace(TableName.NAMESPACE_DELIM, '='));
        assertEquals(parsedRef.getFirst(), tableName);
        assertEquals(encodedRegion, parsedRef.getSecond());

        Path storeFileDir = new Path(refLinkDir, encodedRegion + "."
          + tableName.getNameAsString().replace(TableName.NAMESPACE_DELIM, '='));
        Path linkPath = new Path(cfDir, refStoreFileName);
        assertEquals(linkPath, HFileLink.getHFileFromBackReference(rootDir, storeFileDir));
      }
    }
  }

}
