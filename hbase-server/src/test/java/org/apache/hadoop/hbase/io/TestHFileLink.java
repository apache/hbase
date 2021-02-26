/**
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.regex.Matcher;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test that FileLink switches between alternate locations
 * when the current location moves or gets deleted.
 */
@Category({IOTests.class, SmallTests.class})
public class TestHFileLink {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHFileLink.class);

  @Rule
  public TestName name = new TestName();

  @Test
  public void testValidLinkNames() {
    String validLinkNames[] = {"foo=fefefe-0123456", "ns=foo=abababa-fefefefe"};

    for(String name : validLinkNames) {
      Assert.assertTrue("Failed validating:" + name, name.matches(HFileLink.LINK_NAME_REGEX));
    }

    for(String name : validLinkNames) {
      Assert.assertTrue("Failed validating:" + name, HFileLink.isHFileLink(name));
    }

    String testName = name.getMethodName() + "=fefefe-0123456";
    Assert.assertEquals(TableName.valueOf(name.getMethodName()),
        HFileLink.getReferencedTableName(testName));
    Assert.assertEquals("fefefe", HFileLink.getReferencedRegionName(testName));
    Assert.assertEquals("0123456", HFileLink.getReferencedHFileName(testName));
    Assert.assertEquals(testName,
        HFileLink.createHFileLinkName(TableName.valueOf(name.getMethodName()), "fefefe", "0123456"));

    testName = "ns=" + name.getMethodName() + "=fefefe-0123456";
    Assert.assertEquals(TableName.valueOf("ns", name.getMethodName()),
        HFileLink.getReferencedTableName(testName));
    Assert.assertEquals("fefefe", HFileLink.getReferencedRegionName(testName));
    Assert.assertEquals("0123456", HFileLink.getReferencedHFileName(testName));
    Assert.assertEquals(testName,
        HFileLink.createHFileLinkName(TableName.valueOf("ns", name.getMethodName()), "fefefe", "0123456"));

    for(String name : validLinkNames) {
      Matcher m = HFileLink.LINK_NAME_PATTERN.matcher(name);
      assertTrue(m.matches());
      Assert.assertEquals(HFileLink.getReferencedTableName(name),
          TableName.valueOf(m.group(1), m.group(2)));
      Assert.assertEquals(HFileLink.getReferencedRegionName(name),
          m.group(3));
      Assert.assertEquals(HFileLink.getReferencedHFileName(name),
          m.group(4));
    }
  }

  @Test
  public void testBackReference() {
    Path rootDir = new Path("/root");
    Path archiveDir = new Path(rootDir, ".archive");
    String storeFileName = "121212";
    String linkDir = FileLink.BACK_REFERENCES_DIRECTORY_PREFIX + storeFileName;
    String encodedRegion = "FEFE";
    String cf = "cf1";

    TableName refTables[] = {TableName.valueOf(name.getMethodName()),
        TableName.valueOf("ns", name.getMethodName())};

    for(TableName refTable : refTables) {
      Path refTableDir = CommonFSUtils.getTableDir(archiveDir, refTable);
      Path refRegionDir = HRegion.getRegionDir(refTableDir, encodedRegion);
      Path refDir = new Path(refRegionDir, cf);
      Path refLinkDir = new Path(refDir, linkDir);
      String refStoreFileName = refTable.getNameAsString().replace(
          TableName.NAMESPACE_DELIM, '=') + "=" + encodedRegion + "-" + storeFileName;

      TableName tableNames[] = {TableName.valueOf(name.getMethodName() + "1"),
              TableName.valueOf("ns", name.getMethodName() + "2"),
              TableName.valueOf(name.getMethodName()+ ":" +name.getMethodName())};

      for( TableName tableName : tableNames) {
        Path tableDir = CommonFSUtils.getTableDir(rootDir, tableName);
        Path regionDir = HRegion.getRegionDir(tableDir, encodedRegion);
        Path cfDir = new Path(regionDir, cf);

        // Verify back reference creation
        assertEquals(
          encodedRegion + "." + tableName.getNameAsString().replace(TableName.NAMESPACE_DELIM, '='),
          HFileLink.createBackReferenceName(CommonFSUtils.getTableName(tableDir).getNameAsString(),
            encodedRegion));

        //verify parsing back reference
        Pair<TableName, String> parsedRef =
            HFileLink.parseBackReferenceName(encodedRegion+"."+
                tableName.getNameAsString().replace(TableName.NAMESPACE_DELIM, '='));
        assertEquals(parsedRef.getFirst(), tableName);
        assertEquals(encodedRegion, parsedRef.getSecond());

        //verify resolving back reference
        Path storeFileDir =  new Path(refLinkDir, encodedRegion+"."+
            tableName.getNameAsString().replace(TableName.NAMESPACE_DELIM, '='));
        Path linkPath = new Path(cfDir, refStoreFileName);
        assertEquals(linkPath, HFileLink.getHFileFromBackReference(rootDir, storeFileDir));
      }
    }
  }


}
