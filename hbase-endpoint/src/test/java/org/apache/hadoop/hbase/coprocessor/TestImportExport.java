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
package org.apache.hadoop.hbase.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MediumTests.class})
public class TestImportExport extends org.apache.hadoop.hbase.mapreduce.TestImportExport {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestImportExport.class);

  @BeforeClass
  public static void beforeClass() throws Throwable {
    UTIL.getConfiguration().setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      org.apache.hadoop.hbase.coprocessor.Export.class.getName());
    org.apache.hadoop.hbase.mapreduce.TestImportExport.beforeClass();
  }

  @Override
  protected boolean runExport(String[] args) throws Throwable {
    Export.run(new Configuration(UTIL.getConfiguration()), args);
    return true;
  }

  @Override
  protected void runExportMain(String[] args) throws Throwable {
    Export.main(args);
  }

  /**
   * Skip the test which is unrelated to the coprocessor.Export.
   */
  @Test
  @Ignore
  public void testImport94Table() throws Throwable {
  }
}
