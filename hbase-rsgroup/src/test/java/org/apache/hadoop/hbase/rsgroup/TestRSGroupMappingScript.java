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
package org.apache.hadoop.hbase.rsgroup;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfoManagerImpl.RSGroupMappingScript;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ SmallTests.class })
public class TestRSGroupMappingScript {

  private static final Logger LOG = LoggerFactory.getLogger(TestRSGroupMappingScript.class);
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRSGroupMappingScript.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private File script;

  @BeforeClass
  public static void setupScript() throws Exception {
    String currentDir = new File("").getAbsolutePath();
    UTIL.getConfiguration().set(
      RSGroupMappingScript.RS_GROUP_MAPPING_SCRIPT,
      currentDir + "/rsgroup_table_mapping.sh"
    );
  }

  @Before
  public void setup() throws Exception {
    script = new File(UTIL.getConfiguration().get(RSGroupMappingScript.RS_GROUP_MAPPING_SCRIPT));
    if (!script.createNewFile()) {
      throw new IOException("Can't create script");
    }

    PrintWriter pw = new PrintWriter(new FileOutputStream(script));
    try {
      pw.println("#!/bin/bash");
      pw.println("namespace=$1");
      pw.println("tablename=$2");
      pw.println("if [[ $namespace == test ]]; then");
      pw.println("  echo test");
      pw.println("elif [[ $tablename == *foo* ]]; then");
      pw.println("  echo other");
      pw.println("else");
      pw.println("  echo default");
      pw.println("fi");
      pw.flush();
    } finally {
      pw.close();
    }
    boolean executable = script.setExecutable(true);
    LOG.info("Created " + script  + ", executable=" + executable);
    verifyScriptContent(script);
  }

  private void verifyScriptContent(File file) throws Exception {
    BufferedReader reader = new BufferedReader(new FileReader(file));
    String line;
    while ((line = reader.readLine()) != null) {
      LOG.info(line);
    }
  }

  @Test
  public void testScript() throws Exception {
    RSGroupMappingScript script = new RSGroupMappingScript(UTIL.getConfiguration());
    TableName testNamespace =
      TableName.valueOf("test", "should_be_in_test");
    String rsgroup = script.getRSGroup(
      testNamespace.getNamespaceAsString(), testNamespace.getQualifierAsString()
    );
    Assert.assertEquals("test", rsgroup);

    TableName otherName =
      TableName.valueOf("whatever", "oh_foo_should_be_in_other");
    rsgroup = script.getRSGroup(otherName.getNamespaceAsString(), otherName.getQualifierAsString());
    Assert.assertEquals("other", rsgroup);

    TableName defaultName =
      TableName.valueOf("nono", "should_be_in_default");
    rsgroup = script.getRSGroup(
      defaultName.getNamespaceAsString(), defaultName.getQualifierAsString()
    );
    Assert.assertEquals("default", rsgroup);
  }

  @After
  public void teardown() throws Exception {
    if (script.exists()) {
      script.delete();
    }
  }

}

