/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.LauncherSecurityManager;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestDriver {
  /**
   * Test main method of Driver class
   */
  @Test
  public void testDriver() throws Throwable {

    PrintStream oldPrintStream = System.out;
    SecurityManager SECURITY_MANAGER = System.getSecurityManager();
    LauncherSecurityManager newSecurityManager= new LauncherSecurityManager();
    System.setSecurityManager(newSecurityManager);
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    String[] args = {};
    System.setOut(new PrintStream(data));
    try {
      System.setOut(new PrintStream(data));

      try {
        Driver.main(args);
        fail("should be SecurityException");
      } catch (InvocationTargetException e) {
        assertEquals(-1, newSecurityManager.getExitCode());
        assertTrue(data.toString().contains(
            "An example program must be given as the first argument."));
        assertTrue(data.toString().contains("CellCounter: Count cells in HBase table"));
        assertTrue(data.toString().contains("completebulkload: Complete a bulk data load."));
        assertTrue(data.toString().contains(
            "copytable: Export a table from local cluster to peer cluster"));
        assertTrue(data.toString().contains("export: Write table data to HDFS."));
        assertTrue(data.toString().contains("import: Import data written by Export."));
        assertTrue(data.toString().contains("importtsv: Import data in TSV format."));
        assertTrue(data.toString().contains("rowcounter: Count rows in HBase table"));
      }
    } finally {
      System.setOut(oldPrintStream);
      System.setSecurityManager(SECURITY_MANAGER);
    }

  }
}