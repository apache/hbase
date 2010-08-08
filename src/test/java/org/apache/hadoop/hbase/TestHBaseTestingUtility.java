/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase;


import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test our testing utility class
 */
public class TestHBaseTestingUtility {
  private HBaseTestingUtility hbt;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    this.hbt = new HBaseTestingUtility();
    this.hbt.cleanupTestDir();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test public void testTestDir() throws IOException {
    Path testdir = HBaseTestingUtility.getTestDir();
    FileSystem fs = this.hbt.getTestFileSystem();
    assertTrue(!fs.exists(testdir));
    assertTrue(fs.mkdirs(testdir));
    assertTrue(this.hbt.cleanupTestDir());
  }
}