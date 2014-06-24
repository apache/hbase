/**
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.StoppableImpl;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.junit.experimental.categories.Category;

import java.net.URLEncoder;
import java.util.Calendar;

@Category(MediumTests.class)
public class TestOldLogsCleaner {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();


  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testLogCleaning() throws Exception{
    Configuration c = TEST_UTIL.getConfiguration();
    // set TTL
    long ttl = 2000;
    c.setLong("hbase.master.logcleaner.ttl", ttl);
    Path oldLogDir = new Path(TEST_UTIL.getTestDir(),
        HConstants.HREGION_OLDLOGDIR_NAME);
    String fakeMachineName = URLEncoder.encode("regionserver:60020", "UTF8");

    FileSystem fs = FileSystem.get(c);
    StoppableImpl stop = new StoppableImpl();
    OldLogsCleaner cleaner = new OldLogsCleaner(1000, stop,c, fs, oldLogDir);

    // Create 2 invalid files, 1 "recent" file, 1 very new file and 30 old files
    long now = System.currentTimeMillis();
    fs.delete(oldLogDir, true);
    fs.mkdirs(oldLogDir);
    fs.createNewFile(new Path(oldLogDir, "a"));
    fs.createNewFile(new Path(oldLogDir, fakeMachineName + "." + "a"));
    System.out.println("Now is: " + now);
    for (int i = 1; i < 31; i++) {
      fs.createNewFile(new Path(oldLogDir, fakeMachineName + "." + (now - i) ));
    }
    // sleep for sometime to get newer modifcation time
    Thread.sleep(ttl);
    fs.createNewFile(new Path(oldLogDir, fakeMachineName + "." + now));
    fs.createNewFile(new Path(oldLogDir, fakeMachineName + "." + (now + 10000) ));

    for (FileStatus stat : fs.listStatus(oldLogDir)) {
      System.out.println(stat.getPath().toString());
    }

    assertEquals(34, fs.listStatus(oldLogDir).length);

    // This will take care of 20 old log files (default max we can delete)
    cleaner.chore();

    assertEquals(14, fs.listStatus(oldLogDir).length);

    // We will delete all remaining log files and those that are invalid
    cleaner.chore();

    // We end up with the current log file and a newer one
    assertEquals(2, fs.listStatus(oldLogDir).length);
  }

  @Test
  public void testLogCleaningWithArchivingToHourlyDir() throws Exception{
    Configuration c = TEST_UTIL.getConfiguration();
    // set TTL to delete 5 hours
    c.setLong("hbase.master.logcleaner.ttl", 4 * 3600 * 1000);
    c.setBoolean("hbase.hlog.archive.hourlydir", true);
    Path oldLogDir = new Path(TEST_UTIL.getTestDir(),
        HConstants.HREGION_OLDLOGDIR_NAME);
    String fakeMachineName = URLEncoder.encode("regionserver:60020", "UTF8");

    FileSystem fs = FileSystem.get(c);
    StoppableImpl stop = new StoppableImpl();
    OldLogsCleaner cleaner = new OldLogsCleaner(1000, stop, c, fs, oldLogDir);

    // Create 1 invalid directory (considering legacy logs), 10 directories representing
    // recent 10 hours respectively
    fs.delete(oldLogDir, true);
    fs.mkdirs(oldLogDir);
    Path legacyDir = new Path(oldLogDir, "abc");
    fs.mkdirs(legacyDir);
    fs.createNewFile(new Path(legacyDir, "123.456"));
    Calendar cal = Calendar.getInstance();
    System.out.println("Now is: " + HLog.getDateFormat().format(cal.getTime()));
    for (int i = 0; i < 10; i++) {
      cal.add(Calendar.HOUR, -1);
      Path hourDir = new Path(oldLogDir,
          HLog.getDateFormat().format(cal.getTime()));
      fs.mkdirs(hourDir);
      fs.createNewFile(new Path(hourDir, new Path(fakeMachineName + "." + i)));
    }

    for (FileStatus stat : fs.listStatus(oldLogDir)) {
      System.out.println(stat.getPath().toString());
    }

    assertEquals(11, fs.listStatus(oldLogDir).length);

    // This will delete oldest sub-directory
    cleaner.chore();

    assertEquals(10, fs.listStatus(oldLogDir).length);

    // We will delete all log dir older than 4 hours
    for (int i = 0; i < 10; i++) {
      cleaner.chore();
    }

    // We should still see 3 newer dirs and an legacy one
    assertEquals(4, fs.listStatus(oldLogDir).length);

    // Verify file "123.456" still exists
    assertEquals(1, fs.listStatus(legacyDir).length);

    Thread.sleep(1000);

    // Update TTL configuration to delete all logs
    c.setLong("hbase.master.logcleaner.ttl", 800);
    cleaner.updateLogCleanerConf(c);

    // Delete an hourly dir. File "123.456" should also be deleted this time.
    cleaner.chore();
    assertEquals(3, fs.listStatus(oldLogDir).length);
    assertEquals(0, fs.listStatus(legacyDir).length);

    // Delete an hourly dir. Dir "abc" should also be deleted this time.
    cleaner.chore();
    assertEquals(1, fs.listStatus(oldLogDir).length);

    // Delete the last
    cleaner.chore();
    assertEquals(0, fs.listStatus(oldLogDir).length);
  }
}
