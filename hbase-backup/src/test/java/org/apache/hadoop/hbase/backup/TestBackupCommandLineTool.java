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
package org.apache.hadoop.hbase.backup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestBackupCommandLineTool {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBackupCommandLineTool.class);

  private final static String USAGE_DESCRIBE = "Usage: hbase backup describe <backup_id>";
  private final static String USAGE_CREATE = "Usage: hbase backup create";
  private final static String USAGE_HISTORY = "Usage: hbase backup history";
  private final static String USAGE_BACKUP = "Usage: hbase backup";
  private final static String USAGE_DELETE = "Usage: hbase backup delete";
  private final static String USAGE_PROGRESS = "Usage: hbase backup progress";
  private final static String USAGE_SET = "Usage: hbase backup set";
  private final static String USAGE_RESTORE = "Usage: hbase restore";

  Configuration conf;

  @Before
  public void setUpBefore() throws Exception {
    conf = HBaseConfiguration.create();
    conf.setBoolean(BackupRestoreConstants.BACKUP_ENABLE_KEY, true);
  }

  @Test
  public void testBackupDriverDescribeHelp() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    String[] args = new String[] { "describe", "-help" };
    ToolRunner.run(conf, new BackupDriver(), args);

    String output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_DESCRIBE) >= 0);

    baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    args = new String[] { "describe", "-h" };
    ToolRunner.run(conf, new BackupDriver(), args);

    output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_DESCRIBE) >= 0);

    baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    args = new String[] { "describe" };
    ToolRunner.run(conf, new BackupDriver(), args);

    output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_DESCRIBE) >= 0);
  }


  @Test
  public void testBackupDriverCreateTopLevelBackupDest() throws Exception {
    String[] args = new String[] { "create", "full", "hdfs://localhost:1020", "-t", "t1" };
    int result = ToolRunner.run(conf, new BackupDriver(), args);
    // FAILED
    assertEquals(1, result);
  }

  @Test
  public void testBackupDriverCreateHelp() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    String[] args = new String[] { "create", "-help" };
    ToolRunner.run(conf, new BackupDriver(), args);

    String output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_CREATE) >= 0);
    assertTrue(output.indexOf(BackupRestoreConstants.OPTION_TABLE_LIST_DESC) > 0);


    baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    args = new String[] { "create", "-h" };
    ToolRunner.run(conf, new BackupDriver(), args);

    output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_CREATE) >= 0);
    assertTrue(output.indexOf(BackupRestoreConstants.OPTION_TABLE_LIST_DESC) > 0);

    baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    args = new String[] { "create" };
    ToolRunner.run(conf, new BackupDriver(), args);

    output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_CREATE) >= 0);
    assertTrue(output.indexOf(BackupRestoreConstants.OPTION_TABLE_LIST_DESC) > 0);

  }

  @Test
  public void testBackupDriverHistoryHelp() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    String[] args = new String[] { "history", "-help" };
    ToolRunner.run(conf, new BackupDriver(), args);

    String output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_HISTORY) >= 0);

    baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    args = new String[] { "history", "-h" };
    ToolRunner.run(conf, new BackupDriver(), args);

    output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_HISTORY) >= 0);

  }

  @Test
  public void testBackupDriverDeleteHelp() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    String[] args = new String[] { "delete", "-help" };
    ToolRunner.run(conf, new BackupDriver(), args);

    String output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_DELETE) >= 0);

    baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    args = new String[] { "delete", "-h" };
    ToolRunner.run(conf, new BackupDriver(), args);

    output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_DELETE) >= 0);

    baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    args = new String[] { "delete" };
    ToolRunner.run(conf, new BackupDriver(), args);

    output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_DELETE) >= 0);
  }

  @Test
  public void testBackupDriverProgressHelp() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    String[] args = new String[] { "progress", "-help" };
    ToolRunner.run(conf, new BackupDriver(), args);

    String output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_PROGRESS) >= 0);

    baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    args = new String[] { "progress", "-h" };
    ToolRunner.run(conf, new BackupDriver(), args);

    output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_PROGRESS) >= 0);
  }

  @Test
  public void testBackupDriverSetHelp() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    String[] args = new String[] { "set", "-help" };
    ToolRunner.run(conf, new BackupDriver(), args);

    String output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_SET) >= 0);

    baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    args = new String[] { "set", "-h" };
    ToolRunner.run(conf, new BackupDriver(), args);

    output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_SET) >= 0);

    baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    args = new String[] { "set" };
    ToolRunner.run(conf, new BackupDriver(), args);

    output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_SET) >= 0);

  }

  @Test
  public void testBackupDriverHelp() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    String[] args = new String[] { "-help" };
    ToolRunner.run(conf, new BackupDriver(), args);

    String output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_BACKUP) >= 0);
    baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    args = new String[] { "-h" };
    ToolRunner.run(conf, new BackupDriver(), args);

    output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_BACKUP) >= 0);

  }

  @Test
  public void testRestoreDriverHelp() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    String[] args = new String[] { "-help" };
    ToolRunner.run(conf, new RestoreDriver(), args);

    String output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_RESTORE) >= 0);
    assertTrue(output.indexOf(BackupRestoreConstants.OPTION_TABLE_LIST_DESC) > 0);

    baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    args = new String[] { "-h" };
    ToolRunner.run(conf, new RestoreDriver(), args);

    output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_RESTORE) >= 0);
    assertTrue(output.indexOf(BackupRestoreConstants.OPTION_TABLE_LIST_DESC) > 0);

  }

  @Test
  public void testBackupDriverUnrecognizedCommand() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    String[] args = new String[] { "command" };
    ToolRunner.run(conf, new BackupDriver(), args);

    String output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_BACKUP) >= 0);

    baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    args = new String[] { "command" };
    ToolRunner.run(conf, new BackupDriver(), args);

    output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_BACKUP) >= 0);
  }

  @Test
  public void testBackupDriverUnrecognizedOption() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    String[] args = new String[] { "create", "-xx" };
    ToolRunner.run(conf, new BackupDriver(), args);

    String output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_BACKUP) >= 0);

    baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    args = new String[] { "describe", "-xx" };
    ToolRunner.run(conf, new BackupDriver(), args);

    output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_BACKUP) >= 0);

    baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    args = new String[] { "history", "-xx" };
    ToolRunner.run(conf, new BackupDriver(), args);

    output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_BACKUP) >= 0);

    baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    args = new String[] { "delete", "-xx" };
    ToolRunner.run(conf, new BackupDriver(), args);

    output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_BACKUP) >= 0);

    baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    args = new String[] { "set", "-xx" };
    ToolRunner.run(conf, new BackupDriver(), args);

    output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_BACKUP) >= 0);
  }

  @Test
  public void testRestoreDriverUnrecognizedOption() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    String[] args = new String[] { "-xx" };
    ToolRunner.run(conf, new RestoreDriver(), args);

    String output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_RESTORE) >= 0);

  }

  @Test
  public void testBackupDriverCreateWrongArgNumber() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    String[] args = new String[] { "create" };
    ToolRunner.run(conf, new BackupDriver(), args);

    String output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_CREATE) >= 0);

    baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    args = new String[] { "create", "22" };
    ToolRunner.run(conf, new BackupDriver(), args);

    output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_CREATE) >= 0);

    baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    args = new String[] { "create", "22", "22", "22", "22", "22" };
    ToolRunner.run(conf, new BackupDriver(), args);

    output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_CREATE) >= 0);
  }

  @Test
  public void testBackupDriverDeleteWrongArgNumber() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    String[] args = new String[] { "delete" };
    ToolRunner.run(conf, new BackupDriver(), args);

    String output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_DELETE) >= 0);

  }

  @Test
  public void testBackupDriverHistoryWrongArgs() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    String[] args = new String[] { "history", "-n", "xx" };
    ToolRunner.run(conf, new BackupDriver(), args);

    String output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf(USAGE_HISTORY) >= 0);

  }

  @Test
  public void testBackupDriverWrongBackupDestination() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    String[] args = new String[] { "create", "full", "clicks" };
    ToolRunner.run(conf, new BackupDriver(), args);

    String output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf("ERROR: invalid backup destination") >= 0);

  }

  @Test
  public void testBackupDriverBackupSetAndList() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    String[] args = new String[] { "create", "full", "file:/localhost", "-t", "clicks", "-s", "s" };
    ToolRunner.run(conf, new BackupDriver(), args);

    String output = baos.toString();
    System.out.println(baos.toString());
    assertTrue(output.indexOf("ERROR: You can specify either backup set or list") >= 0);

  }

}
