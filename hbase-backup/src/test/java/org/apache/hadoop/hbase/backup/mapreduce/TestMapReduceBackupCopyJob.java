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
package org.apache.hadoop.hbase.backup.mapreduce;

import static org.apache.hadoop.hbase.backup.mapreduce.MapReduceBackupCopyJob.BACKUP_COPY_OPTION_PREFIX;
import static org.apache.hadoop.tools.DistCpConstants.CONF_LABEL_DIRECT_WRITE;
import static org.apache.hadoop.tools.DistCpConstants.CONF_LABEL_MAX_MAPS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;

@Tag(SmallTests.TAG)
public class TestMapReduceBackupCopyJob {

  @Test
  public void testDistCpOptionParsing() {
    Configuration conf = new Configuration();
    conf.setInt(BACKUP_COPY_OPTION_PREFIX + CONF_LABEL_MAX_MAPS, 1000);
    conf.setBoolean(BACKUP_COPY_OPTION_PREFIX + CONF_LABEL_DIRECT_WRITE, true);
    List<String> args = MapReduceBackupCopyJob.parseDistCpOptions(conf);

    List<String> expectedArgs =
      ImmutableList.<String> builder().add("-m", "1000").add("-direct").build();

    assertEquals(expectedArgs, args);
  }

  @Test
  public void testCopyResetsErrorFromPreviousCopy() throws IOException {
    Configuration conf = new Configuration();
    Path root = new Path(Files.createTempDirectory("backup-copy-job").toString());
    String missingSource = new Path(root, "missing").toString();
    String dest = new Path(root, "dest").toString();
    MapReduceBackupCopyJob copyJob = new MapReduceBackupCopyJob();
    copyJob.setConf(conf);

    int failedCopy = copyJob.copy(new BackupInfo(), null, conf, BackupType.INCREMENTAL,
      new String[] { missingSource, dest });
    assertNotEquals(0, failedCopy);
    assertTrue(copyJob.getError().isPresent());

    int invalidArgsCopy =
      copyJob.copy(new BackupInfo(), null, conf, BackupType.INCREMENTAL, new String[] { dest });
    assertNotEquals(0, invalidArgsCopy);
    assertFalse(copyJob.getError().isPresent());
  }

}
