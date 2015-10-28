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
package org.apache.hadoop.hbase.util.hbck;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter.ERROR_CODE;

public class HbckTestingUtil {
  private static ExecutorService exec = new ScheduledThreadPoolExecutor(10);
  public static HBaseFsck doFsck(
      Configuration conf, boolean fix) throws Exception {
    return doFsck(conf, fix, null);
  }

  public static HBaseFsck doFsck(
      Configuration conf, boolean fix, TableName table) throws Exception {
    return doFsck(conf, fix, fix, fix, fix, fix, fix, fix, fix, fix, fix, fix, table);
  }

  public static HBaseFsck doFsck(Configuration conf, boolean fixAssignments, boolean fixMeta,
      boolean fixHdfsHoles, boolean fixHdfsOverlaps, boolean fixHdfsOrphans,
      boolean fixTableOrphans, boolean fixVersionFile, boolean fixReferenceFiles,
      boolean fixEmptyMetaRegionInfo, boolean fixTableLocks, Boolean fixReplication,
      TableName table) throws Exception {
    HBaseFsck fsck = new HBaseFsck(conf, exec);
    try {
      HBaseFsck.setDisplayFullReport(); // i.e. -details
      fsck.setTimeLag(0);
      fsck.setFixAssignments(fixAssignments);
      fsck.setFixMeta(fixMeta);
      fsck.setFixHdfsHoles(fixHdfsHoles);
      fsck.setFixHdfsOverlaps(fixHdfsOverlaps);
      fsck.setFixHdfsOrphans(fixHdfsOrphans);
      fsck.setFixTableOrphans(fixTableOrphans);
      fsck.setFixVersionFile(fixVersionFile);
      fsck.setFixReferenceFiles(fixReferenceFiles);
      fsck.setFixEmptyMetaCells(fixEmptyMetaRegionInfo);
      fsck.setFixTableLocks(fixTableLocks);
      fsck.setFixReplication(fixReplication);
      if (table != null) {
        fsck.includeTable(table);
      }

      // Parse command line flags before connecting, to grab the lock.
      fsck.connect();
      fsck.onlineHbck();
    } finally {
      fsck.close();
    }
    return fsck;
  }

  /**
   * Runs hbck with the -sidelineCorruptHFiles option
   * @param conf
   * @param table table constraint
   * @return <returncode, hbckInstance>
   * @throws Exception
   */
  public static HBaseFsck doHFileQuarantine(Configuration conf, TableName table) throws Exception {
    String[] args = {"-sidelineCorruptHFiles", "-ignorePreCheckPermission", table.getNameAsString()};
    HBaseFsck hbck = new HBaseFsck(conf, exec);
    hbck.exec(exec, args);
    return hbck;
  }

  public static boolean inconsistencyFound(HBaseFsck fsck) throws Exception {
    List<ERROR_CODE> errs = fsck.getErrors().getErrorList();
    return (errs != null && !errs.isEmpty());
  }

  public static void assertNoErrors(HBaseFsck fsck) throws Exception {
    List<ERROR_CODE> errs = fsck.getErrors().getErrorList();
    assertEquals(new ArrayList<ERROR_CODE>(), errs);
  }

  public static void assertErrors(HBaseFsck fsck, ERROR_CODE[] expectedErrors) {
    List<ERROR_CODE> errs = fsck.getErrors().getErrorList();
    Collections.sort(errs);
    List<ERROR_CODE> expErrs = Lists.newArrayList(expectedErrors);
    Collections.sort(expErrs);
    assertEquals(expErrs, errs);
  }
}
