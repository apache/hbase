/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.mapreduce;

import com.google.common.collect.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.*;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;

@Category({SmallTests.class})
public class TestMultiTableSnapshotInputFormatImpl {

  private MultiTableSnapshotInputFormatImpl subject;
  private Map<String, Collection<Scan>> snapshotScans;
  private Path restoreDir;
  private Configuration conf;
  private Path rootDir;

  @Before
  public void setUp() throws Exception {
    this.subject = Mockito.spy(new MultiTableSnapshotInputFormatImpl());

    // mock out restoreSnapshot
    // TODO: this is kind of meh; it'd be much nicer to just inject the RestoreSnapshotHelper dependency into the
    // input format. However, we need a new RestoreSnapshotHelper per snapshot in the current design, and it *also*
    // feels weird to introduce a RestoreSnapshotHelperFactory and inject that, which would probably be the more "pure"
    // way of doing things. This is the lesser of two evils, perhaps?
    doNothing().when(this.subject).
        restoreSnapshot(any(Configuration.class), any(String.class), any(Path.class), any(Path.class), any(FileSystem.class)) ;

    this.conf = new Configuration();
    this.rootDir = new Path("file:///test-root-dir");
    FSUtils.setRootDir(conf, rootDir);
    this.snapshotScans = ImmutableMap.<String, Collection<Scan>>of(
        "snapshot1", ImmutableList.of(new Scan(Bytes.toBytes("1"), Bytes.toBytes("2"))),
        "snapshot2", ImmutableList.of(new Scan(Bytes.toBytes("3"), Bytes.toBytes("4")),
                                      new Scan(Bytes.toBytes("5"), Bytes.toBytes("6")))
    );

    this.restoreDir = new Path(FSUtils.getRootDir(conf), "restore-dir");

  }

  public void callSetInput() throws IOException {
    subject.setInput(this.conf, snapshotScans, restoreDir);
  }

  public Map<String, Collection<ScanWithEquals>> toScanWithEquals(Map<String, Collection<Scan>> snapshotScans) throws IOException {
    Map<String, Collection<ScanWithEquals>> rtn = Maps.newHashMap();

    for (Map.Entry<String, Collection<Scan>> entry : snapshotScans.entrySet()) {
      List<ScanWithEquals> scans = Lists.newArrayList();

      for (Scan scan : entry.getValue()) {
        scans.add(new ScanWithEquals(scan));
      }
      rtn.put(entry.getKey(), scans);
    }

    return rtn;
  }

  public static class ScanWithEquals  {

    private final String startRow;
    private final String stopRow;

    /**
     * Creates a new instance of this class while copying all values.
     *
     * @param scan The scan instance to copy from.
     * @throws java.io.IOException When copying the values fails.
     */
    public ScanWithEquals(Scan scan) throws IOException {
      this.startRow = Bytes.toStringBinary(scan.getStartRow());
      this.stopRow = Bytes.toStringBinary(scan.getStopRow());
    }

    @Override
    public boolean equals(Object obj) {
      if (! (obj instanceof ScanWithEquals)) {
        return false;
      }
      ScanWithEquals otherScan = (ScanWithEquals) obj;
      return Objects.equals(this.startRow, otherScan.startRow) && Objects.equals(this.stopRow, otherScan.stopRow);
    }

    @Override
    public String toString() {
      return com.google.common.base.Objects.toStringHelper(this)
          .add("startRow", startRow)
          .add("stopRow", stopRow)
          .toString();
    }
  }

  @Test
  public void testSetInputSetsSnapshotToScans() throws Exception {

    callSetInput();

    Map<String, Collection<Scan>> actual = subject.getSnapshotsToScans(conf);

    // convert to scans we can use .equals on
    Map<String, Collection<ScanWithEquals>> actualWithEquals = toScanWithEquals(actual);
    Map<String, Collection<ScanWithEquals>> expectedWithEquals = toScanWithEquals(snapshotScans);

    assertEquals(expectedWithEquals, actualWithEquals);
  }

  @Test
  public void testSetInputPushesRestoreDirectories() throws Exception {
    callSetInput();

    Map<String, Path> restoreDirs = subject.getSnapshotDirs(conf);

    assertEquals(this.snapshotScans.keySet(), restoreDirs.keySet());
  }

  @Test
  public void testSetInputCreatesRestoreDirectoriesUnderRootRestoreDir() throws Exception {
    callSetInput();

    Map<String, Path> restoreDirs = subject.getSnapshotDirs(conf);

    for (Path snapshotDir : restoreDirs.values()) {
      assertEquals("Expected " + snapshotDir + " to be a child of " + restoreDir, restoreDir, snapshotDir.getParent());
    }
  }

  @Test
  public void testSetInputRestoresSnapshots() throws Exception {
    callSetInput();

    Map<String, Path> snapshotDirs = subject.getSnapshotDirs(conf);

    for (Map.Entry<String, Path> entry : snapshotDirs.entrySet()) {
      verify(this.subject).restoreSnapshot(eq(this.conf), eq(entry.getKey()), eq(this.rootDir), eq(entry.getValue()), any(FileSystem.class));
    }
  }
}
