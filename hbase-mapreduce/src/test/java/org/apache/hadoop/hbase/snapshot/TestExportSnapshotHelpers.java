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
package org.apache.hadoop.hbase.snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotFileInfo;

/**
 * Test Export Snapshot Tool helpers
 */
@Category({ RegionServerTests.class, SmallTests.class })
public class TestExportSnapshotHelpers {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestExportSnapshotHelpers.class);

  /**
   * Verfy the result of getBalanceSplits() method. The result are groups of files, used as input
   * list for the "export" mappers. All the groups should have similar amount of data. The input
   * list is a pair of file path and length. The getBalanceSplits() function sort it by length, and
   * assign to each group a file, going back and forth through the groups.
   */
  @Test
  public void testBalanceSplit() {
    // Create a list of files
    List<Pair<SnapshotFileInfo, Long>> files = new ArrayList<>(21);
    for (long i = 0; i <= 20; i++) {
      SnapshotFileInfo fileInfo = SnapshotFileInfo.newBuilder().setType(SnapshotFileInfo.Type.HFILE)
        .setHfile("file-" + i).build();
      files.add(new Pair<>(fileInfo, i));
    }

    // Create 5 groups (total size 210)
    // group 0: 20, 11, 10, 1 (total size: 42)
    // group 1: 19, 12, 9, 2 (total size: 42)
    // group 2: 18, 13, 8, 3 (total size: 42)
    // group 3: 17, 12, 7, 4 (total size: 42)
    // group 4: 16, 11, 6, 5 (total size: 42)
    List<List<Pair<SnapshotFileInfo, Long>>> splits = ExportSnapshot.getBalancedSplits(files, 5);
    assertEquals(5, splits.size());

    String[] split0 = new String[] { "file-20", "file-11", "file-10", "file-1", "file-0" };
    verifyBalanceSplit(splits.get(0), split0, 42);
    String[] split1 = new String[] { "file-19", "file-12", "file-9", "file-2" };
    verifyBalanceSplit(splits.get(1), split1, 42);
    String[] split2 = new String[] { "file-18", "file-13", "file-8", "file-3" };
    verifyBalanceSplit(splits.get(2), split2, 42);
    String[] split3 = new String[] { "file-17", "file-14", "file-7", "file-4" };
    verifyBalanceSplit(splits.get(3), split3, 42);
    String[] split4 = new String[] { "file-16", "file-15", "file-6", "file-5" };
    verifyBalanceSplit(splits.get(4), split4, 42);
  }

  private void verifyBalanceSplit(final List<Pair<SnapshotFileInfo, Long>> split,
    final String[] expected, final long expectedSize) {
    assertEquals(expected.length, split.size());
    long totalSize = 0;
    for (int i = 0; i < expected.length; ++i) {
      Pair<SnapshotFileInfo, Long> fileInfo = split.get(i);
      assertEquals(expected[i], fileInfo.getFirst().getHfile());
      totalSize += fileInfo.getSecond();
    }
    assertEquals(expectedSize, totalSize);
  }

  @Test
  public void testGroupFilesForSplitsWithoutCustomFileGrouper() {
    List<Pair<SnapshotFileInfo, Long>> files = new ArrayList<>();
    for (long i = 0; i < 10; i++) {
      SnapshotFileInfo fileInfo = SnapshotFileInfo.newBuilder().setType(SnapshotFileInfo.Type.HFILE)
        .setHfile("file-" + i).build();
      files.add(new Pair<>(fileInfo, i * 10));
    }

    Configuration conf = new Configuration();
    conf.setInt("snapshot.export.format.splits", 3);

    ExportSnapshot.ExportSnapshotInputFormat inputFormat =
      new ExportSnapshot.ExportSnapshotInputFormat();
    Collection<List<Pair<SnapshotFileInfo, Long>>> groups =
      inputFormat.groupFilesForSplits(conf, files);

    assertEquals("Should create 3 groups", 3, groups.size());

    long totalSize = 0;
    int totalFiles = 0;
    for (List<Pair<SnapshotFileInfo, Long>> group : groups) {
      for (Pair<SnapshotFileInfo, Long> file : group) {
        totalSize += file.getSecond();
        totalFiles++;
      }
    }

    assertEquals("All files should be included", 10, totalFiles);
    assertEquals("Total size should be preserved", 450, totalSize);
  }

  @Test
  public void testGroupFilesForSplitsWithCustomFileGrouper() {
    List<Pair<SnapshotFileInfo, Long>> files = new ArrayList<>();
    for (long i = 0; i < 8; i++) {
      SnapshotFileInfo fileInfo = SnapshotFileInfo.newBuilder().setType(SnapshotFileInfo.Type.HFILE)
        .setHfile("file-" + i).build();
      files.add(new Pair<>(fileInfo, i * 5));
    }

    Configuration conf = new Configuration();
    conf.setInt("snapshot.export.format.splits", 4);
    conf.setClass("snapshot.export.input.file.grouper.class", TestCustomFileGrouper.class,
      ExportSnapshot.CustomFileGrouper.class);

    ExportSnapshot.ExportSnapshotInputFormat inputFormat =
      new ExportSnapshot.ExportSnapshotInputFormat();
    Collection<List<Pair<SnapshotFileInfo, Long>>> groups =
      inputFormat.groupFilesForSplits(conf, files);

    assertEquals("Should create splits based on custom grouper output", 4, groups.size());

    long totalSize = 0;
    int totalFiles = 0;
    for (List<Pair<SnapshotFileInfo, Long>> group : groups) {
      for (Pair<SnapshotFileInfo, Long> file : group) {
        totalSize += file.getSecond();
        totalFiles++;
      }
    }

    assertEquals("All files should be included", 8, totalFiles);
    assertEquals("Total size should be preserved", 140, totalSize);
  }

  @Test
  public void testFileLocationResolverWithNoopResolver() {
    List<Pair<SnapshotFileInfo, Long>> files = new ArrayList<>();
    for (long i = 0; i < 3; i++) {
      SnapshotFileInfo fileInfo = SnapshotFileInfo.newBuilder().setType(SnapshotFileInfo.Type.HFILE)
        .setHfile("file-" + i).build();
      files.add(new Pair<>(fileInfo, i * 10));
    }

    ExportSnapshot.NoopFileLocationResolver resolver =
      new ExportSnapshot.NoopFileLocationResolver();
    Set<String> locations = resolver.getLocationsForInputFiles(files);

    assertTrue("NoopFileLocationResolver should return empty locations", locations.isEmpty());
  }

  @Test
  public void testFileLocationResolverWithCustomResolver() {
    List<Pair<SnapshotFileInfo, Long>> files = new ArrayList<>();
    for (long i = 0; i < 3; i++) {
      SnapshotFileInfo fileInfo = SnapshotFileInfo.newBuilder().setType(SnapshotFileInfo.Type.HFILE)
        .setHfile("file-" + i).build();
      files.add(new Pair<>(fileInfo, i * 10));
    }

    TestFileLocationResolver resolver = new TestFileLocationResolver();
    Set<String> locations = resolver.getLocationsForInputFiles(files);

    assertEquals("Should return expected locations", 2, locations.size());
    assertTrue("Should contain rack1", locations.contains("rack1"));
    assertTrue("Should contain rack2", locations.contains("rack2"));
  }

  @Test
  public void testInputSplitWithFileLocationResolver() {
    List<Pair<SnapshotFileInfo, Long>> files = new ArrayList<>();
    for (long i = 0; i < 3; i++) {
      SnapshotFileInfo fileInfo = SnapshotFileInfo.newBuilder().setType(SnapshotFileInfo.Type.HFILE)
        .setHfile("file-" + i).build();
      files.add(new Pair<>(fileInfo, i * 10));
    }

    TestFileLocationResolver resolver = new TestFileLocationResolver();
    ExportSnapshot.ExportSnapshotInputFormat.ExportSnapshotInputSplit split =
      new ExportSnapshot.ExportSnapshotInputFormat.ExportSnapshotInputSplit(files, resolver);

    try {
      String[] locations = split.getLocations();
      assertEquals("Should return 2 locations", 2, locations.length);

      boolean hasRack1 = false;
      boolean hasRack2 = false;
      for (String location : locations) {
        if ("rack1".equals(location)) {
          hasRack1 = true;
        }
        if ("rack2".equals(location)) {
          hasRack2 = true;
        }
      }

      assertTrue("Should contain rack1", hasRack1);
      assertTrue("Should contain rack2", hasRack2);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get locations", e);
    }
  }

  public static class TestCustomFileGrouper implements ExportSnapshot.CustomFileGrouper {
    @Override
    public Collection<Collection<Pair<SnapshotFileInfo, Long>>>
      getGroupedInputFiles(Collection<Pair<SnapshotFileInfo, Long>> snapshotFiles) {
      List<Collection<Pair<SnapshotFileInfo, Long>>> groups = new ArrayList<>();
      List<Pair<SnapshotFileInfo, Long>> group1 = new ArrayList<>();
      List<Pair<SnapshotFileInfo, Long>> group2 = new ArrayList<>();

      int count = 0;
      for (Pair<SnapshotFileInfo, Long> file : snapshotFiles) {
        if (count % 2 == 0) {
          group1.add(file);
        } else {
          group2.add(file);
        }
        count++;
      }

      groups.add(group1);
      groups.add(group2);
      return groups;
    }
  }

  public static class TestFileLocationResolver implements ExportSnapshot.FileLocationResolver {
    @Override
    public Set<String> getLocationsForInputFiles(Collection<Pair<SnapshotFileInfo, Long>> files) {
      Set<String> locations = new HashSet<>();
      locations.add("rack1");
      locations.add("rack2");
      return locations;
    }
  }
}
