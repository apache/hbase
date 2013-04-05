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

package org.apache.hadoop.hbase.regionserver.compactions;

import com.google.common.base.Objects;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(SmallTests.class)
@RunWith(Parameterized.class)
public class PerfTestCompactionPolicies {

  static final Log LOG = LogFactory.getLog(PerfTestCompactionPolicies.class);

  private final RatioBasedCompactionPolicy cp;
  private final int max;
  private final int min;
  private final float ratio;
  private long written = 0;
  private long fileDiff = 0;
  private Random random;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {RatioBasedCompactionPolicy.class, 3, 2, 1.2f},
        {ExploringCompactionPolicy.class, 3, 2, 1.2f},
        {RatioBasedCompactionPolicy.class, 4, 2, 1.2f},
        {ExploringCompactionPolicy.class, 4, 2, 1.2f},
        {RatioBasedCompactionPolicy.class, 5, 2, 1.2f},
        {ExploringCompactionPolicy.class, 5, 2, 1.2f},
        {RatioBasedCompactionPolicy.class, 4, 2, 1.3f},
        {ExploringCompactionPolicy.class, 4, 2, 1.3f},
        {RatioBasedCompactionPolicy.class, 4, 2, 1.4f},
        {ExploringCompactionPolicy.class, 4, 2, 1.4f},

    });
  }

  /**
   * Test the perf of a CompactionPolicy with settings
   * @param cp The compaction policy to test
   * @param max The maximum number of file to compact
   * @param min The min number of files to compact
   * @param ratio The ratio that files must be under to be compacted.
   */
  public PerfTestCompactionPolicies(Class<? extends CompactionPolicy> cpClass,
      int max, int min, float ratio) {
    this.max = max;
    this.min = min;
    this.ratio = ratio;

    //Hide lots of logging so the sysout is usable as a tab delimited file.
    org.apache.log4j.Logger.getLogger(CompactionConfiguration.class).
        setLevel(org.apache.log4j.Level.ERROR);

    org.apache.log4j.Logger.getLogger(cpClass).setLevel(org.apache.log4j.Level.ERROR);

    Configuration configuration = HBaseConfiguration.create();

    //Make sure that this doesn't include every file.
    configuration.setInt("hbase.hstore.compaction.max", max);
    configuration.setInt("hbase.hstore.compaction.min", min);
    configuration.setFloat("hbase.hstore.compaction.ratio", ratio);

    HStore store = createMockStore();
    this.cp = ReflectionUtils.instantiateWithCustomCtor(cpClass.getName(),
        new Class[] { Configuration.class, StoreConfigInformation.class },
        new Object[] { configuration, store });

    //Used for making paths
    random = new Random(42);
  }

  @Test
  public void testSelection() throws Exception {
    //Some special cases. To simulate bulk loading patterns.
    int[][] fileSizes = new int[][]{
        {1000, 350, 200, 100, 20, 10, 10},
        {1000, 450, 200, 100, 20, 10, 10},
        {1000, 550, 200, 100, 20, 10, 10},
        {1000, 650, 200, 100, 20, 10, 10},
        {1000, 250, 25, 25, 25, 25, 25, 25},
        {25, 25, 25, 25, 25, 25, 500},
        {1000, 1000, 1000, 1000, 900},
        {107, 50, 10, 10, 10, 10},
        {2000, 107, 50, 10, 10, 10, 10},
        {9, 8, 7, 6, 5, 4, 3, 2, 1},
        {11, 18, 9, 8, 7, 6, 5, 4, 3, 2, 1},
        {110, 18, 18, 18, 18, 9, 8, 7, 6, 5, 4, 3, 2, 1},
        {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15}
    };

    for (int[] fs : fileSizes) {
      List<StoreFile> storeFiles = createStoreFileList(fs);
      storeFiles = runIteration(storeFiles);
      runIteration(storeFiles);
    }

    for (int i = 0; i < 100; i++) {
      List<StoreFile> storeFiles = new LinkedList<StoreFile>();

      //Add some files to start with so that things are more normal
      storeFiles.add(createMockStoreFile(random.nextInt(1700) + 500));
      storeFiles.add(createMockStoreFile(random.nextInt(700) + 400));
      storeFiles.add(createMockStoreFile(random.nextInt(400) + 300));
      storeFiles.add(createMockStoreFile(random.nextInt(400) + 200));

      for (int x = 0; x < 50; x++) {
        storeFiles.add(createMockStoreFile(random.nextInt(90) + 10));
        storeFiles.add(createMockStoreFile(random.nextInt(90) + 10));
        storeFiles.add(createMockStoreFile(random.nextInt(90) + 10));
        storeFiles.add(createMockStoreFile(random.nextInt(90) + 10));
        storeFiles.add(createMockStoreFile(random.nextInt(90) + 10));
        storeFiles.add(createMockStoreFile(random.nextInt(90) + 10));
        storeFiles = runIteration(storeFiles);
        storeFiles = runIteration(storeFiles);
      }
    }

    //print out tab delimited so that it can be used in excel/gdocs.
    System.out.println(
                     cp.getClass().getSimpleName()
            + "\t" + max
            + "\t" + min
            + "\t" + ratio
            + "\t" + written
            + "\t" + fileDiff
    );
  }


  private List<StoreFile> runIteration(List<StoreFile> startingStoreFiles) throws IOException {

    List<StoreFile> storeFiles = new ArrayList<StoreFile>(startingStoreFiles);
    CompactionRequest req = cp.selectCompaction(
        storeFiles, new ArrayList<StoreFile>(), false, false, false);
    int newFileSize = 0;

    Collection<StoreFile> filesToCompact = req.getFiles();

    if (!filesToCompact.isEmpty()) {

      storeFiles = new ArrayList<StoreFile>(storeFiles);
      storeFiles.removeAll(filesToCompact);

      for (StoreFile storeFile : filesToCompact) {
        newFileSize += storeFile.getReader().length();
      }

      storeFiles.add(createMockStoreFile(newFileSize));
    }

    written += newFileSize;
    fileDiff += storeFiles.size() - startingStoreFiles.size();
    return storeFiles;
  }

  private List<StoreFile> createStoreFileList(int[] fs) {
    List<StoreFile> storeFiles = new LinkedList<StoreFile>();
    for (int fileSize : fs) {
      storeFiles.add(createMockStoreFile(fileSize));
    }
    return storeFiles;
  }

  private StoreFile createMockStoreFile(int sizeMb) {
    return createMockStoreFile(sizeMb, -1l);
  }


  private StoreFile createMockStoreFile(int sizeMb, long seqId) {
    StoreFile mockSf = mock(StoreFile.class);
    StoreFile.Reader reader = mock(StoreFile.Reader.class);
    String stringPath = "/hbase/" + RandomStringUtils.random(10, 0, 0, true, true, null, random);
    Path path = new Path(stringPath);

    when(reader.getSequenceID()).thenReturn(seqId);
    when(reader.getTotalUncompressedBytes()).thenReturn(Long.valueOf(sizeMb));
    when(reader.length()).thenReturn(Long.valueOf(sizeMb));

    when(mockSf.getPath()).thenReturn(path);
    when(mockSf.excludeFromMinorCompaction()).thenReturn(false);
    when(mockSf.isReference()).thenReturn(false); // TODO come back to
                                                  // this when selection takes this into account
    when(mockSf.getReader()).thenReturn(reader);
    String toString = Objects.toStringHelper("MockStoreFile")
                              .add("isReference", false)
                              .add("fileSize", sizeMb)
                              .add("seqId", seqId)
                              .add("path", stringPath).toString();
    when(mockSf.toString()).thenReturn(toString);

    return mockSf;
  }

  private HStore createMockStore() {
    HStore s = mock(HStore.class);
    when(s.getStoreFileTtl()).thenReturn(Long.MAX_VALUE);
    return s;
  }

}
