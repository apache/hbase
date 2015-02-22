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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
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
import java.util.Collection;
import java.util.List;

import static org.mockito.Mockito.mock;

import static org.mockito.Mockito.when;

@Category({RegionServerTests.class, MediumTests.class})
@RunWith(Parameterized.class)
public class PerfTestCompactionPolicies extends MockStoreFileGenerator {


  private static final Log LOG = LogFactory.getLog(PerfTestCompactionPolicies.class);

  private final RatioBasedCompactionPolicy cp;
  private final StoreFileListGenerator generator;
  private final HStore store;
  private Class<? extends StoreFileListGenerator> fileGenClass;
  private final int max;
  private final int min;
  private final float ratio;
  private long written = 0;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {



    Class[] policyClasses = new Class[]{
        EverythingPolicy.class,
        RatioBasedCompactionPolicy.class,
        ExploringCompactionPolicy.class,
    };

    Class[] fileListGenClasses = new Class[]{
        ExplicitFileListGenerator.class,
        ConstantSizeFileListGenerator.class,
        SemiConstantSizeFileListGenerator.class,
        GaussianFileListGenerator.class,
        SinusoidalFileListGenerator.class,
        SpikyFileListGenerator.class
     };

    int[] maxFileValues = new int[] {10};
    int[] minFilesValues = new int[] {3};
    float[] ratioValues = new float[] {1.2f};

    List<Object[]> params = new ArrayList<Object[]>(
        maxFileValues.length
        * minFilesValues.length
        * fileListGenClasses.length
        * policyClasses.length);


    for (Class policyClass :  policyClasses) {
      for (Class genClass: fileListGenClasses) {
        for (int maxFile:maxFileValues) {
          for (int minFile:minFilesValues) {
            for (float ratio:ratioValues) {
              params.add(new Object[] {policyClass, genClass, maxFile, minFile, ratio});
            }
          }
        }
      }
    }

    return params;
  }

  /**
   * Test the perf of a CompactionPolicy with settings.
   * @param cpClass The compaction policy to test
   * @param inMmax The maximum number of file to compact
   * @param inMin The min number of files to compact
   * @param inRatio The ratio that files must be under to be compacted.
   */
  public PerfTestCompactionPolicies(
      final Class<? extends CompactionPolicy> cpClass,
      final Class<? extends StoreFileListGenerator> fileGenClass,
      final int inMmax,
      final int inMin,
      final float inRatio) throws IllegalAccessException, InstantiationException {
    super(PerfTestCompactionPolicies.class);
    this.fileGenClass = fileGenClass;
    this.max = inMmax;
    this.min = inMin;
    this.ratio = inRatio;

    // Hide lots of logging so the system out is usable as a tab delimited file.
    org.apache.log4j.Logger.getLogger(CompactionConfiguration.class).
        setLevel(org.apache.log4j.Level.ERROR);
    org.apache.log4j.Logger.getLogger(RatioBasedCompactionPolicy.class).
        setLevel(org.apache.log4j.Level.ERROR);

    org.apache.log4j.Logger.getLogger(cpClass).setLevel(org.apache.log4j.Level.ERROR);


    Configuration configuration = HBaseConfiguration.create();

    // Make sure that this doesn't include every file.
    configuration.setInt("hbase.hstore.compaction.max", max);
    configuration.setInt("hbase.hstore.compaction.min", min);
    configuration.setFloat("hbase.hstore.compaction.ratio", ratio);

    store = createMockStore();
    this.cp = ReflectionUtils.instantiateWithCustomCtor(cpClass.getName(),
        new Class[] {Configuration.class, StoreConfigInformation.class },
        new Object[] {configuration, store });

    this.generator = fileGenClass.newInstance();
    // Used for making paths
  }

  @Test
  public final void testSelection() throws Exception {
    long fileDiff = 0;
    for (List<StoreFile> storeFileList : generator) {
      List<StoreFile> currentFiles = new ArrayList<StoreFile>(18);
      for (StoreFile file : storeFileList) {
        currentFiles.add(file);
        currentFiles = runIteration(currentFiles);
      }
      fileDiff += (storeFileList.size() - currentFiles.size());
    }

    // print out tab delimited so that it can be used in excel/gdocs.
    System.out.println(
            cp.getClass().getSimpleName()
            + "\t" + fileGenClass.getSimpleName()
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
    long newFileSize = 0;

    Collection<StoreFile> filesToCompact = req.getFiles();

    if (!filesToCompact.isEmpty()) {

      storeFiles = new ArrayList<StoreFile>(storeFiles);
      storeFiles.removeAll(filesToCompact);

      for (StoreFile storeFile : filesToCompact) {
        newFileSize += storeFile.getReader().length();
      }

      storeFiles.add(createMockStoreFileBytes(newFileSize));
    }

    written += newFileSize;
    return storeFiles;
  }

  private HStore createMockStore() {
    HStore s = mock(HStore.class);
    when(s.getStoreFileTtl()).thenReturn(Long.MAX_VALUE);
    when(s.getBlockingFileCount()).thenReturn(7L);
    return s;
  }

}
