/**
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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.*;

import org.junit.experimental.categories.Category;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SmallTests;

@Category(SmallTests.class)
public class TestTierCompactSelection extends TestDefaultCompactSelection {
  private final static Log LOG = LogFactory.getLog(TestTierCompactSelection.class);

  private static final int numTiers = 4;

  private String strPrefix, strSchema, strTier;


  @Override
  public void setUp() throws Exception {

    super.setUp();

    // setup config values necessary for store
    strPrefix = "hbase.hstore.compaction.";
    strSchema = "tbl." + store.getHRegion().getTableDesc().getNameAsString()
                           + "cf." + store.getFamily().getNameAsString() + ".";

    this.conf.setStrings(strPrefix + "CompactionPolicy", "TierBasedCompactionPolicy");

    this.conf.setLong(HConstants.MAJOR_COMPACTION_PERIOD, 0);

    // The following parameters are for default compaction
    // Some of them are used as default values of tier based compaction
    this.conf.setInt(strPrefix + "min", 2);
    this.conf.setInt(strPrefix + "max", 10);
    this.conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 0);
    this.conf.setLong(strPrefix +  "max.size", 10000);
    this.conf.setFloat(strPrefix + "ratio", 10.0F);

    // Specifying the family parameters here
    conf.setInt(strPrefix + strSchema + "NumCompactionTiers", numTiers);
    conf.setLong(strPrefix + strSchema + "MinCompactSize", minSize);
    conf.setLong(strPrefix + strSchema + "MaxCompactSize", maxSize);

    // Specifying parameters for the default tier
    strTier = "";
    conf.setFloat(strPrefix + strSchema + strTier + "CompactionRatio", 0.1F);
    conf.setInt(strPrefix + strSchema + strTier + "MinFilesToCompact", minFiles);
    conf.setInt(strPrefix + strSchema + strTier + "MaxFilesToCompact", maxFiles);

    // Specifying parameters for individual tiers here

    // Don't compact in this tier (likely to be in block cache)
    strTier = "Tier.0.";
    conf.setFloat(strPrefix + strSchema + strTier + "CompactionRatio", 0.0F);

    // Most aggressive tier
    strTier = "Tier.1.";
    conf.setFloat(strPrefix + strSchema + strTier + "CompactionRatio", 2.0F);
    conf.setInt(strPrefix + strSchema + strTier + "MinFilesToCompact", 2);
    conf.setInt(strPrefix + strSchema + strTier + "MaxFilesToCompact", 10);

    // Medium tier
    strTier = "Tier.2.";
    conf.setFloat(strPrefix + strSchema + strTier + "CompactionRatio", 1.0F);
    // Also include files in tier 1 here
    conf.setInt(strPrefix + strSchema + strTier + "EndingIndexForTier", 1);

    // Last tier - least aggressive compaction
    // has default tier settings only
    // Max Time elapsed is Infinity by default

  }

  @Override
  void compactEquals(
    List<StoreFile> candidates, boolean forcemajor,
    long... expected
  )
    throws IOException {
    store.forceMajor = forcemajor;
    //update the policy for now in case any change
    store.setCompactionPolicy(TierCompactionManager.class.getName());
    List<StoreFile> actual =
      store.compactionManager.selectCompaction(candidates, Store.NO_PRIORITY, forcemajor).getFilesToCompact();
    store.forceMajor = false;
    assertEquals(Arrays.toString(expected), Arrays.toString(getSizes(actual)));
  }

  public void testAgeBasedAssignment() throws IOException {

    conf.setLong(strPrefix + strSchema + "Tier.0.MaxAgeInDisk", 10L);
    conf.setLong(strPrefix + strSchema + "Tier.1.MaxAgeInDisk", 100L);
    conf.setLong(strPrefix + strSchema + "Tier.2.MaxAgeInDisk", 1000L);
    conf.setLong(strPrefix + strSchema + "Tier.0.MaxSize", Long.MAX_VALUE);
    conf.setLong(strPrefix + strSchema + "Tier.1.MaxSize", Long.MAX_VALUE);
    conf.setLong(strPrefix + strSchema + "Tier.2.MaxSize", Long.MAX_VALUE);

    //everything in first tier, don't compact!
    compactEquals(sfCreate(toArrayList(
      151, 30, 13, 12, 11   ), toArrayList( // Sizes
        8,  5,  4,  2,  1   ))              // ageInDisk ( = currentTime - minFlushTime)
      /* empty expected */  );              // Selected sizes

    //below minSize should compact
    compactEquals(sfCreate(toArrayList(
      12, 11, 8, 3, 1   ), toArrayList(
       8,  5, 4, 2, 1   )),
             8, 3, 1   );

    //everything in second tier
    compactEquals(sfCreate(toArrayList(
      251, 70, 13, 12, 11   ), toArrayList(
       80, 50, 40, 20, 11   )),
           70, 13, 12, 11   );

    //everything in third tier
    compactEquals(sfCreate(toArrayList(
      251,  70,  13,  12,  11   ), toArrayList(
      800, 500, 400, 200, 110   )),
                 13,  12,  11   );

    //everything in fourth tier
    compactEquals(sfCreate(toArrayList(
      251,   70,   13,   12,   11   ), toArrayList(
     8000, 5000, 4000, 2000, 1100   ))
     /* empty expected */           );

    //Valid compaction in 4th tier with ratio 0.10, hits maxFilesToCompact
    compactEquals(sfCreate(toArrayList(
      500, 93, 92, 91, 90, 89, 88, 87, 86, 85, 84, 83, 82, 81, 80   ), toArrayList(
      5094, 5093, 5092, 5091, 5090, 5089, 5088, 5087, 5086, 5085, 5084, 5083, 5082, 5081, 5080)),
           93, 92, 91, 90, 89                                       );

    //Now mixing tiers 1,0, expected selection in tier 1 only
    compactEquals(sfCreate(toArrayList(
      999, 110, 100, 12, 11   ), toArrayList(
       90,  80,  50, 4,  1    )),
           110, 100           );

    //Mixing tier 2,1, expected selection in tier 2 including tier 1 but not zero
    compactEquals(sfCreate(toArrayList(
      999, 110, 100, 12, 11   ), toArrayList(
      900, 800, 500, 40,  1   )),
           110, 100, 12       );

    //Mixing tier 2,1, expected selection in tier 1 because of recentFirstOrder = true
    compactEquals(sfCreate(toArrayList(
      999, 110, 100, 12, 13, 11   ), toArrayList(
      900, 800, 500, 40, 30,  1   )),
                     12, 13       );

    conf.setBoolean(strPrefix + strSchema + "IsRecentFirstOrder", false);

    //Mixing tier 2,1, expected selection in tier 1 because of recentFirstOrder = false
    compactEquals(sfCreate(toArrayList(
      999, 110, 100, 12, 13, 11   ), toArrayList(
      900, 800, 500, 40, 30,  1   )),
           110, 100, 12, 13       );

    //Mixing all tier 3,2,1,0 expected selection in tier 1 only
    compactEquals(sfCreate(toArrayList(
      999, 800, 110, 100, 12, 13, 11    ), toArrayList(
     9000, 800,  50,  40,  8,  3,  1    )),
                110, 100                );

    //Checking backward compatibility, first 3 files don't have minFlushTime,
    //all should go to tier 1, not tier 0
    compactEquals(sfCreate(toArrayList(
      999, 800, 110, 100, 12, 13, 11    ), toArrayList(
        0,   0,   0,  40,  8,  3,  1    )),
      999, 800, 110, 100                );

    //make sure too big files don't get compacted
    compactEquals(sfCreate(toArrayList(
      1002, 1001, 999, 800, 700, 12, 13, 11   ), toArrayList(
       900,   80,  50,  40,  30, 20,  4,  2   )),
                  999, 800, 700, 12           );

  }

  public void testSizeBasedAssignment() throws IOException {

    conf.setLong(strPrefix + strSchema + "MinCompactSize", 3);

    conf.setLong(strPrefix + strSchema + "Tier.0.MaxSize", 10L);
    conf.setLong(strPrefix + strSchema + "Tier.1.MaxSize", 100L);
    conf.setLong(strPrefix + strSchema + "Tier.2.MaxSize", 1000L);
    conf.setLong(strPrefix + strSchema + "Tier.0.MaxAgeInDisk", Long.MAX_VALUE);
    conf.setLong(strPrefix + strSchema + "Tier.1.MaxAgeInDisk", Long.MAX_VALUE);
    conf.setLong(strPrefix + strSchema + "Tier.2.MaxAgeInDisk", Long.MAX_VALUE);

    compactEquals(sfCreate(false,
      500, 3, 2, 1     ),
           3, 2, 1     );

    compactEquals(sfCreate(false,
      500, 8, 7, 6, 5, 4, 2, 1     )
      /* empty */                  );

    compactEquals(sfCreate(false,
      500, 6, 8, 4, 7, 4, 2, 1     )
      /* empty */                  );

    compactEquals(sfCreate(false,
      500, 23, 11, 8, 4, 1     )
      /* empty */              );

    compactEquals(sfCreate(false,
      500, 11, 23, 8, 4, 1     ),
           11, 23              );

    compactEquals(sfCreate(false,
      500, 9, 23, 8, 4, 1     ),
           9, 23              );

    compactEquals(sfCreate(false,
      500, 70, 23, 11, 8, 4, 1     )
      /* empty */                  );

    compactEquals(sfCreate(false,
      500, 60, 23, 11, 8, 4, 1     ),
           60, 23, 11              );

    compactEquals(sfCreate(false,
      500, 90, 60, 23, 11, 8, 4, 1     ),
           90, 60, 23, 11              );

    conf.setBoolean(strPrefix + strSchema + "IsRecentFirstOrder", false);

    compactEquals(sfCreate(false,
      500, 450, 60, 23, 11, 8, 4, 1     ),
      500, 450, 60, 23, 11              );

    compactEquals(sfCreate(false,
      450, 500, 60, 23, 11, 8, 4, 1     ),
      450, 500, 60, 23, 11              );

    compactEquals(sfCreate(false,
      1013, 1012, 1011, 1010, 1009, 1008, 1007, 1006, 1005, 1004, 1003, 1002, 1001, 999, 450, 550 ),
                                                                                    999, 450, 550 );

    conf.setLong(strPrefix + strSchema + "MaxCompactSize", 10000);

    compactEquals(sfCreate(false,
      1013, 1012, 1011, 1010, 1009, 1008, 1007, 1006, 1005, 1004, 1003, 1002, 1001, 999, 450, 550 ),
      1013, 1012, 1011, 1010, 1009                                                                );

    compactEquals(sfCreate(false,
      1013,  992, 1011, 1010, 1009,  1008, 1007, 1006, 1005, 1004, 1003, 1002, 1001, 999, 450, 550),
      1013,  992, 1011, 1010, 1009                                                                );

    compactEquals(sfCreate(false,
      992,   993, 1011, 990, 1009,  998, 1007, 996, 1005, 994, 1003, 992, 1001, 999, 450, 550     ),
      992,   993, 1011, 990, 1009                                                                 );

    conf.setBoolean(strPrefix + strSchema + "IsRecentFirstOrder", true);

    compactEquals(sfCreate(false,
      500, 450, 60, 23, 11, 8, 4, 1     ),
                60, 23, 11              );

    compactEquals(sfCreate(false,
      450, 500, 60, 23, 11, 8, 4, 1     ),
                60, 23, 11              );

    compactEquals(sfCreate(false,
      1013, 1012, 1011, 1010, 1009, 1008, 1007, 1006, 1005, 1004, 1003, 1002, 1001, 999, 450, 550 ),
                                                                                    999, 450, 550 );

    compactEquals(sfCreate(false,
      992,   993, 1011, 990, 1009,  998, 1007, 996, 1005, 994, 1003, 992, 1001, 999, 450, 550     ),
                                                                                999, 450, 550     );

    compactEquals(sfCreate(false,
      992,   993, 1011, 990, 1009,  998, 1007, 996, 1005, 994, 1003, 992, 991, 999, 450, 550      ),
                                                                     992, 991, 999, 450, 550      );

    compactEquals(sfCreate(false,
      992,   993, 1011, 990, 1009,  998, 1007, 996, 1005, 994, 1003, 992, 991, 999, 450, 550, 1001),
      992,   993, 1011, 990, 1009                                                                 );

  }

  @Override
  public void testCompactionRatio() throws IOException {
    conf.setInt(strPrefix + strSchema + "NumCompactionTiers", 1);
    conf.setFloat(strPrefix + strSchema + "Tier.0.CompactionRatio", 1.0F);
    conf.setInt(strPrefix + "max", 5);
    super.testCompactionRatio();
  }

  @Override
  public void testOffPeakCompactionRatio() throws IOException {}

}
