/**
 * Copyright 2012 The Apache Software Foundation
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
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestTierCompactSelection extends TestDefaultCompactSelection {
  private final static Log LOG = LogFactory.getLog(TestTierCompactSelection.class);

  private static final int numTiers = 5;

  private String strPrefix, strSchema, strTier;
  private Calendar currCal;
  private Calendar[] expectedCals;

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
    conf.setBoolean(strPrefix + strSchema + "IsTierBoundaryFixed", false);

    // Last tier - least aggressive compaction
    // has default tier settings only
    // Max Time elapsed is Infinity by default
    currCal = Calendar.getInstance();
    expectedCals = new Calendar[numTiers + 1];
    for (int i = 0; i < numTiers + 1; i++) {
      expectedCals[i] = Calendar.getInstance();
    }
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
      store.compactionManager.selectCompaction(candidates, forcemajor).getFilesToCompact();
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

  /**
   * 
   * @throws IOException
   */
  public void testTierCompactionBoundary() throws IOException {
    conf.setBoolean(strPrefix + strSchema + "IsTierBoundaryFixed", true);
    String strTierPrefix = strPrefix + strSchema;
    conf.setInt(strPrefix + strSchema + "NumCompactionTiers", 3);

    // -------------------------------------
    // two Tiers
    // -------------------------------------
    // NOW: 2012 Nov 10, 12:11
    // wild card
    // -------------------------------------
    this.conf.setStrings(strTierPrefix + "Tier.0.Boundary", "1 0 * * *");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11}, // pseudo now
                   new int[] {2012, 11, 10,  0, 1});  // expected tier boundary

    // -------------------------------------
    // ?/2 at DAY_OF_MONTH
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 0 ?/2 * *");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 11, 8, 0, 0});
    // -------------------------------------
    // different step size at DAY_OF_MONTH
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 0 ?/4 * *");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 11,  6,  0,  0});

    // -------------------------------------
    // */4 at DAY_OF_MONTH
    // [1, 5, 9... ]
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 0 */4 * *");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 11,  9,  0,  0});

    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 0 */4 * *");
    boundaryEquals(new int[] { 2012, 11, 8, 12, 11 },
                   new int[] {2012, 11, 5, 0, 0});

    // -------------------------------------
    // an explicit value at DAY_OF_MONTH
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 0 7 * *");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 11, 7, 0, 0});

    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 0 7 8 * 2011");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11}, 
                   new int[] { 2011, 8, 7, 0, 0 });

    // -------------------------------------
    // comma at DAY_OF_MONTH
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 0 6,7 * *");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 11, 7, 0, 0});
    
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 0 6,7 * *");
    boundaryEquals(new int[] {2012, 11, 7, 0, 1},
                   new int[] {2012, 11, 7, 0, 0});

    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 0 6,7 * *");
    boundaryEquals(new int[] { 2012, 11, 7, 0, 0 },
                   new int[] {2012, 11, 6, 0, 0});

    // -------------------------------------
    // hyphen at DAY_OF_MONTH
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 0 6-7 * *");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 11, 7, 0, 0});

    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 0 6-7 * *");
    boundaryEquals(new int[] { 2012, 11, 7, 0, 0 },
                   new int[] {2012, 11, 6, 0, 0});

    // -------------------------------------
    // an explicit value at DAY_OF_MONTH
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 0 1L * *");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 10, 31, 0, 0});

    // -------------------------------------
    // an explicit value at DAY_OF_MONTH
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 0 1L ?/2 *");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 9, 30, 0, 0});

    // -------------------------------------
    // an explicit value at DAY_OF_MONTH
    // [1, 6, 11]
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 0 1L */5 *");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 6, 30, 0, 0});

    // -------------------------------------
    // comma at DAY_OF_WEEK 
    // -------------------------------------
    this.conf.setStrings(strTierPrefix + "Tier.0.Boundary", "1 0 * * 1,3,5");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 11, 9, 0, 1});

    this.conf.setStrings(strTierPrefix + "Tier.0.Boundary", "1 0 * * 1,3,5");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 11, 9, 0, 1});

    this.conf.setStrings(strTierPrefix + "Tier.0.Boundary", "1 0 * * 1,3,5");
    boundaryEquals(new int[] {2012, 11, 9, 0, 1},
                   new int[] {2012, 11, 7, 0, 1});

    // -------------------------------------
    // hyphen at DAY_OF_WEEK 
    // -------------------------------------
    this.conf.setStrings(strTierPrefix + "Tier.0.Boundary", "1 0 * * 1-5");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 11, 9, 0, 1});

    this.conf.setStrings(strTierPrefix + "Tier.0.Boundary", "1 0 * * 1-4");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 11, 8, 0, 1});

    // -------------------------------------
    // DAY_OF_WEEK with "#"
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 12 * * 5#1");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 11, 2, 12, 0});

    // -------------------------------------
    // week_of_day with "#" with roll over in month
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 12 * * 5#1");
    boundaryEquals(new int[] {2012, 11, 2, 12, 0},
                   new int[] {2012, 10, 5, 12, 0});

    // -------------------------------------
    // week_of_day with "#" with explicit month
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 12 * 8 5#1");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 8, 3, 12, 0});
    
    // -------------------------------------
    // week_of_day with "#" with ?/2 month
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 12 * ?/2 5#1");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 9, 7, 12, 0});
    
    // -------------------------------------
    // week_of_day with "#" with */ month
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 12 * */2 5#1");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 11, 2, 12, 0});
    
    // -------------------------------------
    // week_of_day with "L"
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 12 * * 5L");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 10, 26, 12, 0});
    
    // -------------------------------------
    // week_of_day with "L" and roll over in month
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 12 * * 5L");
    boundaryEquals(new int[] { 2012, 10, 22, 12, 11 },
                   new int[] {2012, 9, 28, 12, 0});
    
    // -------------------------------------
    // week_of_day with "L" in a given month
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 12 * 8 5L");
    boundaryEquals(new int[] { 2012, 10, 22, 12, 11 },
                   new int[] {2012, 8, 31, 12, 0});
    
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 12 * 8 5L 2011");
    boundaryEquals(new int[] { 2012, 10, 22, 12, 11 },
                   new int[] {2011, 8, 26, 12, 0});
    
    // -------------------------------------
    // week_of_day with "L" for dynamic month
    // [1, 3, 5, ...]
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 12 * */2 5L");
    boundaryEquals(new int[] { 2012, 10, 22, 12, 11 },
                   new int[] {2012, 9, 28, 12, 0});

    // -------------------------------------
    // week_of_day with "L" with ?
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 12 * ?/2 5L");
    boundaryEquals(new int[] { 2012, 10, 22, 12, 11 },
                   new int[] {2012, 8, 31, 12, 0});

    // -------------------------------------
    // week_of_day with "L" with ? and different stepsize
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 12 * ?/5 5L");
    boundaryEquals(new int[] { 2012, 10, 22, 12, 11 },
                   new int[] {2012, 5, 25, 12, 0});

    // -------------------------------------
    // week_of_day with "L" with ? and different stepsize
    // [1, 6, 11]
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 12 * */5 5L");
    boundaryEquals(new int[] { 2012, 10, 22, 12, 11 },
                   new int[] {2012, 6, 29, 12, 0});

    // -------------------------------------
    // Three Tiers
    // -------------------------------------
    // test wild card
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 0 * * *");
    conf.setStrings(strTierPrefix + "Tier.1.Boundary", "0 0 * * *");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 11, 10, 0, 0},
                   new int[] {2012, 11, 9, 0, 0});

    // -------------------------------------
    // test combination of wild card & ?/2
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 0 * * *");
    conf.setStrings(strTierPrefix + "Tier.1.Boundary", "0 0 ?/2 * *");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 11, 10, 0, 0}, 
                   new int[] {2012, 11, 8, 0, 0});

    // -------------------------------------
    // test stepsize
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 0 * * *");
    conf.setStrings(strTierPrefix + "Tier.1.Boundary", "0 0 ?/3 * *");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 11, 10, 0, 0}, 
                   new int[] {2012, 11, 7, 0, 0});

    // -------------------------------------
    // test combination of wildcard and "L"
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 0 * * *");
    conf.setStrings(strTierPrefix + "Tier.1.Boundary", "59 23 1L * *");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 11, 10, 0, 0}, 
                   new int[] {2012, 10, 31, 23, 59});

    // -------------------------------------
    // test with a given year/month/day
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 4 1 1 * 2012");
    conf.setStrings(strTierPrefix + "Tier.1.Boundary", "0 4 1 1 * 2011");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 1, 1, 4, 0}, 
                   new int[] {2011, 1, 1, 4, 0});

    // -------------------------------------
    // five Tiers
    // -------------------------------------
    // T0 :0 0 * * *
    // T1: 0 0 * * *
    // T2: 0 0 * * 0
    // T3: 0 0 1 * *
    // 
    // NOW: 2012 Nov 10, 12:11
    // T0 : Nov 10 -- today's midnight
    // T1 : Nov 09 -- yesterday's midnight
    // T2 : Nov 04 -- last sunday's midnight
    // T3 : Nov 01 -- 1st day of the month's midnight
    // -------------------------------------
    conf.setStrings(strTierPrefix + "Tier.0.Boundary", "0 0 * * *");
    conf.setStrings(strTierPrefix + "Tier.1.Boundary", "0 0 * * *");
    conf.setStrings(strTierPrefix + "Tier.2.Boundary", "0 0 * * 0");
    conf.setStrings(strTierPrefix + "Tier.3.Boundary", "0 0 1 * *");
    boundaryEquals(new int[] {2012, 11, 10, 12, 11},
                   new int[] {2012, 11, 10, 0, 0}, 
                   new int[] {2012, 11, 9, 0, 0},
                   new int[] {2012, 11, 4, 0, 0},
                   new int[] {2012, 11, 1, 0, 0});

  }

  /**
   * 
   * @throws IOException
   */
  public void testCompactionBoundarDefault() throws IOException {
    conf.setBoolean(strPrefix + strSchema + "IsTierBoundaryFixed", true);
    Calendar[] expectedCals = new Calendar[numTiers];
    for (int i = 0; i < numTiers; i++) {
      expectedCals[i] = Calendar.getInstance();
    }
    // every minute
    testCronExpression("* * * * *", 
        new int[] {2012, 11, 10, 12, 11}, // the pseudo now
        new int[] {2012, 11, 10, 12, 10}, 
        new int[] {2012, 11, 10, 12, 9},
        new int[] {2012, 11, 10, 12, 8});
                  
    // every minute cross hour
    testCronExpression("* * * * *", 
        new int[] {2012, 11, 10, 12, 2},
        new int[] {2012, 11, 10, 12, 1}, 
        new int[] {2012, 11, 10, 12, 0},
        new int[] {2012, 11, 10, 11, 59});
    
    // every minute cross day
    testCronExpression("* * * * *", 
        new int[] {2012, 11, 10, 0, 2},
        new int[] {2012, 11, 10, 0, 1}, 
        new int[] {2012, 11, 10, 0, 0},
        new int[] {2012, 11, 9, 23, 59});

    // every minute cross month
    testCronExpression("* * * * *", 
        new int[] {2012, 11, 1, 0, 2},
        new int[] {2012, 11, 1, 0, 1}, 
        new int[] {2012, 11, 1, 0, 0},
        new int[] {2012, 10, 31, 23, 59});

    // every minute cross year
    testCronExpression("* * * * *", 
        new int[] {2012, 1, 1, 0, 2},
        new int[] {2012, 1, 1, 0, 1}, 
        new int[] {2012, 1, 1, 0, 0},
        new int[] {2011, 12, 31, 23, 59});

    // every hour 1/2 hr boundary
    testCronExpression("30 * * * *", 
        new int[] {2012, 11, 10, 5, 30},
        new int[] {2012, 11, 10, 4, 30}, 
        new int[] {2012, 11, 10, 3, 30},
        new int[] {2012, 11, 10, 2, 30});

    // every hour cross day
    testCronExpression("1 * * * *", 
        new int[] {2012, 11, 0, 1, 11},
        new int[] {2012, 11, 0, 1, 1}, 
        new int[] {2012, 11, 0, 0, 1},
        new int[] {2012, 10, 30, 23, 1});

    // every day
    testCronExpression("1 2 * * *", 
        new int[] {2012, 11, 10, 12, 11},
        new int[] {2012, 11, 10, 2, 1}, 
        new int[] {2012, 11, 9, 2, 1},
        new int[] {2012, 11, 8, 2, 1});

    // every day at 4am
    testCronExpression("0 4 * * *", 
        new int[] {2012, 11, 10, 3, 11},
        new int[] {2012, 11, 9, 4, 0}, 
        new int[] {2012, 11, 8, 4, 0},
        new int[] {2012, 11, 7, 4, 0});

    // every day cross month
    testCronExpression("1 2 * * *", 
        new int[] {2012, 1, 1, 3, 11},
        new int[] {2012, 1, 1, 2, 1}, 
        new int[] {2011, 12, 31, 2, 1},
        new int[] {2011, 12, 30, 2, 1});

    // every month
    testCronExpression("1 2 3 * *", 
        new int[] {2012, 11, 10, 1, 11},
        new int[] {2012, 11, 3, 2, 1}, 
        new int[] {2012, 10, 3, 2, 1},
        new int[] {2012, 9, 3, 2, 1});

    // every month cross year
    testCronExpression("1 2 3 * *", 
        new int[] {2012, 1, 2, 1, 11},
        new int[] {2011, 12, 3, 2, 1}, 
        new int[] {2011, 11, 3, 2, 1},
        new int[] {2011, 10, 3, 2, 1});
    
    // every march cross year
    testCronExpression("1 2 3 3 *", 
        new int[] {2012, 1, 2, 1, 11},
        new int[] {2011, 3, 3, 2, 1}, 
        new int[] {2010, 3, 3, 2, 1},
        new int[] {2009, 3, 3, 2, 1});
    
    // every Thursday per month cross year
    testCronExpression("1 2 * * 4", 
        new int[] {2012, 11, 8, 2, 11},
        new int[] {2012, 11, 8, 2, 1}, 
        new int[] {2012, 11, 1, 2, 1},
        new int[] {2012, 10, 25, 2, 1});
    
    // last day per month 
    testCronExpression("1 2 1L * *",
        new int[] {2012, 11, 8, 2, 11},
        new int[] {2012, 10, 31, 2, 1}, 
        new int[] {2012, 9, 30, 2, 1},
        new int[] {2012, 8, 31, 2, 1});

    // last day bi-monthly
    testCronExpression("1 2 1L ?/2 *", 
        new int[] {2012, 11, 8, 2, 11},
        new int[] {2012, 9, 30, 2, 1}, 
        new int[] {2012, 7, 31, 2, 1},
        new int[] {2012, 5, 31, 2, 1});

    // last Friday bi-monthly
    testCronExpression("1 2 * ?/2 5L",
        new int[] {2012, 11, 8, 2, 11},
        new int[] {2012, 9, 28, 2, 1}, 
        new int[] {2012, 7, 27, 2, 1},
        new int[] {2012, 5, 25, 2, 1});
    
    // last Friday per month
    testCronExpression("1 2 * * 5L",
        new int[] {2012, 11, 8, 2, 11},
        new int[] {2012, 10, 26, 2, 1}, 
        new int[] {2012, 9, 28, 2, 1},
        new int[] {2012, 8, 31, 2, 1});

    // last day of the first month per quarter
    // [1, 4, 7, 10]
    testCronExpression("1 2 1L */3 *",
       new int[] {2012, 11, 8, 2, 11},
       new int[] {2012, 10, 31, 2, 1},
       new int[] {2012, 7, 31, 2, 1},
       new int[] {2012, 4, 30, 2, 1});

    // the very first day of every quarter
    testCronExpression("0 0 1 */3 *",
        new int[] {2012, 11, 10, 2, 11},
        new int[] {2012, 10, 1, 0, 0}, 
        new int[] {2012, 7, 1, 0, 0},
        new int[] {2012, 4, 1, 0, 0});

    // the very first day of every quarter
    // NOW: 11/8/2012, 2:11
    // the first valid month by ?/3 means three months from the current month
    // [2, 5, 8, 11] 
    testCronExpression("0 0 1 ?/3 *", 
        new int[] {2012, 11, 8, 2, 11},
        new int[] {2012, 8, 1, 0, 0}, 
        new int[] {2012, 5, 1, 0, 0},
        new int[] {2012, 2, 1, 0, 0});

    // every mon/wed/fri cross month boundary
    testCronExpression("1 0 * * 1,3,5", 
        new int[] {2012, 12, 4, 2, 11},
        new int[] {2012, 12, 3, 0, 1}, 
        new int[] {2012, 11, 30, 0, 1},
        new int[] {2012, 11, 28, 0, 1});

    // same effect as the previous cron expression
    testCronExpression("1 0 * * 1-5/2", 
        new int[] {2012, 12, 4, 2, 11},
        new int[] {2012, 12, 3, 0, 1}, 
        new int[] {2012, 11, 30, 0, 1},
        new int[] {2012, 11, 28, 0, 1});

  }

  private void testCronExpression(String cronExpression, int[] currVal,
      int[]... expectedVals) throws IOException {
    for (int i = 0; i < expectedVals.length; i++) {
      String strTier = "Tier." + String.valueOf(i) + ".";
      this.conf.setStrings(strPrefix + strSchema + strTier + "Boundary",
          cronExpression);
    }
    String strTier = "Tier." + String.valueOf(expectedVals.length) + ".";
    this.conf.setStrings(strPrefix + strSchema + strTier + "Boundary", "");
    boundaryEquals(currVal, expectedVals);
  }

  /**
   * 
   * @param currVal: the pseudo now from when the tier boundaries 
   *                 start to be calculated
   * @param expectedVals: expected tier boundaries
   */
  private void boundaryEquals(int[] currVal, int[]... expectedVals) {
    int localNumTiers = expectedVals.length + 1;
    conf.setInt(strPrefix + strSchema + "NumCompactionTiers", localNumTiers);
    currCal.set(currVal[0], currVal[1] - 1, currVal[2], currVal[3], currVal[4]);
    String strTier = "Tier." + String.valueOf(localNumTiers - 1) + ".";
    conf.set(strPrefix + strSchema + strTier + "Boundary", "");
    for (int i = 0; i < localNumTiers - 1; i++) {
      expectedCals[i].set(expectedVals[i][0], expectedVals[i][1] - 1,
          expectedVals[i][2], expectedVals[i][3], expectedVals[i][4]);
    }
    expectedCals[localNumTiers - 1].setTimeInMillis(0);

    TierCompactionConfiguration tierConf = new TierCompactionConfiguration(
        conf, store);
    Calendar localCal = (Calendar) currCal.clone();
    TierCompactionConfiguration.CompactionTier tier;
    if (tierConf.isTierBoundaryFixed()) {
      for (int i = 0; i < localNumTiers; i++) {
        tier = tierConf.getCompactionTier(i);
        localCal = tier.getTierBoundary(localCal);
        // we can ignore all the numbers below the minute level
        // since our cron expression starts with the minute
        assertEquals(expectedCals[i].getTimeInMillis() / 60000, 
            localCal.getTimeInMillis() / 60000);
      }
    }
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
