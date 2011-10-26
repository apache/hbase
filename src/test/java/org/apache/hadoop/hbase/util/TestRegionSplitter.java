/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.RegionSplitter.HexStringSplit;
import org.apache.hadoop.hbase.util.RegionSplitter.UniformSplit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for {@link RegionSplitter}, which can create a pre-split table or do a
 * rolling split of an existing table.
 */
public class TestRegionSplitter {
    private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();
    private final static String HEX_SPLIT_CLASS_NAME =
            "org.apache.hadoop.hbase.util.RegionSplitter$HexStringSplit";
    private final static String UNIFORM_SPLIT_CLASS_NAME =
            "org.apache.hadoop.hbase.util.RegionSplitter$UniformSplit";
    private final static String CF_NAME = "SPLIT_TEST_CF";
    private final static byte xFF = (byte)0xff;

    @BeforeClass
    public static void setup() throws Exception {
        UTIL.startMiniCluster();
    }

    @AfterClass
    public static void teardown() throws Exception {
        UTIL.shutdownMiniCluster();
    }

    /**
     * Test creating a pre-split table using the HexStringSplit algorithm.
     */
    @Test
    public void testCreatePresplitTableHex() throws Exception {
        final List<byte[]> expectedBounds = new ArrayList<byte[]>();
        expectedBounds.add(ArrayUtils.EMPTY_BYTE_ARRAY);
        expectedBounds.add("0fffffff".getBytes());
        expectedBounds.add("1ffffffe".getBytes());
        expectedBounds.add("2ffffffd".getBytes());
        expectedBounds.add("3ffffffc".getBytes());
        expectedBounds.add("4ffffffb".getBytes());
        expectedBounds.add("5ffffffa".getBytes());
        expectedBounds.add("6ffffff9".getBytes());
        expectedBounds.add("7ffffff8".getBytes());
        expectedBounds.add("8ffffff7".getBytes());
        expectedBounds.add("9ffffff6".getBytes());
        expectedBounds.add("affffff5".getBytes());
        expectedBounds.add("bffffff4".getBytes());
        expectedBounds.add("cffffff3".getBytes());
        expectedBounds.add("dffffff2".getBytes());
        expectedBounds.add("effffff1".getBytes());
        expectedBounds.add(ArrayUtils.EMPTY_BYTE_ARRAY);

        // Do table creation/pre-splitting and verification of region boundaries
        preSplitTableAndVerify(expectedBounds, HEX_SPLIT_CLASS_NAME,
                "NewHexPresplitTable");
    }

    /**
     * Test creating a pre-split table using the UniformSplit algorithm.
     */
    @Test
    public void testCreatePresplitTableUniform() throws Exception {
        List<byte[]> expectedBounds = new ArrayList<byte[]>();
        expectedBounds.add(ArrayUtils.EMPTY_BYTE_ARRAY);
        expectedBounds.add(new byte[] {      0x0f, xFF, xFF, xFF, xFF, xFF, xFF,        xFF});
        expectedBounds.add(new byte[] {      0x1f, xFF, xFF, xFF, xFF, xFF, xFF, (byte)0xfe});
        expectedBounds.add(new byte[] {      0x2f, xFF, xFF, xFF, xFF, xFF, xFF, (byte)0xfd});
        expectedBounds.add(new byte[] {      0x3f, xFF, xFF, xFF, xFF, xFF, xFF, (byte)0xfc});
        expectedBounds.add(new byte[] {      0x4f, xFF, xFF, xFF, xFF, xFF, xFF, (byte)0xfb});
        expectedBounds.add(new byte[] {      0x5f, xFF, xFF, xFF, xFF, xFF, xFF, (byte)0xfa});
        expectedBounds.add(new byte[] {      0x6f, xFF, xFF, xFF, xFF, xFF, xFF, (byte)0xf9});
        expectedBounds.add(new byte[] {      0x7f, xFF, xFF, xFF, xFF, xFF, xFF, (byte)0xf8});
        expectedBounds.add(new byte[] {(byte)0x8f, xFF, xFF, xFF, xFF, xFF, xFF, (byte)0xf7});
        expectedBounds.add(new byte[] {(byte)0x9f, xFF, xFF, xFF, xFF, xFF, xFF, (byte)0xf6});
        expectedBounds.add(new byte[] {(byte)0xaf, xFF, xFF, xFF, xFF, xFF, xFF, (byte)0xf5});
        expectedBounds.add(new byte[] {(byte)0xbf, xFF, xFF, xFF, xFF, xFF, xFF, (byte)0xf4});
        expectedBounds.add(new byte[] {(byte)0xcf, xFF, xFF, xFF, xFF, xFF, xFF, (byte)0xf3});
        expectedBounds.add(new byte[] {(byte)0xdf, xFF, xFF, xFF, xFF, xFF, xFF, (byte)0xf2});
        expectedBounds.add(new byte[] {(byte)0xef, xFF, xFF, xFF, xFF, xFF, xFF, (byte)0xf1});
        expectedBounds.add(ArrayUtils.EMPTY_BYTE_ARRAY);

        // Do table creation/pre-splitting and verification of region boundaries
        preSplitTableAndVerify(expectedBounds,
                "org.apache.hadoop.hbase.util.RegionSplitter$UniformSplit",
                "NewUniformPresplitTable");
    }

    /**
     * Unit tests for the HexStringSplit algorithm. Makes sure it divides up the
     * space of keys in the way that we expect.
     */
    @Test
    public void unitTestHexStringSplit() {
        HexStringSplit splitter = new HexStringSplit();
        // Check splitting while starting from scratch

        byte[][] twoRegionsSplits = splitter.split(2);
        assertEquals(1, twoRegionsSplits.length);
        assertArrayEquals(twoRegionsSplits[0], "7fffffff".getBytes());

        byte[][] threeRegionsSplits = splitter.split(3);
        assertEquals(2, threeRegionsSplits.length);
        byte[] expectedSplit0 = "55555555".getBytes();
        assertArrayEquals(expectedSplit0, threeRegionsSplits[0]);
        byte[] expectedSplit1 = "aaaaaaaa".getBytes();
        assertArrayEquals(expectedSplit1, threeRegionsSplits[1]);

        // Check splitting existing regions that have start and end points
        byte[] splitPoint = splitter.split("10000000".getBytes(), "30000000".getBytes());
        assertArrayEquals("20000000".getBytes(), splitPoint);

        byte[] lastRow = "ffffffff".getBytes();
        assertArrayEquals(lastRow, splitter.lastRow());
        byte[] firstRow = "00000000".getBytes();
        assertArrayEquals(firstRow, splitter.firstRow());

        // Halfway between 00... and 20... should be 10...
        splitPoint = splitter.split(firstRow, "20000000".getBytes());
        assertArrayEquals(splitPoint, "10000000".getBytes());

        // Halfway between 5f... and 7f... should be 6f....
        splitPoint = splitter.split("dfffffff".getBytes(), lastRow);
        assertArrayEquals(splitPoint,"efffffff".getBytes());
    }

    /**
     * Unit tests for the UniformSplit algorithm. Makes sure it divides up the space of
     * keys in the way that we expect.
     */
    @Test
    public void unitTestUniformSplit() {
        UniformSplit splitter = new UniformSplit();

        // Check splitting while starting from scratch
        try {
            splitter.split(1);
            throw new AssertionError("Splitting into <2 regions should have thrown exception");
        } catch (IllegalArgumentException e) { }

        byte[][] twoRegionsSplits = splitter.split(2);
        assertEquals(1, twoRegionsSplits.length);
        assertArrayEquals(twoRegionsSplits[0],
                new byte[] {0x7f, xFF, xFF, xFF, xFF, xFF, xFF, xFF});

        byte[][] threeRegionsSplits = splitter.split(3);
        assertEquals(2, threeRegionsSplits.length);
        byte[] expectedSplit0 = new byte[] {0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55};
        assertArrayEquals(expectedSplit0, threeRegionsSplits[0]);
        byte[] expectedSplit1 = new byte[] {(byte)0xAA, (byte)0xAA, (byte)0xAA, (byte)0xAA,
                (byte)0xAA, (byte)0xAA, (byte)0xAA, (byte)0xAA};
        assertArrayEquals(expectedSplit1, threeRegionsSplits[1]);

        // Check splitting existing regions that have start and end points
        byte[] splitPoint = splitter.split(new byte[] {0x10}, new byte[] {0x30});
        assertArrayEquals(new byte[] {0x20}, splitPoint);

        byte[] lastRow = new byte[] {xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF};
        assertArrayEquals(lastRow, splitter.lastRow());
        byte[] firstRow = ArrayUtils.EMPTY_BYTE_ARRAY;
        assertArrayEquals(firstRow, splitter.firstRow());

        splitPoint = splitter.split(firstRow, new byte[] {0x20});
        assertArrayEquals(splitPoint, new byte[] {0x10});

        splitPoint = splitter.split(new byte[] {(byte)0xdf, xFF, xFF, xFF, xFF,
                xFF, xFF, xFF}, lastRow);
        assertArrayEquals(splitPoint,
                new byte[] {(byte)0xef, xFF, xFF, xFF, xFF, xFF, xFF, xFF});
    }

    /**
     * Creates a pre-split table with expectedBounds.size()+1 regions, then
     * verifies that the region boundaries are the same as the expected
     * region boundaries in expectedBounds.
     * @throws Various junit assertions
     */
    private void preSplitTableAndVerify(List<byte[]> expectedBounds,
            String splitAlgo, String tableName) throws Exception {
        final int numRegions = expectedBounds.size()-1;
        final Configuration conf = UTIL.getConfiguration();
        conf.setInt("split.count", numRegions);
        RegionSplitter.createPresplitTable(tableName, splitAlgo,
                new String[] {CF_NAME}, conf);
        verifyBounds(expectedBounds, tableName);
    }

    private void rollingSplitAndVerify(String tableName, String splitAlgo,
            List<byte[]> expectedBounds)  throws Exception {
        final Configuration conf = UTIL.getConfiguration();

        // Set this larger than the number of splits so RegionSplitter won't block
        conf.setInt("split.outstanding", 5);
        RegionSplitter.rollingSplit(tableName, splitAlgo, conf);
        verifyBounds(expectedBounds, tableName);
    }

    private void verifyBounds(List<byte[]> expectedBounds, String tableName)
            throws Exception {
        // Get region boundaries from the cluster and verify their endpoints
        final Configuration conf = UTIL.getConfiguration();
        final int numRegions = expectedBounds.size()-1;
        final HTable hTable = new HTable(conf, tableName.getBytes());
        final Map<HRegionInfo, HServerAddress> regionInfoMap =
                hTable.getRegionsInfo();
        assertEquals(numRegions, regionInfoMap.size());
        for (Map.Entry<HRegionInfo, HServerAddress> entry:
           regionInfoMap.entrySet()) {
            final HRegionInfo regionInfo = entry.getKey();
            byte[] regionStart = regionInfo.getStartKey();
            byte[] regionEnd = regionInfo.getEndKey();

            // This region's start key should be one of the region boundaries
            int startBoundaryIndex = indexOfBytes(expectedBounds, regionStart);
            assertNotSame(-1, startBoundaryIndex);

            // This region's end key should be the region boundary that comes
            // after the starting boundary.
            byte[] expectedRegionEnd = expectedBounds.get(
                    startBoundaryIndex+1);
            assertEquals(0, Bytes.compareTo(regionEnd, expectedRegionEnd));
        }
    }

    /**
     * List.indexOf() doesn't really work for a List<byte[]>, because byte[]
     * doesn't override equals(). This method checks whether a list contains
     * a given element by checking each element using the byte array
     * comparator.
     * @return the index of the first element that equals compareTo, or -1
     * if no elements are equal.
     */
    static private int indexOfBytes(List<byte[]> list,  byte[] compareTo) {
        int listIndex = 0;
        for(byte[] elem: list) {
            if(Bytes.BYTES_COMPARATOR.compare(elem, compareTo) == 0) {
                return listIndex;
            }
            listIndex++;
        }
        return -1;
    }

    /**
     * Inserts some meaningless data into a CF so the regions can be split.
     */
    static void insertSomeData(String table) throws IOException {
        HTable hTable = new HTable(table);
        for(byte b=Byte.MIN_VALUE; b<Byte.MAX_VALUE; b++) {
            byte[] whateverBytes = new byte[] {b};
            Put p = new Put(whateverBytes);
            p.add(CF_NAME.getBytes(), whateverBytes, whateverBytes);
            hTable.put(p);
        }
    }
}
