/*
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

package org.apache.hadoop.hbase.regionserver.querymatcher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher.MatchCode;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestScanWildcardColumnTracker {

  final static int VERSIONS = 2;

  @Test
  public void testCheckColumnOk() throws IOException {
    ScanWildcardColumnTracker tracker = new ScanWildcardColumnTracker(0, VERSIONS, Long.MIN_VALUE);

    // Create list of qualifiers
    List<byte[]> qualifiers = new ArrayList<byte[]>();
    qualifiers.add(Bytes.toBytes("qualifier1"));
    qualifiers.add(Bytes.toBytes("qualifier2"));
    qualifiers.add(Bytes.toBytes("qualifier3"));
    qualifiers.add(Bytes.toBytes("qualifier4"));

    // Setting up expected result
    List<MatchCode> expected = new ArrayList<MatchCode>();
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);

    List<ScanQueryMatcher.MatchCode> actual = new ArrayList<MatchCode>();

    for (byte[] qualifier : qualifiers) {
      ScanQueryMatcher.MatchCode mc = ScanQueryMatcher.checkColumn(tracker, qualifier, 0,
        qualifier.length, 1, KeyValue.Type.Put.getCode(), false);
      actual.add(mc);
    }

    // Compare actual with expected
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), actual.get(i));
    }
  }

  @Test
  public void testCheckColumnEnforceVersions() throws IOException {
    ScanWildcardColumnTracker tracker = new ScanWildcardColumnTracker(0, VERSIONS, Long.MIN_VALUE);

    // Create list of qualifiers
    List<byte[]> qualifiers = new ArrayList<byte[]>();
    qualifiers.add(Bytes.toBytes("qualifier1"));
    qualifiers.add(Bytes.toBytes("qualifier1"));
    qualifiers.add(Bytes.toBytes("qualifier1"));
    qualifiers.add(Bytes.toBytes("qualifier2"));

    // Setting up expected result
    List<ScanQueryMatcher.MatchCode> expected = new ArrayList<MatchCode>();
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);

    List<MatchCode> actual = new ArrayList<ScanQueryMatcher.MatchCode>();

    long timestamp = 0;
    for (byte[] qualifier : qualifiers) {
      MatchCode mc = ScanQueryMatcher.checkColumn(tracker, qualifier, 0, qualifier.length,
        ++timestamp, KeyValue.Type.Put.getCode(), false);
      actual.add(mc);
    }

    // Compare actual with expected
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), actual.get(i));
    }
  }

  @Test
  public void DisabledTestCheckColumnWrongOrder() {
    ScanWildcardColumnTracker tracker = new ScanWildcardColumnTracker(0, VERSIONS, Long.MIN_VALUE);

    // Create list of qualifiers
    List<byte[]> qualifiers = new ArrayList<byte[]>();
    qualifiers.add(Bytes.toBytes("qualifier2"));
    qualifiers.add(Bytes.toBytes("qualifier1"));

    try {
      for (byte[] qualifier : qualifiers) {
        ScanQueryMatcher.checkColumn(tracker, qualifier, 0, qualifier.length, 1,
          KeyValue.Type.Put.getCode(), false);
      }
      fail();
    } catch (IOException e) {
      // expected
    }
  }
}
