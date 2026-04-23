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
package org.apache.hadoop.hbase.util;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Category({ MiscTests.class, SmallTests.class })
class TestXXH3 {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestXXH3.class);

  private static final String RESOURCE = "xxh3/xxh3_vectors.csv";
  private static final XXH3 xxh3 = new XXH3();

  static class Case {
    final int len;
    final long seed;
    final long expected;

    Case(int len, long seed, long expected) {
      this.len = len;
      this.seed = seed;
      this.expected = expected;
    }
  }

  static Stream<Case> vectors() throws Exception {
    InputStream is = TestXXH3.class.getClassLoader().getResourceAsStream(RESOURCE);
    if (is == null) {
      throw new IllegalStateException(RESOURCE + " not found in resources");
    }

    try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
      List<Case> out = new ArrayList<>(4096);

      br.lines().filter(s -> !s.isEmpty()).forEach(line -> {
        if (line.startsWith("#")) {
          return;
        }
        String[] p = line.split(",");
        int len = Integer.parseInt(p[0]);

        for (int i = 1; i + 1 < p.length; i += 2) {
          long seed = Long.parseLong(p[i]);
          long expected = Long.parseLong(p[i + 1]);
          out.add(new Case(len, seed, expected));
        }
      });

      return out.stream();
    }
  }

  @ParameterizedTest
  @MethodSource("vectors")
  void testXxh3(Case c) {
    byte[] buf = new byte[c.len];
    for (int i = 0; i < c.len; i++) {
      buf[i] = (byte) i;
    }

    final ByteArrayHashKey key = new ByteArrayHashKey(buf, 0, buf.length);
    long actual = xxh3.hash64(key, c.seed);
    assertEquals(c.expected, actual, "len=" + c.len + " seed=" + c.seed);
  }
}
