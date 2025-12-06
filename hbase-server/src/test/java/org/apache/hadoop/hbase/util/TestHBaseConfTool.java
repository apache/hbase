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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.SystemExitRule;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveLogCleaner;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestHBaseConfTool {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHBaseConfTool.class);

  @Test
  public void testHBaseConfTool() {
    String[] args = { TimeToLiveLogCleaner.TTL_CONF_KEY };
    PrintStream stdout = System.out;

    try {
      DummyPrintStream printStream = new DummyPrintStream(System.out);
      System.setOut(printStream);

      HBaseConfTool.main(args);

      List<String> printedLines = printStream.getPrintedLines();
      assertNotNull(printedLines);
      assertEquals(1, printedLines.size());
      assertEquals(String.valueOf(TimeToLiveLogCleaner.DEFAULT_TTL), printedLines.get(0));
    } finally {
      // reset to standard output
      System.setOut(stdout);
    }
  }

  @Test
  public void testHBaseConfToolError() {
    String[] args = {};
    PrintStream stdout = System.out;
    PrintStream stderr = System.err;

    DummyPrintStream printStream = new DummyPrintStream(System.out);
    DummyPrintStream errStream = new DummyPrintStream(System.out);
    System.setOut(printStream);
    System.setErr(errStream);

    try {
      HBaseConfTool.main(args);
    } catch (SystemExitRule.SystemExitInTestException ex) {
      List<String> printedLines = printStream.getPrintedLines();
      assertNotNull(printedLines);
      assertEquals(0, printedLines.size());

      List<String> printedErrLines = errStream.getPrintedLines();
      assertNotNull(printedErrLines);
      assertEquals(1, printedErrLines.size());
      assertTrue(printedErrLines.get(0).contains("Usage: HBaseConfTool"));
    } finally {
      // reset to standard output
      System.setOut(stdout);
      System.setErr(stderr);
    }
  }

  static class DummyPrintStream extends PrintStream {

    private List<String> printedLines = new LinkedList<>();

    public DummyPrintStream(PrintStream printStream) {
      super(printStream);
    }

    @Override
    public void println(String line) {
      printedLines.add(line);
      super.println(line);
    }

    public List<String> getPrintedLines() {
      return printedLines;
    }
  }
}
