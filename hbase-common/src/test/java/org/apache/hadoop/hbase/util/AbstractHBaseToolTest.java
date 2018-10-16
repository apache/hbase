/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.util;

import static org.apache.hadoop.hbase.util.AbstractHBaseTool.EXIT_FAILURE;
import static org.apache.hadoop.hbase.util.AbstractHBaseTool.EXIT_SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Before;
import org.junit.Test;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Option;

public class AbstractHBaseToolTest {
  static final class Options {
    static final Option REQUIRED = new Option(null, "required", true, "");
    static final Option OPTIONAL = new Option(null, "optional", true, "");
    static final Option BOOLEAN = new Option(null, "boolean", false, "");
  }

  /**
   * Simple tool to test options parsing.
   * 3 options: required, optional, and boolean
   * 2 deprecated options to test backward compatibility: -opt (old version of --optional) and
   * -bool (old version of --boolean).
   */
  private static class TestTool extends AbstractHBaseTool {
    String requiredValue;
    String optionalValue;
    boolean booleanValue;

    @Override
    protected void addOptions() {
      addRequiredOption(Options.REQUIRED);
      addOption(Options.OPTIONAL);
      addOption(Options.BOOLEAN);
    }

    @Override
    protected void processOptions(CommandLine cmd) {
      requiredValue = cmd.getOptionValue(Options.REQUIRED.getLongOpt());
      if (cmd.hasOption(Options.OPTIONAL.getLongOpt())) {
        optionalValue = cmd.getOptionValue(Options.OPTIONAL.getLongOpt());
      }
      booleanValue = booleanValue || cmd.hasOption(Options.BOOLEAN.getLongOpt());
    }

    @Override
    protected void processOldArgs(List<String> args) {
      List<String> invalidArgs = new ArrayList<>();
      while(args.size() > 0) {
        String cmd = args.remove(0);
        if (cmd.equals("-opt")) {
          optionalValue = args.remove(0);
        } else if (cmd.equals("-bool")) {
          booleanValue = true;
        } else {
          invalidArgs.add(cmd);
        }
      }
      args.addAll(invalidArgs);
    }

    @Override
    protected int doWork() throws Exception {
      return EXIT_SUCCESS;
    }
  }

  TestTool tool;

  @Before
  public void setup() {
    tool = new TestTool();
    tool.setConf(HBaseConfiguration.create());
  }

  @Test
  public void testAllOptionsSet() throws Exception {
    String[] args = new String[] { "--required=foo", "--optional=bar", "--boolean"};
    int returnValue = tool.run(args);
    assertEquals(EXIT_SUCCESS, returnValue);
    assertEquals("foo", tool.requiredValue);
    assertEquals("bar", tool.optionalValue);
    assertTrue(tool.booleanValue);
  }

  @Test
  public void testOptionsNotSet() throws Exception {
    String[] args = new String[] { "--required=foo" };
    int returnValue = tool.run(args);
    assertEquals(EXIT_SUCCESS, returnValue);
    assertEquals("foo", tool.requiredValue);
    assertNull(tool.optionalValue);
    assertFalse(tool.booleanValue);
  }

  @Test
  public void testMissingRequiredOption() throws Exception {
    String[] args = new String[0];
    int returnValue = tool.run(args);
    assertEquals(EXIT_FAILURE, returnValue);
  }

  @Test
  public void testFailureOnUnrecognizedOption() throws Exception {
    String[] args = new String[] { "--required=foo", "-asdfs" };
    int returnValue = tool.run(args);
    assertEquals(EXIT_FAILURE, returnValue);
  }

  @Test
  public void testOldOptionsWork() throws Exception {
    String[] args = new String[] { "--required=foo", "-opt", "bar", "-bool" };
    int returnValue = tool.run(args);
    assertEquals(EXIT_SUCCESS, returnValue);
    assertEquals("foo", tool.requiredValue);
    assertEquals("bar", tool.optionalValue);
    assertTrue(tool.booleanValue);
  }

  @Test
  public void testNewOptionOverridesOldOption() throws Exception {
    String[] args = new String[] { "--required=foo", "--optional=baz", "-opt", "bar", "-bool" };
    int returnValue = tool.run(args);
    assertEquals(EXIT_SUCCESS, returnValue);
    assertEquals("foo", tool.requiredValue);
    assertEquals("baz", tool.optionalValue);
    assertTrue(tool.booleanValue);
  }
}
