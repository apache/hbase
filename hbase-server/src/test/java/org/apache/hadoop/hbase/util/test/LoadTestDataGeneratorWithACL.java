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
package org.apache.hadoop.hbase.util.test;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.util.MultiThreadedAction.DefaultDataGenerator;

@InterfaceAudience.Private
public class LoadTestDataGeneratorWithACL extends DefaultDataGenerator {
  private static final Log LOG = LogFactory.getLog(LoadTestDataGeneratorWithACL.class);
  private String[] userNames = null;
  private static final String COMMA = ",";
  private int specialPermCellInsertionFactor = 100;

  public LoadTestDataGeneratorWithACL(int minValueSize, int maxValueSize, int minColumnsPerKey,
      int maxColumnsPerKey, byte[]... columnFamilies) {
    super(minValueSize, maxValueSize, minColumnsPerKey, maxColumnsPerKey, columnFamilies);
  }

  @Override
  public void initialize(String[] args) {
    super.initialize(args);
    if (args.length != 3) {
      throw new IllegalArgumentException(
          "LoadTestDataGeneratorWithACL can have "
              + "1st arguement which would be super user, the 2nd argument "
              + "would be the user list and the 3rd argument should be the factor representing "
              + "the row keys for which only write ACLs will be added.");
    }
    String temp = args[1];
    // This will be comma separated list of expressions.
    this.userNames = temp.split(COMMA);
    this.specialPermCellInsertionFactor = Integer.parseInt(args[2]);
  }

  @Override
  public Mutation beforeMutate(long rowkeyBase, Mutation m) throws IOException {
    if (!(m instanceof Delete)) {
      if (userNames != null && userNames.length > 0) {
        int mod = ((int) rowkeyBase % this.userNames.length);
        if (((int) rowkeyBase % specialPermCellInsertionFactor) == 0) {
          // These cells cannot be read back when running as user userName[mod]
          if (LOG.isTraceEnabled()) {
            LOG.trace("Adding special perm " + rowkeyBase);
          }
          m.setACL(userNames[mod], new Permission(Permission.Action.WRITE));
        } else {
          m.setACL(userNames[mod], new Permission(Permission.Action.READ));
        }
      }
    }
    return m;
  }
}
