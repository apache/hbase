/**
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
package org.apache.hadoop.hbase.security.visibility;

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.MultiThreadedAction.DefaultDataGenerator;

@InterfaceAudience.Private
public class LoadTestDataGeneratorWithVisibilityLabels extends DefaultDataGenerator {

  private static final String COMMA = ",";
  private String[] visibilityExps = null;
  private String[][] authorizations = null;

  public LoadTestDataGeneratorWithVisibilityLabels(int minValueSize, int maxValueSize,
      int minColumnsPerKey, int maxColumnsPerKey, byte[]... columnFamilies) {
    super(minValueSize, maxValueSize, minColumnsPerKey, maxColumnsPerKey, columnFamilies);
  }

  @Override
  public void initialize(String[] args) {
    super.initialize(args);
    if (args.length < 1 || args.length > 2) {
      throw new IllegalArgumentException("LoadTestDataGeneratorWithVisibilityLabels can have "
          + "1 or 2 initialization arguments");
    }
    // 1st arg in args is supposed to be the visibilityExps to be used with Mutations.
    String temp = args[0];
    // This will be comma separated list of expressions.
    this.visibilityExps = temp.split(COMMA);
    // 2nd arg in args,if present, is supposed to be comma separated set of authorizations to be
    // used with Gets. Each of the set will be comma separated within square brackets.
    // Eg: [secret,private],[confidential,private],[public]
    if (args.length == 2) {
      this.authorizations = toAuthorizationsSet(args[1]);
    }
  }

  private static String[][] toAuthorizationsSet(String authorizationsStr) {
    // Eg: [secret,private],[confidential,private],[public]
    String[] split = authorizationsStr.split("],");
    String[][] result = new String[split.length][];
    for (int i = 0; i < split.length; i++) {
      String s = split[i].trim();
      assert s.charAt(0) == '[';
      s = s.substring(1);
      if (i == split.length - 1) {
        assert s.charAt(s.length() - 1) == ']';
        s = s.substring(0, s.length() - 1);
      }
      String[] tmp = s.split(COMMA);
      for (int j = 0; j < tmp.length; j++) {
        tmp[j] = tmp[j].trim();
      }
      result[i] = tmp;
    }
    return result;
  }

  @Override
  public Mutation beforeMutate(long rowkeyBase, Mutation m) throws IOException {
    m.setCellVisibility(new CellVisibility(this.visibilityExps[(int) rowkeyBase
        % this.visibilityExps.length]));
    return m;
  }

  @Override
  public Get beforeGet(long rowkeyBase, Get get) {
    get.setAuthorizations(new Authorizations(
        authorizations[(int) (rowkeyBase % authorizations.length)]));
    return get;
  }
}
