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
package org.apache.hadoop.hbase.devtools.buildstats;

import com.offbytwo.jenkins.model.BaseModel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestSuite extends BaseModel {
  List<TestCaseResult> cases;

  public TestSuite() {
    this(new ArrayList<TestCaseResult>());
  }

  public TestSuite(List<TestCaseResult> s) {
    this.cases = s;
  }

  public TestSuite(TestCaseResult... s) {
    this(Arrays.asList(s));
  }

  public List<TestCaseResult> getCases() {
    return cases;
  }

  public void setCases(List<TestCaseResult> s) {
    cases = s;
  }
}
