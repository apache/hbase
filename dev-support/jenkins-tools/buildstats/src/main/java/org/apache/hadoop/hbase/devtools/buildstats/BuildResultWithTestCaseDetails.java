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

public class BuildResultWithTestCaseDetails extends BaseModel {

  List<TestSuite> suites;

  /* default constructor needed for Jackson */
  public BuildResultWithTestCaseDetails() {
    this(new ArrayList<TestSuite>());
  }

  public BuildResultWithTestCaseDetails(List<TestSuite> s) {
    this.suites = s;
  }

  public BuildResultWithTestCaseDetails(TestSuite... s) {
    this(Arrays.asList(s));
  }

  public List<TestSuite> getSuites() {
    return suites;
  }

  public void setSuites(List<TestSuite> s) {
    suites = s;
  }
}
