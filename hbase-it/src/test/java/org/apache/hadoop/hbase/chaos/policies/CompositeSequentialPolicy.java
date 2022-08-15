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
package org.apache.hadoop.hbase.chaos.policies;

import java.util.Arrays;
import java.util.List;

/** A policy that runs multiple other policies one after the other */
public class CompositeSequentialPolicy extends Policy {
  private final List<Policy> policies;

  public CompositeSequentialPolicy(Policy... policies) {
    this.policies = Arrays.asList(policies);
  }

  @Override
  public void stop(String why) {
    super.stop(why);
    for (Policy p : policies) {
      p.stop(why);
    }
  }

  @Override
  public void run() {
    for (Policy p : policies) {
      p.run();
    }
  }

  @Override
  public void init(PolicyContext context) throws Exception {
    super.init(context);
    for (Policy p : policies) {
      p.init(context);
    }
  }
}
