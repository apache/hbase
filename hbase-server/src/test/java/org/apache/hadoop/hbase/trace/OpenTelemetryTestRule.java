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
package org.apache.hadoop.hbase.trace;

import org.junit.rules.ExternalResource;

/**
 * Used alongside {@link OpenTelemetryClassRule}. See that class's javadoc for details on when to
 * use these classes instead of {@link io.opentelemetry.sdk.testing.junit4.OpenTelemetryRule} and
 * an example of how to use these classes together.
 */
public final class OpenTelemetryTestRule extends ExternalResource {

  private final OpenTelemetryClassRule classRuleSupplier;

  public OpenTelemetryTestRule(final OpenTelemetryClassRule classRule) {
    this.classRuleSupplier = classRule;
  }

  @Override
  protected void before() throws Throwable {
    classRuleSupplier.clearSpans();
  }
}
