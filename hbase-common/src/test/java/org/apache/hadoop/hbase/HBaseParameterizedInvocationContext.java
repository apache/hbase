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
package org.apache.hadoop.hbase;

import java.util.List;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.params.provider.Arguments;

/**
 * @see HBaseParameterizedTestTemplate
 */
public class HBaseParameterizedInvocationContext implements TestTemplateInvocationContext {

  private final Arguments arguments;

  private final String namePattern;

  HBaseParameterizedInvocationContext(Arguments arguments, String namePattern) {
    this.arguments = arguments;
    this.namePattern = namePattern;
  }

  @Override
  public String getDisplayName(int invocationIndex) {
    String result = namePattern.replace("{index}", String.valueOf(invocationIndex));

    Object[] args = arguments.get();
    for (int i = 0; i < args.length; i++) {
      result = result.replace("{" + i + "}", String.valueOf(args[i]));
    }
    return result;
  }

  @Override
  public List<Extension> getAdditionalExtensions() {
    return List.of(new HBaseParameterizedParameterResolver(arguments));
  }
}
