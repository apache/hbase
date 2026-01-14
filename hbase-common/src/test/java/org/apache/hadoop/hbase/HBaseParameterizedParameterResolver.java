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

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.params.provider.Arguments;

import org.apache.hbase.thirdparty.com.google.common.primitives.Primitives;

/**
 * @see HBaseParameterizedTestTemplate
 */
public class HBaseParameterizedParameterResolver implements ParameterResolver {

  private final Object[] values;

  HBaseParameterizedParameterResolver(Arguments arguments) {
    this.values = arguments.get();
  }

  @Override
  public boolean supportsParameter(ParameterContext pc, ExtensionContext ec)
    throws ParameterResolutionException {
    int index = pc.getIndex();
    if (index >= values.length) {
      return false;
    }
    Object value = values[index];
    Class<?> expectedType = pc.getParameter().getType();
    if (expectedType.isPrimitive()) {
      // primitive type can not accept null value
      if (value == null) {
        return false;
      }
      // test with wrapper type, otherwise it will always return false
      return Primitives.wrap(expectedType).isAssignableFrom(value.getClass());
    }
    return expectedType.isAssignableFrom(value.getClass());
  }

  @Override
  public Object resolveParameter(ParameterContext pc, ExtensionContext ec)
    throws ParameterResolutionException {
    return values[pc.getIndex()];
  }
}
