/**
 *
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

package org.apache.hadoop.hbase.tool.coprocessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class CoprocessorMethod {
  private final String name;
  private final List<String> parameters;

  public CoprocessorMethod(String name) {
    this.name = name;

    parameters = new ArrayList<>();
  }

  public CoprocessorMethod withParameters(String ... parameters) {
    for (String parameter : parameters) {
      this.parameters.add(parameter);
    }

    return this;
  }

  public CoprocessorMethod withParameters(Class<?> ... parameters) {
    for (Class<?> parameter : parameters) {
      this.parameters.add(parameter.getCanonicalName());
    }

    return this;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (!(obj instanceof CoprocessorMethod)) {
      return false;
    }

    CoprocessorMethod other = (CoprocessorMethod)obj;

    return Objects.equals(name, other.name) &&
        Objects.equals(parameters, other.parameters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, parameters);
  }
}
