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
package org.apache.hadoop.hbase.util.customthreadattribute;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.slf4j.MDC;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RequestIdContextHandler implements AttributeTypeHandler {

  private static String KEY = "RequestID";

  RequestIdContextHandler() {
  }

  @Override public List<CustomThreadAttribute> getAllAttributes() {
    List<CustomThreadAttribute> list = new ArrayList<>();
    CustomThreadAttribute attribute = getAttribute(null);
    if (attribute != null) {
      list.add(attribute);
    }
    return list;
  }

  @Override public void setAttribute(String key, Object value) {
    MDC.put(KEY, value.toString());
  }

  @Override public void clearAttribute(String key) {
    MDC.clear();
  }

  @Override public CustomThreadAttribute getAttribute(String key) {
    return new CustomThreadAttribute(KEY, MDC.get(KEY), null);
  }
}
