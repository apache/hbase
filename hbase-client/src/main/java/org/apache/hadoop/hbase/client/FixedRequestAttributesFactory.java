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
package org.apache.hadoop.hbase.client;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A {@link RequestAttributesFactory} that returns a fixed set of attributes for every call. Use
 * this when attributes do not need to change for the lifetime of the {@link AsyncTable}.
 * @see AsyncTableBuilder#setRequestAttributesFactory(RequestAttributesFactory)
 */
@InterfaceAudience.Public
public final class FixedRequestAttributesFactory implements RequestAttributesFactory {

  /**
   * A factory that always returns an empty map.
   */
  public static final RequestAttributesFactory EMPTY = Collections::emptyMap;

  /**
   * Builder for creating {@link FixedRequestAttributesFactory} instances.
   */
  public static final class Builder {
    private final Map<String, byte[]> requestAttributes = new HashMap<>();

    /**
     * Sets a request attribute. If value is null, the attribute is removed.
     * @param key   the attribute key
     * @param value the attribute value, or null to remove
     * @return this builder
     */
    public Builder setAttribute(String key, byte[] value) {
      if (value == null) {
        requestAttributes.remove(key);
      } else {
        requestAttributes.put(key, value);
      }
      return this;
    }

    /**
     * Builds a {@link FixedRequestAttributesFactory} with the configured attributes.
     * @return the factory
     */
    public FixedRequestAttributesFactory build() {
      return new FixedRequestAttributesFactory(requestAttributes);
    }
  }

  /**
   * Returns a new builder.
   * @return a new builder instance
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  private final Map<String, byte[]> requestAttributes;

  private FixedRequestAttributesFactory(Map<String, byte[]> requestAttributes) {
    this.requestAttributes = Map.copyOf(requestAttributes);
  }

  @Override
  public Map<String, byte[]> create() {
    return requestAttributes;
  }
}
