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

package org.apache.hadoop.hbase.util;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.escape.Escaper;
import org.apache.hbase.thirdparty.com.google.common.escape.Escapers;

/**
 * Utility class for converting objects to JRuby.
 *
 * It handles null, Boolean, Number, String, byte[], List&lt;Object>, Map&lt;String, Object>
 *   structures.
 *
 * <p>
 * E.g.
 * <pre>
 * Map&lt;String, Object> map = new LinkedHashMap&lt;>();
 * map.put("null", null);
 * map.put("boolean", true);
 * map.put("number", 1);
 * map.put("string", "str");
 * map.put("binary", new byte[] { 1, 2, 3 });
 * map.put("list", Lists.newArrayList(1, "2", true));
 * </pre>
 * </p>
 *
 * <p>
 * Calling {@link #print(Object)} method will result:
 * <pre>
 * { null => '', boolean => 'true', number => '1', string => 'str',
 *   binary => '010203', list => [ '1', '2', 'true' ] }
 * </pre>
 * </p>
 */
@InterfaceAudience.Private
public final class JRubyFormat {
  private static final Escaper escaper;

  static {
    escaper = Escapers.builder()
      .addEscape('\\', "\\\\")
      .addEscape('\'', "\\'")
      .addEscape('\n', "\\n")
      .addEscape('\r', "\\r")
      .addEscape('\t', "\\t")
      .addEscape('\f', "\\f")
      .build();
  }

  private JRubyFormat() {
  }

  private static String escape(Object object) {
    if (object == null) {
      return "";
    } else {
      return escaper.escape(object.toString());
    }
  }

  @SuppressWarnings({ "unchecked" })
  private static void appendJRuby(StringBuilder builder, Object object) {
    if (object == null) {
      builder.append("''");
    } else if (object instanceof List) {
      builder.append("[");

      boolean first = true;

      for (Object element: (List<Object>)object) {
        if (first) {
          first = false;
          builder.append(" ");
        } else {
          builder.append(", ");
        }

        appendJRuby(builder, element);
      }

      if (!first) {
        builder.append(" ");
      }

      builder.append("]");
    } else if (object instanceof Map) {
      builder.append("{");

      boolean first = true;

      for (Entry<String, Object> entry: ((Map<String, Object>)object).entrySet()) {
        if (first) {
          first = false;
          builder.append(" ");
        } else {
          builder.append(", ");
        }

        String key = entry.getKey();
        String escapedKey = escape(key);

        if (key.equals(escapedKey)) {
          builder.append(key);
        } else {
          builder.append("'").append(escapedKey).append("'");
        }

        builder.append(" => ");
        appendJRuby(builder, entry.getValue());
      }

      if (!first) {
        builder.append(" ");
      }

      builder.append("}");
    } else if (object instanceof byte[]) {
      String byteString = Bytes.toHex((byte[])object);
      builder.append("'").append(escape(byteString)).append("'");
    } else {
      builder.append("'").append(escape(object)).append("'");
    }
  }

  public static String print(Object object) {
    StringBuilder builder = new StringBuilder();

    appendJRuby(builder, object);

    return builder.toString();
  }
}
