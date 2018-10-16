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

package org.apache.hadoop.hbase.protobuf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.gson.JsonArray;
import org.apache.hbase.thirdparty.com.google.gson.JsonElement;
import org.apache.hbase.thirdparty.com.google.gson.JsonObject;
import org.apache.hbase.thirdparty.com.google.gson.JsonParser;
import org.apache.hbase.thirdparty.com.google.gson.JsonPrimitive;
import org.apache.hbase.thirdparty.com.google.protobuf.BytesValue;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hbase.thirdparty.com.google.protobuf.MessageOrBuilder;
import org.apache.hbase.thirdparty.com.google.protobuf.util.JsonFormat;
import org.apache.hbase.thirdparty.com.google.protobuf.util.JsonFormat.TypeRegistry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * This class converts PB Messages to various representations, like:
 * <ul>
 * <li>JSON string: {@link #toJsonElement(MessageOrBuilder)}</li>
 * <li>JSON object (gson): {@link #toJsonElement(MessageOrBuilder)}</li>
 * <li>Java objects (Boolean, Number, String, List, Map):
 * {@link #toJavaObject(JsonElement)}</li>
 * </ul>
 */
@InterfaceAudience.Private
public class ProtobufMessageConverter {
  private static final String TYPE_KEY = "@type";

  private static final JsonFormat.Printer jsonPrinter;

  static {
    TypeRegistry.Builder builder = TypeRegistry.newBuilder();
    builder
      .add(BytesValue.getDescriptor())
      .add(LockServiceProtos.getDescriptor().getMessageTypes())
      .add(MasterProcedureProtos.getDescriptor().getMessageTypes())
      .add(ProcedureProtos.getDescriptor().getMessageTypes());
    TypeRegistry typeRegistry = builder.build();
    jsonPrinter = JsonFormat.printer()
        .usingTypeRegistry(typeRegistry)
        .omittingInsignificantWhitespace();
  }

  private ProtobufMessageConverter() {
  }

  public static String toJsonString(MessageOrBuilder messageOrBuilder)
      throws InvalidProtocolBufferException {
    return jsonPrinter.print(messageOrBuilder);
  }

  private static void removeTypeFromJson(JsonElement json) {
    if (json.isJsonArray()) {
      for (JsonElement child: json.getAsJsonArray()) {
        removeTypeFromJson(child);
      }
    } else if (json.isJsonObject()) {
      Iterator<Entry<String, JsonElement>> iterator =
          json.getAsJsonObject().entrySet().iterator();

      while (iterator.hasNext()) {
        Entry<String, JsonElement> entry = iterator.next();
        if (TYPE_KEY.equals(entry.getKey())) {
          iterator.remove();
        } else {
          removeTypeFromJson(entry.getValue());
        }
      }
    }
  }

  public static JsonElement toJsonElement(MessageOrBuilder messageOrBuilder)
      throws InvalidProtocolBufferException {
    return toJsonElement(messageOrBuilder, true);
  }

  public static JsonElement toJsonElement(MessageOrBuilder messageOrBuilder,
      boolean removeType) throws InvalidProtocolBufferException {
    String jsonString = toJsonString(messageOrBuilder);
    JsonParser parser = new JsonParser();
    JsonElement element = parser.parse(jsonString);
    if (removeType) {
      removeTypeFromJson(element);
    }
    return element;
  }

  private static Object toJavaObject(JsonElement element) {
    if (element.isJsonNull()) {
      return null;
    } else if (element.isJsonPrimitive()) {
      JsonPrimitive primitive = element.getAsJsonPrimitive();
      if (primitive.isBoolean()) {
        return primitive.getAsBoolean();
      } else if (primitive.isNumber()) {
        return primitive.getAsNumber();
      } else if (primitive.isString()) {
        return primitive.getAsString();
      } else {
        return null;
      }
    } else if (element.isJsonArray()) {
      JsonArray array = element.getAsJsonArray();
      List<Object> list = new ArrayList<>();

      for (JsonElement arrayElement : array) {
        Object javaObject = toJavaObject(arrayElement);
        list.add(javaObject);
      }

      return list;
    } else if (element.isJsonObject()) {
      JsonObject object = element.getAsJsonObject();
      Map<String, Object> map = new LinkedHashMap<>();

      for (Entry<String, JsonElement> entry: object.entrySet()) {
        Object javaObject = toJavaObject(entry.getValue());
        map.put(entry.getKey(), javaObject);
      }

      return map;
    } else {
      return null;
    }
  }

  public static Object toJavaObject(MessageOrBuilder messageOrBuilder)
      throws InvalidProtocolBufferException {
    JsonElement element = toJsonElement(messageOrBuilder);
    return toJavaObject(element);
  }
}
