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
package org.apache.hadoop.hbase.master.http.gson;

import java.lang.reflect.Type;
import org.apache.hadoop.hbase.Size;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.gson.JsonElement;
import org.apache.hbase.thirdparty.com.google.gson.JsonPrimitive;
import org.apache.hbase.thirdparty.com.google.gson.JsonSerializationContext;
import org.apache.hbase.thirdparty.com.google.gson.JsonSerializer;

/**
 * Simplify representation of a {@link Size} instance by converting to bytes.
 */
@InterfaceAudience.Private
final class SizeAsBytesSerializer implements JsonSerializer<Size> {

  @Override
  public JsonElement serialize(Size src, Type typeOfSrc, JsonSerializationContext context) {
    return new JsonPrimitive(src.get(Size.Unit.BYTE));
  }
}
