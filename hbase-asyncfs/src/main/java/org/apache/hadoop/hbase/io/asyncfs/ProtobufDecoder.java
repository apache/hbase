/**
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
package org.apache.hadoop.hbase.io.asyncfs;

import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBufUtil;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.hbase.thirdparty.io.netty.util.internal.ObjectUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Modified based on io.netty.handler.codec.protobuf.ProtobufDecoder.
 * The Netty's ProtobufDecode supports unshaded protobuf messages (com.google.protobuf).
 *
 * Hadoop 3.3.0 and above relocates protobuf classes to a shaded jar (hadoop-thirdparty), and
 * so we must use reflection to detect which one (relocated or not) to use.
 *
 * Do not use this to process HBase's shaded protobuf messages. This is meant to process the
 * protobuf messages in HDFS for the asyncfs use case.
 * */
@InterfaceAudience.Private
public class ProtobufDecoder extends MessageToMessageDecoder<ByteBuf> {
  private static final Logger LOG =
    LoggerFactory.getLogger(ProtobufDecoder.class);

  private static Class<?> protobufMessageLiteClass = null;
  private static Class<?> protobufMessageLiteBuilderClass = null;

  private static final boolean HAS_PARSER;

  private static Method getParserForTypeMethod;
  private static Method newBuilderForTypeMethod;

  private Method parseFromMethod;
  private Method mergeFromMethod;
  private Method buildMethod;

  private Object parser;
  private Object builder;


  public ProtobufDecoder(Object prototype) {
    try {
      Method getDefaultInstanceForTypeMethod = protobufMessageLiteClass.getMethod(
        "getDefaultInstanceForType");
      Object prototype1 = getDefaultInstanceForTypeMethod
        .invoke(ObjectUtil.checkNotNull(prototype, "prototype"));

      // parser = prototype.getParserForType()
      parser = getParserForTypeMethod.invoke(prototype1);
      parseFromMethod = parser.getClass().getMethod(
        "parseFrom", byte[].class, int.class, int.class);

      // builder = prototype.newBuilderForType();
      builder = newBuilderForTypeMethod.invoke(prototype1);
      mergeFromMethod = builder.getClass().getMethod(
        "mergeFrom", byte[].class, int.class, int.class);

      // All protobuf message builders inherits from MessageLite.Builder
      buildMethod = protobufMessageLiteBuilderClass.getDeclaredMethod("build");

    } catch (IllegalAccessException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e.getTargetException());
    }
  }

  protected void decode(
    ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
    int length = msg.readableBytes();
    byte[] array;
    int offset;
    if (msg.hasArray()) {
      array = msg.array();
      offset = msg.arrayOffset() + msg.readerIndex();
    } else {
      array = ByteBufUtil.getBytes(msg, msg.readerIndex(), length, false);
      offset = 0;
    }

    Object addObj;
    if (HAS_PARSER) {
      // addObj = parser.parseFrom(array, offset, length);
      addObj = parseFromMethod.invoke(parser, array, offset, length);
    } else {
      // addObj = builder.mergeFrom(array, offset, length).build();
      Object builderObj = mergeFromMethod.invoke(builder, array, offset, length);
      addObj = buildMethod.invoke(builderObj);
    }
    out.add(addObj);
  }

  static {
    boolean hasParser = false;

    // These are the protobuf classes coming from Hadoop. Not the one from hbase-shaded-protobuf
    protobufMessageLiteClass = com.google.protobuf.MessageLite.class;
    protobufMessageLiteBuilderClass = com.google.protobuf.MessageLite.Builder.class;

    try {
      protobufMessageLiteClass = Class.forName("org.apache.hadoop.thirdparty.protobuf.MessageLite");
      protobufMessageLiteBuilderClass = Class.forName(
        "org.apache.hadoop.thirdparty.protobuf.MessageLite$Builder");
      LOG.debug("Hadoop 3.3 and above shades protobuf.");
    } catch (ClassNotFoundException e) {
      LOG.debug("Hadoop 3.2 and below use unshaded protobuf.", e);
    }

    try {
      getParserForTypeMethod = protobufMessageLiteClass.getDeclaredMethod("getParserForType");
      newBuilderForTypeMethod = protobufMessageLiteClass.getDeclaredMethod("newBuilderForType");
      // TODO: If this is false then the class will fail to load? Can refactor it out?
      hasParser = true;
    } catch (NoSuchMethodException e) {
      // If the method is not found, we are in trouble. Abort.
      throw new RuntimeException(e);
    }

    HAS_PARSER = hasParser;
  }
}
