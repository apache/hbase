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

/** Modified based on io.netty.handler.codec.protobuf.ProtobufDecoder */
@InterfaceAudience.Private
public class ProtobufDecoder extends MessageToMessageDecoder<ByteBuf> {
  private static final Logger LOG =
    LoggerFactory.getLogger(ProtobufDecoder.class);

  private static Class<?> shadedHadoopProtobufMessageLite = null;
  private static final boolean HAS_PARSER;
  private Object prototype;

  private static Method getParserForTypeMethod;
  private static Method newBuilderForTypeMethod;

  private Method parseFromMethod;
  private Method mergeFromMethod;

  private Object parser;
  private Object builder;


  public ProtobufDecoder(Object prototype) {
    try {
      Method getDefaultInstanceForTypeMethod = shadedHadoopProtobufMessageLite.getMethod("getDefaultInstanceForType");
      this.prototype = getDefaultInstanceForTypeMethod.invoke(ObjectUtil.checkNotNull(prototype, "prototype"));

      parser = getParserForTypeMethod.invoke(this.prototype);
      parseFromMethod = parser.getClass().getMethod("parseFrom", byte[].class, int.class, int.class);

      builder = newBuilderForTypeMethod.invoke(this.prototype);
      mergeFromMethod = builder.getClass().getMethod("mergeFrom", byte[].class, int.class, int.class);

    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
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
      addObj = parseFromMethod.invoke(parser, array, offset, length);
    } else {
      Object builderObj = mergeFromMethod.invoke(builder, array, offset, length);
      Method buildMethod = builderObj.getClass().getDeclaredMethod("build");
      addObj = buildMethod.invoke(builderObj);
    }
    out.add(addObj);
  }

  static {
    boolean hasParser = false;

    try {
      shadedHadoopProtobufMessageLite = Class.forName("org.apache.hadoop.thirdparty.protobuf.MessageLite");
      LOG.debug("Hadoop 3.3 and above shades protobuf.");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    if (shadedHadoopProtobufMessageLite == null) {
      shadedHadoopProtobufMessageLite = com.google.protobuf.MessageLite.class;
      LOG.debug("Hadoop 3.2 and below use unshaded protobuf.");
    }

    try {
      getParserForTypeMethod = shadedHadoopProtobufMessageLite.getDeclaredMethod("getParserForType");
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    }

    try {
      newBuilderForTypeMethod = shadedHadoopProtobufMessageLite.getDeclaredMethod("newBuilderForType");
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    }

    try {
      shadedHadoopProtobufMessageLite.getDeclaredMethod("getParserForType");
      hasParser = true;
    } catch (Throwable var2) {
    }

    HAS_PARSER = hasParser;
  }
}
