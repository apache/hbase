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

package org.apache.hadoop.hbase.rest.provider.consumer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import org.apache.hadoop.hbase.rest.Constants;
import org.apache.hadoop.hbase.rest.ProtobufMessageHandler;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.javax.ws.rs.Consumes;
import org.apache.hbase.thirdparty.javax.ws.rs.WebApplicationException;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MultivaluedMap;
import org.apache.hbase.thirdparty.javax.ws.rs.ext.MessageBodyReader;
import org.apache.hbase.thirdparty.javax.ws.rs.ext.Provider;

/**
 * Adapter for hooking up Jersey content processing dispatch to
 * ProtobufMessageHandler interface capable handlers for decoding protobuf input.
 */
@Provider
@Consumes({Constants.MIMETYPE_PROTOBUF, Constants.MIMETYPE_PROTOBUF_IETF})
@InterfaceAudience.Private
public class ProtobufMessageBodyConsumer
    implements MessageBodyReader<ProtobufMessageHandler> {
  private static final Logger LOG =
    LoggerFactory.getLogger(ProtobufMessageBodyConsumer.class);

  @Override
  public boolean isReadable(Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType) {
    return ProtobufMessageHandler.class.isAssignableFrom(type);
  }

  @Override
  public ProtobufMessageHandler readFrom(Class<ProtobufMessageHandler> type, Type genericType,
      Annotation[] annotations, MediaType mediaType,
      MultivaluedMap<String, String> httpHeaders, InputStream inputStream)
      throws IOException, WebApplicationException {
    ProtobufMessageHandler obj = null;
    try {
      obj = type.getDeclaredConstructor().newInstance();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buffer = new byte[4096];
      int read;
      do {
        read = inputStream.read(buffer, 0, buffer.length);
        if (read > 0) {
          baos.write(buffer, 0, read);
        }
      } while (read > 0);
      if (LOG.isTraceEnabled()) {
        LOG.trace(getClass() + ": read " + baos.size() + " bytes from " +
          inputStream);
      }
      obj = obj.getObjectFromMessage(baos.toByteArray());
    } catch (InstantiationException | NoSuchMethodException | InvocationTargetException
        | IllegalAccessException e) {
      throw new WebApplicationException(e);
    }
    return obj;
  }
}
