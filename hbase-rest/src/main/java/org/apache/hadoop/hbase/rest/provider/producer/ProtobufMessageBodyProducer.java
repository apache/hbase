/*
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

package org.apache.hadoop.hbase.rest.provider.producer;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import org.apache.hadoop.hbase.rest.Constants;
import org.apache.hadoop.hbase.rest.ProtobufMessageHandler;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An adapter between Jersey and ProtobufMessageHandler implementors. Hooks up
 * protobuf output producing methods to the Jersey content handling framework.
 * Jersey will first call getSize() to learn the number of bytes that will be
 * sent, then writeTo to perform the actual I/O.
 */
@Provider
@Produces({Constants.MIMETYPE_PROTOBUF, Constants.MIMETYPE_PROTOBUF_IETF})
@InterfaceAudience.Private
public class ProtobufMessageBodyProducer
  implements MessageBodyWriter<ProtobufMessageHandler> {

  @Override
  public boolean isWriteable(Class<?> type, Type genericType, 
      Annotation[] annotations, MediaType mediaType) {
    return ProtobufMessageHandler.class.isAssignableFrom(type);
  }

  @Override
  public long getSize(ProtobufMessageHandler m, Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType) {
    // deprecated by JAX-RS 2.0 and ignored by Jersey runtime
    return -1;
  }

  @Override
  public void writeTo(ProtobufMessageHandler m, Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType, 
      MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) 
      throws IOException, WebApplicationException {
    entityStream.write(m.createProtobufOutput());
  }
}
