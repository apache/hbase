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
package org.apache.hadoop.hbase.http.gson;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Optional;
import javax.inject.Inject;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.gson.Gson;
import org.apache.hbase.thirdparty.javax.ws.rs.Produces;
import org.apache.hbase.thirdparty.javax.ws.rs.WebApplicationException;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MultivaluedMap;
import org.apache.hbase.thirdparty.javax.ws.rs.ext.MessageBodyWriter;

/**
 * Implements JSON serialization via {@link Gson} for JAX-RS.
 */
@InterfaceAudience.Private
@Produces(MediaType.APPLICATION_JSON)
public final class GsonMessageBodyWriter<T> implements MessageBodyWriter<T> {
  private static final Logger logger = LoggerFactory.getLogger(GsonMessageBodyWriter.class);

  private final Gson gson;

  @Inject
  public GsonMessageBodyWriter(Gson gson) {
    this.gson = gson;
  }

  @Override
  public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations,
    MediaType mediaType) {
    return mediaType == null || MediaType.APPLICATION_JSON_TYPE.isCompatible(mediaType);
  }

  @Override
  public void writeTo(
    T t,
    Class<?> type,
    Type genericType,
    Annotation[] annotations,
    MediaType mediaType,
    MultivaluedMap<String, Object> httpHeaders,
    OutputStream entityStream
  ) throws IOException, WebApplicationException {
    final Charset outputCharset = requestedCharset(mediaType);
    try (Writer writer = new OutputStreamWriter(entityStream, outputCharset)) {
      gson.toJson(t, writer);
    }
  }

  private static Charset requestedCharset(MediaType mediaType) {
    return Optional.ofNullable(mediaType)
      .map(MediaType::getParameters)
      .map(params -> params.get("charset"))
      .map(c -> {
        try {
          return Charset.forName(c);
        } catch (IllegalCharsetNameException e) {
          logger.debug("Client requested illegal Charset '{}'", c);
          return null;
        } catch (UnsupportedCharsetException e) {
          logger.debug("Client requested unsupported Charset '{}'", c);
          return null;
        } catch (Exception e) {
          logger.debug("Error while resolving Charset '{}'", c, e);
          return null;
        }
      })
      .orElse(StandardCharsets.UTF_8);
  }
}
