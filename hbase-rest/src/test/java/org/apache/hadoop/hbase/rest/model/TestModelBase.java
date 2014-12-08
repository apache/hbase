/*
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.rest.model;

import junit.framework.TestCase;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.rest.ProtobufMessageHandler;
import org.apache.hadoop.hbase.rest.provider.JAXBContextResolver;
import org.apache.hadoop.hbase.util.Base64;
import org.codehaus.jackson.jaxrs.JacksonJaxbJsonProvider;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.experimental.categories.Category;

import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

@Category(SmallTests.class)
public abstract class TestModelBase<T> extends TestCase {

  protected String AS_XML;

  protected String AS_PB;

  protected String AS_JSON;

  protected JAXBContext context;

  protected Class<?> clazz;

  protected ObjectMapper mapper;

  protected TestModelBase(Class<?> clazz) throws Exception {
    super();
    this.clazz = clazz;
    context = new JAXBContextResolver().getContext(clazz);
    mapper = new JacksonJaxbJsonProvider().locateMapper(clazz,
        MediaType.APPLICATION_JSON_TYPE);
  }

  protected abstract T buildTestModel();

  @SuppressWarnings("unused")
  protected String toXML(T model) throws JAXBException {
    StringWriter writer = new StringWriter();
    context.createMarshaller().marshal(model, writer);
    return writer.toString();
  }

  protected String toJSON(T model) throws JAXBException, IOException {
    StringWriter writer = new StringWriter();
    mapper.writeValue(writer, model);
//  original marshaller, uncomment this and comment mapper to verify backward compatibility
//  ((JSONJAXBContext)context).createJSONMarshaller().marshallToJSON(model, writer);
    return writer.toString();
  }

  public T fromJSON(String json) throws JAXBException, IOException {
    return (T)
      mapper.readValue(json, clazz);
  }

  public T fromXML(String xml) throws JAXBException {
    return (T)
      context.createUnmarshaller().unmarshal(new StringReader(xml));
  }

  @SuppressWarnings("unused")
  protected byte[] toPB(ProtobufMessageHandler model) {
    return model.createProtobufOutput();
  }

  protected T fromPB(String pb) throws
      Exception {
    return (T)clazz.getMethod("getObjectFromMessage", byte[].class).invoke(
        clazz.newInstance(),
        Base64.decode(AS_PB));
  }

  protected abstract  void checkModel(T model);

  public void testBuildModel() throws Exception {
    checkModel(buildTestModel());
  }

  public void testFromPB() throws Exception {
    checkModel(fromPB(AS_PB));
  }

  public void testFromXML() throws Exception {
    checkModel(fromXML(AS_XML));
  }

  public void testToXML() throws Exception {
    assertEquals(AS_XML, toXML(buildTestModel()));
  }

  public void testToJSON() throws Exception {
    try {
      ObjectNode expObj = mapper.readValue(AS_JSON, ObjectNode.class);
      ObjectNode actObj = mapper.readValue(toJSON(buildTestModel()), ObjectNode.class);
      assertEquals(expObj, actObj);
    } catch(Exception e) {
      assertEquals(AS_JSON, toJSON(buildTestModel()));
    }
  }

  public void testFromJSON() throws Exception {
    checkModel(fromJSON(AS_JSON));
  }
}

