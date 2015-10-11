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

package org.apache.hadoop.hbase.rest.model;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.rest.ProtobufMessageHandler;
import org.apache.hadoop.hbase.rest.protobuf
  .generated.NamespacePropertiesMessage.NamespaceProperties;

/**
 * List a HBase namespace's key/value properties.
 * <ul>
 * <li>NamespaceProperties: outer element</li>
 * <li>properties: sequence property elements</li>
 * <li>entry</li>
 * <li>key: property key</li>
 * <li>value: property value</li>
 * </ul>
 */
@XmlRootElement(name="NamespaceProperties")
@XmlAccessorType(XmlAccessType.FIELD)
@InterfaceAudience.Private
public class NamespacesInstanceModel implements Serializable, ProtobufMessageHandler {

  private static final long serialVersionUID = 1L;

  // JAX-RS automatically converts Map to XMLAnyElement.
  private Map<String,String> properties = null;

  @XmlTransient
  private String namespaceName;

  /**
   * Default constructor. Do not use.
   */
  public NamespacesInstanceModel() {}

  /**
   * Constructor to use if namespace does not exist in HBASE.
   * @param namespaceName the namespace name.
   * @throws IOException
   */
  public NamespacesInstanceModel(String namespaceName) throws IOException {
    this(null, namespaceName);
  }

  /**
   * Constructor
   * @param admin the administrative API
   * @param namespaceName the namespace name.
   * @throws IOException
   */
  public NamespacesInstanceModel(Admin admin, String namespaceName) throws IOException {
    this.namespaceName = namespaceName;
    if(admin == null) { return; }

    NamespaceDescriptor nd = admin.getNamespaceDescriptor(namespaceName);

    // For properly formed JSON, if no properties, field has to be null (not just no elements).
    if(nd.getConfiguration().size() == 0){ return; }

    properties = new HashMap<String,String>();
    properties.putAll(nd.getConfiguration());
  }

  /**
   * Add property to the namespace.
   * @param key attribute name
   * @param value attribute value
   */
  public void addProperty(String key, String value) {
    if(properties == null){
      properties = new HashMap<String,String>();
    }
    properties.put(key, value);
  }

  /**
   * @return The map of uncategorized namespace properties.
   */
  public Map<String,String> getProperties() {
    if(properties == null){
      properties = new HashMap<String,String>();
    }
    return properties;
  }

  public String getNamespaceName(){
    return namespaceName;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{NAME => \'");
    sb.append(namespaceName);
    sb.append("\'");
    if(properties != null){
      for(String key: properties.keySet()){
        sb.append(", ");
        sb.append(key);
        sb.append(" => '");
        sb.append(properties.get(key));
        sb.append("\'");
      }
    }
    sb.append("}");
    return sb.toString();
  }

  @Override
  public byte[] createProtobufOutput() {
    NamespaceProperties.Builder builder = NamespaceProperties.newBuilder();
    if(properties != null){
      for(String key: properties.keySet()){
        NamespaceProperties.Property.Builder property = NamespaceProperties.Property.newBuilder();
        property.setKey(key);
        property.setValue(properties.get(key));
        builder.addProps(property);
      }
    }
    return builder.build().toByteArray();
  }

  @Override
  public ProtobufMessageHandler getObjectFromMessage(byte[] message) throws IOException {
    NamespaceProperties.Builder builder = NamespaceProperties.newBuilder();
    builder.mergeFrom(message);
    List<NamespaceProperties.Property> properties = builder.getPropsList();
    for(NamespaceProperties.Property property: properties){
      addProperty(property.getKey(), property.getValue());
    }
    return this;
  }
}
