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
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.rest.ProtobufMessageHandler;
import org.apache.hadoop.hbase.rest.protobuf.generated.NamespacesMessage.Namespaces;

import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * A list of HBase namespaces.
 * <ul>
 * <li>Namespace: namespace name</li>
 * </ul>
 */
@XmlRootElement(name="Namespaces")
@XmlAccessorType(XmlAccessType.FIELD)
@InterfaceAudience.Private
public class NamespacesModel implements Serializable, ProtobufMessageHandler {

  private static final long serialVersionUID = 1L;

  @JsonProperty("Namespace")
  @XmlElement(name="Namespace")
  private List<String> namespaces = new ArrayList<>();

  /**
   * Default constructor. Do not use.
   */
  public NamespacesModel() {}

  /**
   * Constructor
   * @param admin the administrative API
   * @throws IOException
   */
  public NamespacesModel(Admin admin) throws IOException {
    NamespaceDescriptor[] nds = admin.listNamespaceDescriptors();
    namespaces = new ArrayList<>(nds.length);
    for (NamespaceDescriptor nd : nds) {
      namespaces.add(nd.getName());
    }
  }

  /**
   * @return all namespaces
   */
  public List<String> getNamespaces() {
    return namespaces;
  }

  /**
   * @param namespaces the namespace name array
   */
  public void setNamespaces(List<String> namespaces) {
    this.namespaces = namespaces;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (String namespace : namespaces) {
      sb.append(namespace);
      sb.append("\n");
    }
    return sb.toString();
  }

  @Override
  public byte[] createProtobufOutput() {
    Namespaces.Builder builder = Namespaces.newBuilder();
    builder.addAllNamespace(namespaces);
    return builder.build().toByteArray();
  }

  @Override
  public ProtobufMessageHandler getObjectFromMessage(byte[] message) throws IOException {
    Namespaces.Builder builder = Namespaces.newBuilder();
    builder.mergeFrom(message);
    namespaces = builder.getNamespaceList();
    return this;
  }
}
