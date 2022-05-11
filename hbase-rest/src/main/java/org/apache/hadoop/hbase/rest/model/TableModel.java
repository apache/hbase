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
package org.apache.hadoop.hbase.rest.model;

import java.io.Serializable;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Simple representation of a table name.
 *
 * <pre>
 * &lt;complexType name="Table"&gt;
 *   &lt;sequence&gt;
 *     &lt;element name="name" type="string"&gt;&lt;/element&gt;
 *   &lt;/sequence&gt;
 * &lt;/complexType&gt;
 * </pre>
 */
@XmlRootElement(name = "table")
@InterfaceAudience.Private
public class TableModel implements Serializable {

  private static final long serialVersionUID = 1L;

  private String name;

  /**
   * Default constructor
   */
  public TableModel() {
  }

  /**
   * Constructor n
   */
  public TableModel(String name) {
    super();
    this.name = name;
  }

  /**
   * @return the name
   */
  @XmlAttribute
  public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName(String name) {
    this.name = name;
  }

  /*
   * (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return this.name;
  }
}
