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
package org.apache.hadoop.hbase.quotas;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Strings;

/**
 * Filter to use to filter the QuotaRetriever results.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class QuotaFilter {
  private Set<QuotaType> types = new HashSet<QuotaType>();
  private boolean hasFilters = false;
  private String namespaceRegex;
  private String tableRegex;
  private String userRegex;

  public QuotaFilter() {
  }

  /**
   * Set the user filter regex
   * @param regex the user filter
   * @return the quota filter object
   */
  public QuotaFilter setUserFilter(final String regex) {
    this.userRegex = regex;
    hasFilters |= !Strings.isEmpty(regex);
    return this;
  }

  /**
   * Set the table filter regex
   * @param regex the table filter
   * @return the quota filter object
   */
  public QuotaFilter setTableFilter(final String regex) {
    this.tableRegex = regex;
    hasFilters |= !Strings.isEmpty(regex);
    return this;
  }

  /**
   * Set the namespace filter regex
   * @param regex the namespace filter
   * @return the quota filter object
   */
  public QuotaFilter setNamespaceFilter(final String regex) {
    this.namespaceRegex = regex;
    hasFilters |= !Strings.isEmpty(regex);
    return this;
  }

  /**
   * Add a type to the filter list
   * @param type the type to filter on
   * @return the quota filter object
   */
  public QuotaFilter addTypeFilter(final QuotaType type) {
    this.types.add(type);
    hasFilters |= true;
    return this;
  }

  /** @return true if the filter is empty */
  public boolean isNull() {
    return !hasFilters;
  }

  /** @return the QuotaType types that we want to filter one */
  public Set<QuotaType> getTypeFilters() {
    return types;
  }

  /** @return the Namespace filter regex */
  public String getNamespaceFilter() {
    return namespaceRegex;
  }

  /** @return the Table filter regex */
  public String getTableFilter() {
    return tableRegex;
  }

  /** @return the User filter regex */
  public String getUserFilter() {
    return userRegex;
  }
}
