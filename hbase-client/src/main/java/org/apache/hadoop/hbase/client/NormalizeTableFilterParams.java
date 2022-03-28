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
package org.apache.hadoop.hbase.client;

import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A collection of criteria used for table selection. The logic of table selection is as follows:
 * <ul>
 *   <li>
 *     When no parameter values are provided, an unfiltered list of all user tables is returned.
 *   </li>
 *   <li>
 *     When a list of {@link TableName TableNames} are provided, the filter starts with any of
 *     these tables that exist.
 *   </li>
 *   <li>
 *     When a {@code namespace} name is provided, the filter starts with all the tables present in
 *     that namespace.
 *   </li>
 *   <li>
 *     If both a list of {@link TableName TableNames} and a {@code namespace} name are provided,
 *     the {@link TableName} list is honored and the {@code namespace} name is ignored.
 *   </li>
 *   <li>
 *     If a {@code regex} is provided, this subset of {@link TableName TableNames} is further
 *     reduced to those that match the provided regular expression.
 *   </li>
 * </ul>
 */
@InterfaceAudience.Public
public final class NormalizeTableFilterParams {
  private final List<TableName> tableNames;
  private final String regex;
  private final String namespace;

  private NormalizeTableFilterParams(final List<TableName> tableNames, final String regex,
    final String namespace) {
    this.tableNames = tableNames;
    this.regex = regex;
    this.namespace = namespace;
  }

  public List<TableName> getTableNames() {
    return tableNames;
  }

  public String getRegex() {
    return regex;
  }

  public String getNamespace() {
    return namespace;
  }

  /**
   * Used to instantiate an instance of {@link NormalizeTableFilterParams}.
   */
  public static class Builder {
    private List<TableName> tableNames;
    private String regex;
    private String namespace;

    public Builder tableFilterParams(final NormalizeTableFilterParams ntfp) {
      this.tableNames = ntfp.getTableNames();
      this.regex = ntfp.getRegex();
      this.namespace = ntfp.getNamespace();
      return this;
    }

    public Builder tableNames(final List<TableName> tableNames) {
      this.tableNames = tableNames;
      return this;
    }

    public Builder regex(final String regex) {
      this.regex = regex;
      return this;
    }

    public Builder namespace(final String namespace) {
      this.namespace = namespace;
      return this;
    }

    public NormalizeTableFilterParams build() {
      return new NormalizeTableFilterParams(tableNames, regex, namespace);
    }
  }
}
