/**
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
package org.apache.hbase.archetypes.exemplars.client;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Successful running of this application requires access to an active instance
 * of HBase. For install instructions for a standalone instance of HBase, please
 * refer to https://hbase.apache.org/book.html#quickstart
 */
public final class HelloHBase {

  protected static final String MY_NAMESPACE_NAME = "myTestNamespace";
  static final TableName MY_TABLE_NAME = TableName.valueOf("myTestTable");
  static final byte[] MY_COLUMN_FAMILY_NAME = Bytes.toBytes("cf");
  static final byte[] MY_FIRST_COLUMN_QUALIFIER
          = Bytes.toBytes("myFirstColumn");
  static final byte[] MY_SECOND_COLUMN_QUALIFIER
          = Bytes.toBytes("mySecondColumn");
  static final byte[] MY_ROW_ID = Bytes.toBytes("rowId01");

  // Private constructor included here to avoid checkstyle warnings
  private HelloHBase() {
  }

  public static void main(final String[] args) throws IOException {
    final boolean deleteAllAtEOJ = true;

    /**
     * ConnectionFactory#createConnection() automatically looks for
     * hbase-site.xml (HBase configuration parameters) on the system's
     * CLASSPATH, to enable creation of Connection to HBase via ZooKeeper.
     */
    try (Connection connection = ConnectionFactory.createConnection();
            Admin admin = connection.getAdmin()) {

      admin.getClusterStatus(); // assure connection successfully established
      System.out.println("\n*** Hello HBase! -- Connection has been "
              + "established via ZooKeeper!!\n");

      createNamespaceAndTable(admin);

      System.out.println("Getting a Table object for [" + MY_TABLE_NAME
              + "] with which to perform CRUD operations in HBase.");
      try (Table table = connection.getTable(MY_TABLE_NAME)) {

        putRowToTable(table);
        getAndPrintRowContents(table);

        if (deleteAllAtEOJ) {
          deleteRow(table);
        }
      }

      if (deleteAllAtEOJ) {
        deleteNamespaceAndTable(admin);
      }
    }
  }

  /**
   * Invokes Admin#createNamespace and Admin#createTable to create a namespace
   * with a table that has one column-family.
   *
   * @param admin Standard Admin object
   * @throws IOException If IO problem encountered
   */
  static void createNamespaceAndTable(final Admin admin) throws IOException {

    if (!namespaceExists(admin, MY_NAMESPACE_NAME)) {
      System.out.println("Creating Namespace [" + MY_NAMESPACE_NAME + "].");

      admin.createNamespace(NamespaceDescriptor
              .create(MY_NAMESPACE_NAME).build());
    }
    if (!admin.tableExists(MY_TABLE_NAME)) {
      System.out.println("Creating Table [" + MY_TABLE_NAME.getNameAsString()
              + "], with one Column Family ["
              + Bytes.toString(MY_COLUMN_FAMILY_NAME) + "].");

      admin.createTable(new HTableDescriptor(MY_TABLE_NAME)
              .addFamily(new HColumnDescriptor(MY_COLUMN_FAMILY_NAME)));
    }
  }

  /**
   * Invokes Table#put to store a row (with two new columns created 'on the
   * fly') into the table.
   *
   * @param table Standard Table object (used for CRUD operations).
   * @throws IOException If IO problem encountered
   */
  static void putRowToTable(final Table table) throws IOException {

    table.put(new Put(MY_ROW_ID).addColumn(MY_COLUMN_FAMILY_NAME,
            MY_FIRST_COLUMN_QUALIFIER,
            Bytes.toBytes("Hello")).addColumn(MY_COLUMN_FAMILY_NAME,
                    MY_SECOND_COLUMN_QUALIFIER,
                    Bytes.toBytes("World!")));

    System.out.println("Row [" + Bytes.toString(MY_ROW_ID)
            + "] was put into Table ["
            + table.getName().getNameAsString() + "] in HBase;\n"
            + "  the row's two columns (created 'on the fly') are: ["
            + Bytes.toString(MY_COLUMN_FAMILY_NAME) + ":"
            + Bytes.toString(MY_FIRST_COLUMN_QUALIFIER)
            + "] and [" + Bytes.toString(MY_COLUMN_FAMILY_NAME) + ":"
            + Bytes.toString(MY_SECOND_COLUMN_QUALIFIER) + "]");
  }

  /**
   * Invokes Table#get and prints out the contents of the retrieved row.
   *
   * @param table Standard Table object
   * @throws IOException If IO problem encountered
   */
  static void getAndPrintRowContents(final Table table) throws IOException {

    Result row = table.get(new Get(MY_ROW_ID));

    System.out.println("Row [" + Bytes.toString(row.getRow())
            + "] was retrieved from Table ["
            + table.getName().getNameAsString()
            + "] in HBase, with the following content:");

    for (Entry<byte[], NavigableMap<byte[], byte[]>> colFamilyEntry
            : row.getNoVersionMap().entrySet()) {
      String columnFamilyName = Bytes.toString(colFamilyEntry.getKey());

      System.out.println("  Columns in Column Family [" + columnFamilyName
              + "]:");

      for (Entry<byte[], byte[]> columnNameAndValueMap
              : colFamilyEntry.getValue().entrySet()) {

        System.out.println("    Value of Column [" + columnFamilyName + ":"
                + Bytes.toString(columnNameAndValueMap.getKey()) + "] == "
                + Bytes.toString(columnNameAndValueMap.getValue()));
      }
    }
  }

  /**
   * Checks to see whether a namespace exists.
   *
   * @param admin Standard Admin object
   * @param namespaceName Name of namespace
   * @return true If namespace exists
   * @throws IOException If IO problem encountered
   */
  static boolean namespaceExists(final Admin admin, final String namespaceName)
          throws IOException {
    try {
      admin.getNamespaceDescriptor(namespaceName);
    } catch (NamespaceNotFoundException e) {
      return false;
    }
    return true;
  }

  /**
   * Invokes Table#delete to delete test data (i.e. the row)
   *
   * @param table Standard Table object
   * @throws IOException If IO problem is encountered
   */
  static void deleteRow(final Table table) throws IOException {
    System.out.println("Deleting row [" + Bytes.toString(MY_ROW_ID)
            + "] from Table ["
            + table.getName().getNameAsString() + "].");
    table.delete(new Delete(MY_ROW_ID));
  }

  /**
   * Invokes Admin#disableTable, Admin#deleteTable, and Admin#deleteNamespace to
   * disable/delete Table and delete Namespace.
   *
   * @param admin Standard Admin object
   * @throws IOException If IO problem is encountered
   */
  static void deleteNamespaceAndTable(final Admin admin) throws IOException {
    if (admin.tableExists(MY_TABLE_NAME)) {
      System.out.println("Disabling/deleting Table ["
              + MY_TABLE_NAME.getNameAsString() + "].");
      admin.disableTable(MY_TABLE_NAME); // Disable a table before deleting it.
      admin.deleteTable(MY_TABLE_NAME);
    }
    if (namespaceExists(admin, MY_NAMESPACE_NAME)) {
      System.out.println("Deleting Namespace [" + MY_NAMESPACE_NAME + "].");
      admin.deleteNamespace(MY_NAMESPACE_NAME);
    }
  }
}
