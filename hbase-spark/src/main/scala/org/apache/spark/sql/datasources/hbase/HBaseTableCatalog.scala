/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.datasources.hbase

import org.apache.hadoop.hbase.spark.datasources._
import org.apache.hadoop.hbase.spark.hbase._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.util.DataTypeParser
import org.apache.spark.sql.types._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable

// Due the access issue defined in spark, we have to locate the file in this package.
// The definition of each column cell, which may be composite type
// TODO: add avro support
case class Field(
    colName: String,
    cf: String,
    col: String,
    sType: Option[String] = None,
    avroSchema: Option[String] = None,
    serdes: Option[SerDes]= None,
    len: Int = -1) extends Logging {
  override def toString = s"$colName $cf $col"
  val isRowKey = cf == HBaseTableCatalog.rowKey
  var start: Int = _

  def cfBytes: Array[Byte] = {
    if (isRowKey) {
      Bytes.toBytes("")
    } else {
      Bytes.toBytes(cf)
    }
  }
  def colBytes: Array[Byte] = {
    if (isRowKey) {
      Bytes.toBytes("key")
    } else {
      Bytes.toBytes(col)
    }
  }

  val dt = {
    sType.map(DataTypeParser.parse(_)).get
  }

  var length: Int = {
    if (len == -1) {
      dt match {
        case BinaryType | StringType => -1
        case BooleanType => Bytes.SIZEOF_BOOLEAN
        case ByteType => 1
        case DoubleType => Bytes.SIZEOF_DOUBLE
        case FloatType => Bytes.SIZEOF_FLOAT
        case IntegerType => Bytes.SIZEOF_INT
        case LongType => Bytes.SIZEOF_LONG
        case ShortType => Bytes.SIZEOF_SHORT
        case _ => -1
      }
    } else {
      len
    }

  }

  override def equals(other: Any): Boolean = other match {
    case that: Field =>
      colName == that.colName && cf == that.cf && col == that.col
    case _ => false
  }
}

// The row key definition, with each key refer to the col defined in Field, e.g.,
// key1:key2:key3
case class RowKey(k: String) {
  val keys = k.split(":")
  var fields: Seq[Field] = _
  var varLength = false
  def length = {
    if (varLength) {
      -1
    } else {
      fields.foldLeft(0){case (x, y) =>
        x + y.length
      }
    }
  }
}
// The map between the column presented to Spark and the HBase field
case class SchemaMap(map: mutable.HashMap[String, Field]) {
  def toFields = map.map { case (name, field) =>
    StructField(name, field.dt)
  }.toSeq

  def fields = map.values

  def getField(name: String) = map(name)
}


// The definition of HBase and Relation relation schema
case class HBaseTableCatalog(
     namespace: String,
     name: String,
     row: RowKey,
     sMap: SchemaMap,
     @transient params: Map[String, String]) extends Logging {
  def toDataType = StructType(sMap.toFields)
  def getField(name: String) = sMap.getField(name)
  def getRowKey: Seq[Field] = row.fields
  def getPrimaryKey= row.keys(0)
  def getColumnFamilies = {
    sMap.fields.map(_.cf).filter(_ != HBaseTableCatalog.rowKey)
  }

  def get(key: String) = params.get(key)

  // Setup the start and length for each dimension of row key at runtime.
  def dynSetupRowKey(rowKey: HBaseType) {
    logDebug(s"length: ${rowKey.length}")
    if(row.varLength) {
      var start = 0
      row.fields.foreach { f =>
        logDebug(s"start: $start")
        f.start = start
        f.length = {
          // If the length is not defined
          if (f.length == -1) {
            f.dt match {
              case StringType =>
                var pos = rowKey.indexOf(HBaseTableCatalog.delimiter, start)
                if (pos == -1 || pos > rowKey.length) {
                  // this is at the last dimension
                  pos = rowKey.length
                }
                pos - start
              // We don't know the length, assume it extend to the end of the rowkey.
              case _ => rowKey.length - start
            }
          } else {
            f.length
          }
        }
        start += f.length
      }
    }
  }

  def initRowKey = {
    val fields = sMap.fields.filter(_.cf == HBaseTableCatalog.rowKey)
    row.fields = row.keys.flatMap(n => fields.find(_.col == n))
    // The length is determined at run time if it is string or binary and the length is undefined.
    if (row.fields.filter(_.length == -1).isEmpty) {
      var start = 0
      row.fields.foreach { f =>
        f.start = start
        start += f.length
      }
    } else {
      row.varLength = true
    }
  }
  initRowKey
}

object HBaseTableCatalog {
  // If defined and larger than 3, a new table will be created with the nubmer of region specified.
  val newTable = "newtable"
  // The json string specifying hbase catalog information
  val regionStart = "regionStart"
  val defaultRegionStart = "aaaaaaa"
  val regionEnd = "regionEnd"
  val defaultRegionEnd = "zzzzzzz"
  val tableCatalog = "catalog"
  // The row key with format key1:key2 specifying table row key
  val rowKey = "rowkey"
  // The key for hbase table whose value specify namespace and table name
  val table = "table"
  // The namespace of hbase table
  val nameSpace = "namespace"
  // The name of hbase table
  val tableName = "name"
  // The name of columns in hbase catalog
  val columns = "columns"
  val cf = "cf"
  val col = "col"
  val `type` = "type"
  // the name of avro schema json string
  val avro = "avro"
  val delimiter: Byte = 0
  val serdes = "serdes"
  val length = "length"

  /**
    * User provide table schema definition
    * {"tablename":"name", "rowkey":"key1:key2",
    * "columns":{"col1":{"cf":"cf1", "col":"col1", "type":"type1"},
    * "col2":{"cf":"cf2", "col":"col2", "type":"type2"}}}
    * Note that any col in the rowKey, there has to be one corresponding col defined in columns
    */
  def apply(params: Map[String, String]): HBaseTableCatalog = {
    val parameters = convert(params)
    //  println(jString)
    val jString = parameters(tableCatalog)
    val map = parse(jString).values.asInstanceOf[Map[String, _]]
    val tableMeta = map.get(table).get.asInstanceOf[Map[String, _]]
    val nSpace = tableMeta.get(nameSpace).getOrElse("default").asInstanceOf[String]
    val tName = tableMeta.get(tableName).get.asInstanceOf[String]
    val cIter = map.get(columns).get.asInstanceOf[Map[String, Map[String, String]]].toIterator
    val schemaMap = mutable.HashMap.empty[String, Field]
    cIter.foreach { case (name, column) =>
      val sd = {
        column.get(serdes).asInstanceOf[Option[String]].map(n =>
          Class.forName(n).newInstance().asInstanceOf[SerDes]
        )
      }
      val len = column.get(length).map(_.toInt).getOrElse(-1)
      val sAvro = column.get(avro).map(parameters(_))
      val f = Field(name, column.getOrElse(cf, rowKey),
        column.get(col).get,
        column.get(`type`),
        sAvro, sd, len)
      schemaMap.+=((name, f))
    }
    val rKey = RowKey(map.get(rowKey).get.asInstanceOf[String])
    HBaseTableCatalog(nSpace, tName, rKey, SchemaMap(schemaMap), parameters)
  }

  val TABLE_KEY: String = "hbase.table"
  val SCHEMA_COLUMNS_MAPPING_KEY: String = "hbase.columns.mapping"

  /* for backward compatibility. Convert the old definition to new json based definition formated as below
    val catalog = s"""{
                      |"table":{"namespace":"default", "name":"htable"},
                      |"rowkey":"key1:key2",
                      |"columns":{
                      |"col1":{"cf":"rowkey", "col":"key1", "type":"string"},
                      |"col2":{"cf":"rowkey", "col":"key2", "type":"double"},
                      |"col3":{"cf":"cf1", "col":"col2", "type":"binary"},
                      |"col4":{"cf":"cf1", "col":"col3", "type":"timestamp"},
                      |"col5":{"cf":"cf1", "col":"col4", "type":"double", "serdes":"${classOf[DoubleSerDes].getName}"},
                      |"col6":{"cf":"cf1", "col":"col5", "type":"$map"},
                      |"col7":{"cf":"cf1", "col":"col6", "type":"$array"},
                      |"col8":{"cf":"cf1", "col":"col7", "type":"$arrayMap"}
                      |}
                      |}""".stripMargin
   */
  @deprecated("Please use new json format to define HBaseCatalog")
  def convert(parameters: Map[String, String]): Map[String, String] = {
    val tableName = parameters.get(TABLE_KEY).getOrElse(null)
    // if the hbase.table is not defined, we assume it is json format already.
    if (tableName == null) return parameters
    val schemaMappingString = parameters.getOrElse(SCHEMA_COLUMNS_MAPPING_KEY, "")
    import scala.collection.JavaConverters._
    val schemaMap = generateSchemaMappingMap(schemaMappingString).asScala.map(_._2.asInstanceOf[SchemaQualifierDefinition])

    val rowkey = schemaMap.filter {
      _.columnFamily == "rowkey"
    }.map(_.columnName)
    val cols = schemaMap.map { x =>
      s""""${x.columnName}":{"cf":"${x.columnFamily}", "col":"${x.qualifier}", "type":"${x.colType}"}""".stripMargin
    }
    val jsonCatalog =
      s"""{
         |"table":{"namespace":"default", "name":"${tableName}"},
         |"rowkey":"${rowkey.mkString(":")}",
         |"columns":{
         |${cols.mkString(",")}
         |}
         |}
       """.stripMargin
    parameters ++ Map(HBaseTableCatalog.tableCatalog->jsonCatalog)
  }

  /**
    * Reads the SCHEMA_COLUMNS_MAPPING_KEY and converts it to a map of
    * SchemaQualifierDefinitions with the original sql column name as the key
    *
    * @param schemaMappingString The schema mapping string from the SparkSQL map
    * @return                    A map of definitions keyed by the SparkSQL column name
    */
  def generateSchemaMappingMap(schemaMappingString:String):
  java.util.HashMap[String, SchemaQualifierDefinition] = {
    println(schemaMappingString)
    try {
      val columnDefinitions = schemaMappingString.split(',')
      val resultingMap = new java.util.HashMap[String, SchemaQualifierDefinition]()
      columnDefinitions.map(cd => {
        val parts = cd.trim.split(' ')

        //Make sure we get three parts
        //<ColumnName> <ColumnType> <ColumnFamily:Qualifier>
        if (parts.length == 3) {
          val hbaseDefinitionParts = if (parts(2).charAt(0) == ':') {
            Array[String]("rowkey", parts(0))
          } else {
            parts(2).split(':')
          }
          resultingMap.put(parts(0), new SchemaQualifierDefinition(parts(0),
            parts(1), hbaseDefinitionParts(0), hbaseDefinitionParts(1)))
        } else {
          throw new IllegalArgumentException("Invalid value for schema mapping '" + cd +
            "' should be '<columnName> <columnType> <columnFamily>:<qualifier>' " +
            "for columns and '<columnName> <columnType> :<qualifier>' for rowKeys")
        }
      })
      resultingMap
    } catch {
      case e:Exception => throw
        new IllegalArgumentException("Invalid value for " + SCHEMA_COLUMNS_MAPPING_KEY +
          " '" +
          schemaMappingString + "'", e )
    }
  }
}

/**
  * Construct to contains column data that spend SparkSQL and HBase
  *
  * @param columnName   SparkSQL column name
  * @param colType      SparkSQL column type
  * @param columnFamily HBase column family
  * @param qualifier    HBase qualifier name
  */
case class SchemaQualifierDefinition(columnName:String,
    colType:String,
    columnFamily:String,
    qualifier:String)
