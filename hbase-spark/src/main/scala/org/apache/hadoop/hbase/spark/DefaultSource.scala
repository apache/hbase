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

package org.apache.hadoop.hbase.spark

import java.util
import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Result, Scan}
import org.apache.hadoop.hbase.types._
import org.apache.hadoop.hbase.util.{SimplePositionedMutableByteRange, PositionedByteRange, Bytes}
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
 * DefaultSource for integration with Spark's dataframe datasources.
 * This class will produce a relationProvider based on input given to it from spark
 *
 * In all this DefaultSource support the following datasource functionality
 * - Scan range pruning through filter push down logic based on rowKeys
 * - Filter push down logic on HBase Cells
 * - Qualifier filtering based on columns used in the SparkSQL statement
 * - Type conversions of basic SQL types.  All conversions will be
 *   Through the HBase Bytes object commands.
 */
class DefaultSource extends RelationProvider {

  val TABLE_KEY:String = "hbase.table"
  val SCHEMA_COLUMNS_MAPPING_KEY:String = "hbase.columns.mapping"
  val BATCHING_NUM_KEY:String = "hbase.batching.num"
  val CACHING_NUM_KEY:String = "hbase.caching.num"
  val HBASE_CONFIG_RESOURCES_LOCATIONS:String = "hbase.config.resources"
  val USE_HBASE_CONTEXT:String = "hbase.use.hbase.context"

  /**
   * Is given input from SparkSQL to construct a BaseRelation
   * @param sqlContext SparkSQL context
   * @param parameters Parameters given to us from SparkSQL
   * @return           A BaseRelation Object
   */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]):
  BaseRelation = {


    val tableName = parameters.get(TABLE_KEY)
    if (tableName.isEmpty)
      new IllegalArgumentException("Invalid value for " + TABLE_KEY +" '" + tableName + "'")

    val schemaMappingString = parameters.getOrElse(SCHEMA_COLUMNS_MAPPING_KEY, "")
    val batchingNumStr = parameters.getOrElse(BATCHING_NUM_KEY, "1000")
    val cachingNumStr = parameters.getOrElse(CACHING_NUM_KEY, "1000")
    val hbaseConfigResources = parameters.getOrElse(HBASE_CONFIG_RESOURCES_LOCATIONS, "")
    val useHBaseReources = parameters.getOrElse(USE_HBASE_CONTEXT, "true")

    val batchingNum:Int = try {
      batchingNumStr.toInt
    } catch {
      case e:NumberFormatException => throw
        new IllegalArgumentException("Invalid value for " + BATCHING_NUM_KEY +" '"
            + batchingNumStr + "'", e)
    }

    val cachingNum:Int = try {
      cachingNumStr.toInt
    } catch {
      case e:NumberFormatException => throw
        new IllegalArgumentException("Invalid value for " + CACHING_NUM_KEY +" '"
            + cachingNumStr + "'", e)
    }

    new HBaseRelation(tableName.get,
      generateSchemaMappingMap(schemaMappingString),
      batchingNum.toInt,
      cachingNum.toInt,
      hbaseConfigResources,
      useHBaseReources.equalsIgnoreCase("true"))(sqlContext)
  }

  /**
   * Reads the SCHEMA_COLUMNS_MAPPING_KEY and converts it to a map of
   * SchemaQualifierDefinitions with the original sql column name as the key
   * @param schemaMappingString The schema mapping string from the SparkSQL map
   * @return                    A map of definitions keyed by the SparkSQL column name
   */
  def generateSchemaMappingMap(schemaMappingString:String):
  java.util.HashMap[String, SchemaQualifierDefinition] = {
    try {
      val columnDefinitions = schemaMappingString.split(',')
      val resultingMap = new java.util.HashMap[String, SchemaQualifierDefinition]()
      columnDefinitions.map(cd => {
        val parts = cd.trim.split(' ')

        //Make sure we get three parts
        //<ColumnName> <ColumnType> <ColumnFamily:Qualifier>
        if (parts.length == 3) {
          val hbaseDefinitionParts = if (parts(2).charAt(0) == ':') {
            Array[String]("", "key")
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
          " '" + schemaMappingString + "'", e )
    }
  }
}

/**
 * Implementation of Spark BaseRelation that will build up our scan logic
 * , do the scan pruning, filter push down, and value conversions
 *
 * @param tableName               HBase table that we plan to read from
 * @param schemaMappingDefinition SchemaMapping information to map HBase
 *                                Qualifiers to SparkSQL columns
 * @param batchingNum             The batching number to be applied to the
 *                                scan object
 * @param cachingNum              The caching number to be applied to the
 *                                scan object
 * @param configResources         Optional comma separated list of config resources
 *                                to get based on their URI
 * @param useHBaseContext         If true this will look to see if
 *                                HBaseContext.latest is populated to use that
 *                                connection information
 * @param sqlContext              SparkSQL context
 */
class HBaseRelation (val tableName:String,
                     val schemaMappingDefinition:
                     java.util.HashMap[String, SchemaQualifierDefinition],
                     val batchingNum:Int,
                     val cachingNum:Int,
                     val configResources:String,
                     val useHBaseContext:Boolean) (
  @transient val sqlContext:SQLContext)
  extends BaseRelation with PrunedFilteredScan with Logging {

  //create or get latest HBaseContext
  @transient val hbaseContext:HBaseContext = if (useHBaseContext) {
    LatestHBaseContextCache.latest
  } else {
    val config = HBaseConfiguration.create()
    configResources.split(",").foreach( r => config.addResource(r))
    new HBaseContext(sqlContext.sparkContext, config)
  }

  /**
   * Generates a Spark SQL schema object so Spark SQL knows what is being
   * provided by this BaseRelation
   *
   * @return schema generated from the SCHEMA_COLUMNS_MAPPING_KEY value
   */
  override def schema: StructType = {

    val metadataBuilder = new MetadataBuilder()

    val structFieldArray = new Array[StructField](schemaMappingDefinition.size())

    val schemaMappingDefinitionIt = schemaMappingDefinition.values().iterator()
    var indexCounter = 0
    while (schemaMappingDefinitionIt.hasNext) {
      val c = schemaMappingDefinitionIt.next()

      val metadata = metadataBuilder.putString("name", c.columnName).build()
      val structField =
        new StructField(c.columnName, c.columnSparkSqlType, nullable = true, metadata)

      structFieldArray(indexCounter) = structField
      indexCounter += 1
    }

    val result = new StructType(structFieldArray)
    result
  }

  /**
   * Here we are building the functionality to populate the resulting RDD[Row]
   * Here is where we will do the following:
   * - Filter push down
   * - Scan or GetList pruning
   * - Executing our scan(s) or/and GetList to generate result
   *
   * @param requiredColumns The columns that are being requested by the requesting query
   * @param filters         The filters that are being applied by the requesting query
   * @return                RDD will all the results from HBase needed for SparkSQL to
   *                        execute the query on
   */
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    val columnFilterCollection = buildColumnFilterCollection(filters)

    val requiredQualifierDefinitionArray = new mutable.MutableList[SchemaQualifierDefinition]
    requiredColumns.foreach( c => {
      val definition = schemaMappingDefinition.get(c)
      if (definition.columnFamilyBytes.length > 0) {
        requiredQualifierDefinitionArray += definition
      }
    })

    //Create a local variable so that scala doesn't have to
    // serialize the whole HBaseRelation Object
    val serializableDefinitionMap = schemaMappingDefinition


    //retain the information for unit testing checks
    DefaultSourceStaticUtils.populateLatestExecutionRules(columnFilterCollection,
      requiredQualifierDefinitionArray)
    var resultRDD: RDD[Row] = null

    if (columnFilterCollection != null) {
      val pushDownFilterJava =
        new SparkSQLPushDownFilter(
          columnFilterCollection.generateFamilyQualifiterFilterMap(schemaMappingDefinition))

      val getList = new util.ArrayList[Get]()
      val rddList = new util.ArrayList[RDD[Row]]()

      val it = columnFilterCollection.columnFilterMap.iterator
      while (it.hasNext) {
        val e = it.next()
        val columnDefinition = schemaMappingDefinition.get(e._1)
        //check is a rowKey
        if (columnDefinition != null && columnDefinition.columnFamily.isEmpty) {
          //add points to getList
          e._2.points.foreach(p => {
            val get = new Get(p)
            requiredQualifierDefinitionArray.foreach( d =>
              get.addColumn(d.columnFamilyBytes, d.qualifierBytes))
            getList.add(get)
          })

          val rangeIt = e._2.ranges.iterator

          while (rangeIt.hasNext) {
            val r = rangeIt.next()

            val scan = new Scan()
            scan.setBatch(batchingNum)
            scan.setCaching(cachingNum)
            requiredQualifierDefinitionArray.foreach( d =>
              scan.addColumn(d.columnFamilyBytes, d.qualifierBytes))

            if (pushDownFilterJava.columnFamilyQualifierFilterMap.size() > 0) {
              scan.setFilter(pushDownFilterJava)
            }

            //Check if there is a lower bound
            if (r.lowerBound != null && r.lowerBound.length > 0) {

              if (r.isLowerBoundEqualTo) {
                //HBase startRow is inclusive: Therefore it acts like  isLowerBoundEqualTo
                // by default
                scan.setStartRow(r.lowerBound)
              } else {
                //Since we don't equalTo we want the next value we need
                // to add another byte to the start key.  That new byte will be
                // the min byte value.
                val newArray = new Array[Byte](r.lowerBound.length + 1)
                System.arraycopy(r.lowerBound, 0, newArray, 0, r.lowerBound.length)

                //new Min Byte
                newArray(r.lowerBound.length) = Byte.MinValue
                scan.setStartRow(newArray)
              }
            }

            //Check if there is a upperBound
            if (r.upperBound != null && r.upperBound.length > 0) {
              if (r.isUpperBoundEqualTo) {
                //HBase stopRow is exclusive: therefore it DOESN'T ast like isUpperBoundEqualTo
                // by default.  So we need to add a new max byte to the stopRow key
                val newArray = new Array[Byte](r.upperBound.length + 1)
                System.arraycopy(r.upperBound, 0, newArray, 0, r.upperBound.length)

                //New Max Bytes
                newArray(r.upperBound.length) = Byte.MaxValue

                scan.setStopRow(newArray)
              } else {
                //Here equalTo is false for Upper bound which is exclusive and
                // HBase stopRow acts like that by default so no need to mutate the
                // rowKey
                scan.setStopRow(r.upperBound)
              }
            }

            val rdd = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan).map(r => {
              Row.fromSeq(requiredColumns.map(c =>
                DefaultSourceStaticUtils.getValue(c, serializableDefinitionMap, r._2)))
            })
            rddList.add(rdd)
          }
        }
      }

      //If there is more then one RDD then we have to union them together
      for (i <- 0 until rddList.size()) {
        if (resultRDD == null) resultRDD = rddList.get(i)
        else resultRDD = resultRDD.union(rddList.get(i))

      }

      //If there are gets then we can get them from the driver and union that rdd in
      // with the rest of the values.
      if (getList.size() > 0) {
        val connection = ConnectionFactory.createConnection(hbaseContext.tmpHdfsConfiguration)
        try {
          val table = connection.getTable(TableName.valueOf(tableName))
          try {
            val results = table.get(getList)
            val rowList = mutable.MutableList[Row]()
            for (i <- 0 until results.length) {
              val rowArray = requiredColumns.map(c =>
                DefaultSourceStaticUtils.getValue(c, schemaMappingDefinition, results(i)))
              rowList += Row.fromSeq(rowArray)
            }
            val getRDD = sqlContext.sparkContext.parallelize(rowList)
            if (resultRDD == null) resultRDD = getRDD
            else {
              resultRDD = resultRDD.union(getRDD)
            }
          } finally {
            table.close()
          }
        } finally {
          connection.close()
        }
      }
    }
    if (resultRDD == null) {
      val scan = new Scan()
      scan.setBatch(batchingNum)
      scan.setCaching(cachingNum)
      requiredQualifierDefinitionArray.foreach( d =>
        scan.addColumn(d.columnFamilyBytes, d.qualifierBytes))

      val rdd = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan).map(r => {
        Row.fromSeq(requiredColumns.map(c => DefaultSourceStaticUtils.getValue(c,
          serializableDefinitionMap, r._2)))
      })
      resultRDD=rdd
    }
    resultRDD
  }

  /**
   * Root recursive function that will loop over the filters provided by
   * SparkSQL.  Some filters are AND or OR functions and contain additional filters
   * hence the need for recursion.
   *
   * @param filters Filters provided by SparkSQL.
   *                Filters are joined with the AND operater
   * @return        A ColumnFilterCollection whish is a consolidated construct to
   *                hold the high level filter information
   */
  def buildColumnFilterCollection(filters: Array[Filter]): ColumnFilterCollection = {
    var superCollection: ColumnFilterCollection = null

    filters.foreach( f => {
      val parentCollection = new ColumnFilterCollection
      buildColumnFilterCollection(parentCollection, f)
      if (superCollection == null)
        superCollection = parentCollection
      else
        superCollection.mergeIntersect(parentCollection)
    })
    superCollection
  }

  /**
   * Recursive function that will work to convert Spark Filter
   * objects to ColumnFilterCollection
   *
   * @param parentFilterCollection Parent ColumnFilterCollection
   * @param filter                 Current given filter from SparkSQL
   */
  def buildColumnFilterCollection(parentFilterCollection:ColumnFilterCollection,
                                  filter:Filter): Unit = {
    filter match {

      case EqualTo(attr, value) =>
        parentFilterCollection.mergeUnion(attr,
          new ColumnFilter(DefaultSourceStaticUtils.getByteValue(attr,
            schemaMappingDefinition, value.toString)))

      case LessThan(attr, value) =>
        parentFilterCollection.mergeUnion(attr, new ColumnFilter(null,
          new ScanRange(DefaultSourceStaticUtils.getByteValue(attr,
            schemaMappingDefinition, value.toString), false,
            new Array[Byte](0), true)))

      case GreaterThan(attr, value) =>
        parentFilterCollection.mergeUnion(attr, new ColumnFilter(null,
        new ScanRange(null, true, DefaultSourceStaticUtils.getByteValue(attr,
          schemaMappingDefinition, value.toString), false)))

      case LessThanOrEqual(attr, value) =>
        parentFilterCollection.mergeUnion(attr, new ColumnFilter(null,
        new ScanRange(DefaultSourceStaticUtils.getByteValue(attr,
          schemaMappingDefinition, value.toString), true,
          new Array[Byte](0), true)))

      case GreaterThanOrEqual(attr, value) =>
        parentFilterCollection.mergeUnion(attr, new ColumnFilter(null,
        new ScanRange(null, true, DefaultSourceStaticUtils.getByteValue(attr,
          schemaMappingDefinition, value.toString), true)))

      case Or(left, right) =>
        buildColumnFilterCollection(parentFilterCollection, left)
        val rightSideCollection = new ColumnFilterCollection
        buildColumnFilterCollection(rightSideCollection, right)
        parentFilterCollection.mergeUnion(rightSideCollection)
      case And(left, right) =>
        buildColumnFilterCollection(parentFilterCollection, left)
        val rightSideCollection = new ColumnFilterCollection
        buildColumnFilterCollection(rightSideCollection, right)
        parentFilterCollection.mergeIntersect(rightSideCollection)
      case _ => //nothing
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
                          qualifier:String) extends Serializable {
  val columnFamilyBytes = Bytes.toBytes(columnFamily)
  val qualifierBytes = Bytes.toBytes(qualifier)
  val columnSparkSqlType:DataType = if (colType.equals("BOOLEAN")) BooleanType
    else if (colType.equals("TINYINT")) IntegerType
    else if (colType.equals("INT")) IntegerType
    else if (colType.equals("BIGINT")) LongType
    else if (colType.equals("FLOAT")) FloatType
    else if (colType.equals("DOUBLE")) DoubleType
    else if (colType.equals("STRING")) StringType
    else if (colType.equals("TIMESTAMP")) TimestampType
    else if (colType.equals("DECIMAL")) StringType //DataTypes.createDecimalType(precision, scale)
    else throw new IllegalArgumentException("Unsupported column type :" + colType)
}

/**
 * Construct to contain a single scan ranges information.  Also
 * provide functions to merge with other scan ranges through AND
 * or OR operators
 *
 * @param upperBound          Upper bound of scan
 * @param isUpperBoundEqualTo Include upper bound value in the results
 * @param lowerBound          Lower bound of scan
 * @param isLowerBoundEqualTo Include lower bound value in the results
 */
class ScanRange(var upperBound:Array[Byte], var isUpperBoundEqualTo:Boolean,
                var lowerBound:Array[Byte], var isLowerBoundEqualTo:Boolean)
  extends Serializable {

  /**
   * Function to merge another scan object through a AND operation
   * @param other Other scan object
   */
  def mergeIntersect(other:ScanRange): Unit = {
    val upperBoundCompare = compareRange(upperBound, other.upperBound)
    val lowerBoundCompare = compareRange(lowerBound, other.lowerBound)

    upperBound = if (upperBoundCompare <0) upperBound else other.upperBound
    lowerBound = if (lowerBoundCompare >0) lowerBound else other.lowerBound

    isLowerBoundEqualTo = if (lowerBoundCompare == 0)
      isLowerBoundEqualTo && other.isLowerBoundEqualTo
    else isLowerBoundEqualTo

    isUpperBoundEqualTo = if (upperBoundCompare == 0)
      isUpperBoundEqualTo && other.isUpperBoundEqualTo
    else isUpperBoundEqualTo
  }

  /**
   * Function to merge another scan object through a OR operation
   * @param other Other scan object
   */
  def mergeUnion(other:ScanRange): Unit = {

    val upperBoundCompare = compareRange(upperBound, other.upperBound)
    val lowerBoundCompare = compareRange(lowerBound, other.lowerBound)

    upperBound = if (upperBoundCompare >0) upperBound else other.upperBound
    lowerBound = if (lowerBoundCompare <0) lowerBound else other.lowerBound

    isLowerBoundEqualTo = if (lowerBoundCompare == 0)
      isLowerBoundEqualTo || other.isLowerBoundEqualTo
    else isLowerBoundEqualTo

    isUpperBoundEqualTo = if (upperBoundCompare == 0)
      isUpperBoundEqualTo || other.isUpperBoundEqualTo
    else isUpperBoundEqualTo
  }

  /**
   * Common function to see if this scan over laps with another
   *
   * Reference Visual
   *
   * A                           B
   * |---------------------------|
   *   LL--------------LU
   *        RL--------------RU
   *
   * A = lowest value is byte[0]
   * B = highest value is null
   * LL = Left Lower Bound
   * LU = Left Upper Bound
   * RL = Right Lower Bound
   * RU = Right Upper Bound
   *
   * @param other Other scan object
   * @return      True is overlap false is not overlap
   */
  def doesOverLap(other:ScanRange): Boolean = {

    var leftRange:ScanRange = null
    var rightRange:ScanRange = null

    //First identify the Left range
    // Also lower bound can't be null
    if (Bytes.compareTo(lowerBound, other.lowerBound) <=0) {
      leftRange = this
      rightRange = other
    } else {
      leftRange = other
      rightRange = this
    }

    //Then see if leftRange goes to null or if leftRange.upperBound
    // upper is greater or equals to rightRange.lowerBound
    leftRange.upperBound == null ||
      Bytes.compareTo(leftRange.upperBound, rightRange.lowerBound) >= 0
  }

  /**
   * Special compare logic because we can have null values
   * for left or right bound
   *
   * @param left  Left byte array
   * @param right Right byte array
   * @return      0 for equals 1 is left is greater and -1 is right is greater
   */
  def compareRange(left:Array[Byte], right:Array[Byte]): Int = {
    if (left == null && right == null) 0
    else if (left == null && right != null) 1
    else if (left != null && right == null) -1
    else Bytes.compareTo(left, right)
  }
  override def toString:String = {
    "ScanRange:(" + Bytes.toString(upperBound) + "," + isUpperBoundEqualTo + "," +
      Bytes.toString(lowerBound) + "," + isLowerBoundEqualTo + ")"
  }
}

/**
 * Contains information related to a filters for a given column.
 * This can contain many ranges or points.
 *
 * @param currentPoint the initial point when the filter is created
 * @param currentRange the initial scanRange when the filter is created
 */
class ColumnFilter (currentPoint:Array[Byte] = null,
                     currentRange:ScanRange = null,
                     var points:mutable.MutableList[Array[Byte]] =
                     new mutable.MutableList[Array[Byte]](),
                     var ranges:mutable.MutableList[ScanRange] =
                     new mutable.MutableList[ScanRange]() ) extends Serializable {
  //Collection of ranges
  if (currentRange != null ) ranges.+=(currentRange)

  //Collection of points
  if (currentPoint != null) points.+=(currentPoint)

  /**
   * This will validate a give value through the filter's points and/or ranges
   * the result will be if the value passed the filter
   *
   * @param value       Value to be validated
   * @param valueOffSet The offset of the value
   * @param valueLength The length of the value
   * @return            True is the value passes the filter false if not
   */
  def validate(value:Array[Byte], valueOffSet:Int, valueLength:Int):Boolean = {
    var result = false

    points.foreach( p => {
      if (Bytes.equals(p, 0, p.length, value, valueOffSet, valueLength)) {
        result = true
      }
    })

    ranges.foreach( r => {
      val upperBoundPass = r.upperBound == null ||
        (r.isUpperBoundEqualTo &&
          Bytes.compareTo(r.upperBound, 0, r.upperBound.length,
            value, valueOffSet, valueLength) >= 0) ||
        (!r.isUpperBoundEqualTo &&
          Bytes.compareTo(r.upperBound, 0, r.upperBound.length,
            value, valueOffSet, valueLength) > 0)

      val lowerBoundPass = r.lowerBound == null || r.lowerBound.length == 0
        (r.isLowerBoundEqualTo &&
          Bytes.compareTo(r.lowerBound, 0, r.lowerBound.length,
            value, valueOffSet, valueLength) <= 0) ||
        (!r.isLowerBoundEqualTo &&
          Bytes.compareTo(r.lowerBound, 0, r.lowerBound.length,
            value, valueOffSet, valueLength) < 0)

      result = result || (upperBoundPass && lowerBoundPass)
    })
    result
  }

  /**
   * This will allow us to merge filter logic that is joined to the existing filter
   * through a OR operator
   *
   * @param other Filter to merge
   */
  def mergeUnion(other:ColumnFilter): Unit = {
    other.points.foreach( p => points += p)

    other.ranges.foreach( otherR => {
      var doesOverLap = false
      ranges.foreach{ r =>
        if (r.doesOverLap(otherR)) {
          r.mergeUnion(otherR)
          doesOverLap = true
        }}
      if (!doesOverLap) ranges.+=(otherR)
    })
  }

  /**
   * This will allow us to merge filter logic that is joined to the existing filter
   * through a AND operator
   *
   * @param other Filter to merge
   */
  def mergeIntersect(other:ColumnFilter): Unit = {
    val survivingPoints = new mutable.MutableList[Array[Byte]]()
    points.foreach( p => {
      other.points.foreach( otherP => {
        if (Bytes.equals(p, otherP)) {
          survivingPoints.+=(p)
        }
      })
    })
    points = survivingPoints

    val survivingRanges = new mutable.MutableList[ScanRange]()

    other.ranges.foreach( otherR => {
      ranges.foreach( r => {
        if (r.doesOverLap(otherR)) {
          r.mergeIntersect(otherR)
          survivingRanges += r
        }
      })
    })
    ranges = survivingRanges
  }

  override def toString:String = {
    val strBuilder = new StringBuilder
    strBuilder.append("(points:(")
    var isFirst = true
    points.foreach( p => {
      if (isFirst) isFirst = false
      else strBuilder.append(",")
      strBuilder.append(Bytes.toString(p))
    })
    strBuilder.append("),ranges:")
    isFirst = true
    ranges.foreach( r => {
      if (isFirst) isFirst = false
      else strBuilder.append(",")
      strBuilder.append(r)
    })
    strBuilder.append("))")
    strBuilder.toString()
  }
}

/**
 * A collection of ColumnFilters indexed by column names.
 *
 * Also contains merge commends that will consolidate the filters
 * per column name
 */
class ColumnFilterCollection {
  val columnFilterMap = new mutable.HashMap[String, ColumnFilter]

  def clear(): Unit = {
    columnFilterMap.clear()
  }

  /**
   * This will allow us to merge filter logic that is joined to the existing filter
   * through a OR operator.  This will merge a single columns filter
   *
   * @param column The column to be merged
   * @param other  The other ColumnFilter object to merge
   */
  def mergeUnion(column:String, other:ColumnFilter): Unit = {
    val existingFilter = columnFilterMap.get(column)
    if (existingFilter.isEmpty) {
      columnFilterMap.+=((column, other))
    } else {
      existingFilter.get.mergeUnion(other)
    }
  }

  /**
   * This will allow us to merge all filters in the existing collection
   * to the filters in the other collection.  All merges are done as a result
   * of a OR operator
   *
   * @param other The other Column Filter Collection to be merged
   */
  def mergeUnion(other:ColumnFilterCollection): Unit = {
    other.columnFilterMap.foreach( e => {
      mergeUnion(e._1, e._2)
    })
  }

  /**
   * This will allow us to merge all filters in the existing collection
   * to the filters in the other collection.  All merges are done as a result
   * of a AND operator
   *
   * @param other The column filter from the other collection
   */
  def mergeIntersect(other:ColumnFilterCollection): Unit = {
    other.columnFilterMap.foreach( e => {
      val existingColumnFilter = columnFilterMap.get(e._1)
      if (existingColumnFilter.isEmpty) {
        columnFilterMap += e
      } else {
        existingColumnFilter.get.mergeIntersect(e._2)
      }
    })
  }

  /**
   * This will collect all the filter information in a way that is optimized
   * for the HBase filter commend.  Allowing the filter to be accessed
   * with columnFamily and qualifier information
   *
   * @param schemaDefinitionMap Schema Map that will help us map the right filters
   *                            to the correct columns
   * @return                    HashMap oc column filters
   */
  def generateFamilyQualifiterFilterMap(schemaDefinitionMap:
                                        java.util.HashMap[String,
                                          SchemaQualifierDefinition]):
  util.HashMap[ColumnFamilyQualifierMapKeyWrapper, ColumnFilter] = {
    val familyQualifierFilterMap =
      new util.HashMap[ColumnFamilyQualifierMapKeyWrapper, ColumnFilter]()

    columnFilterMap.foreach( e => {
      val definition = schemaDefinitionMap.get(e._1)
      //Don't add rowKeyFilter
      if (definition.columnFamilyBytes.size > 0) {
        familyQualifierFilterMap.put(
          new ColumnFamilyQualifierMapKeyWrapper(
            definition.columnFamilyBytes, 0, definition.columnFamilyBytes.length,
            definition.qualifierBytes, 0, definition.qualifierBytes.length), e._2)
      }
    })
    familyQualifierFilterMap
  }

  override def toString:String = {
    val strBuilder = new StringBuilder
    columnFilterMap.foreach( e => strBuilder.append(e))
    strBuilder.toString()
  }
}

/**
 * Status object to store static functions but also to hold last executed
 * information that can be used for unit testing.
 */
object DefaultSourceStaticUtils {

  val rawInteger = new RawInteger
  val rawLong = new RawLong
  val rawFloat = new RawFloat
  val rawDouble = new RawDouble
  val rawString = RawString.ASCENDING

  val byteRange = new ThreadLocal[PositionedByteRange]{
    override def initialValue(): PositionedByteRange = {
      val range = new SimplePositionedMutableByteRange()
      range.setOffset(0)
      range.setPosition(0)
    }
  }

  def getFreshByteRange(bytes:Array[Byte]): PositionedByteRange = {
    getFreshByteRange(bytes, 0, bytes.length)
  }

  def getFreshByteRange(bytes:Array[Byte],  offset:Int = 0, length:Int): PositionedByteRange = {
    byteRange.get().set(bytes).setLength(length).setOffset(offset)
  }

  //This will contain the last 5 filters and required fields used in buildScan
  // These values can be used in unit testing to make sure we are converting
  // The Spark SQL input correctly
  val lastFiveExecutionRules =
    new ConcurrentLinkedQueue[ExecutionRuleForUnitTesting]()

  /**
   * This method is to populate the lastFiveExecutionRules for unit test perposes
   * This method is not thread safe.
   *
   * @param columnFilterCollection           The filters in the last job
   * @param requiredQualifierDefinitionArray The required columns in the last job
   */
  def populateLatestExecutionRules(columnFilterCollection: ColumnFilterCollection,
                                   requiredQualifierDefinitionArray:
                                   mutable.MutableList[SchemaQualifierDefinition]):Unit = {
    lastFiveExecutionRules.add(new ExecutionRuleForUnitTesting(
      columnFilterCollection, requiredQualifierDefinitionArray))
    while (lastFiveExecutionRules.size() > 5) {
      lastFiveExecutionRules.poll()
    }
  }

  /**
   * This method will convert the result content from HBase into the
   * SQL value type that is requested by the Spark SQL schema definition
   *
   * @param columnName              The name of the SparkSQL Column
   * @param schemaMappingDefinition The schema definition map
   * @param r                       The result object from HBase
   * @return                        The converted object type
   */
  def getValue(columnName: String,
               schemaMappingDefinition:
               java.util.HashMap[String, SchemaQualifierDefinition],
               r: Result): Any = {

    val columnDef = schemaMappingDefinition.get(columnName)

    if (columnDef == null) throw new IllegalArgumentException("Unknown column:" + columnName)


    if (columnDef.columnFamilyBytes.isEmpty) {
      val row = r.getRow

      columnDef.columnSparkSqlType match {
        case IntegerType => rawInteger.decode(getFreshByteRange(row))
        case LongType => rawLong.decode(getFreshByteRange(row))
        case FloatType => rawFloat.decode(getFreshByteRange(row))
        case DoubleType => rawDouble.decode(getFreshByteRange(row))
        case StringType => rawString.decode(getFreshByteRange(row))
        case TimestampType => rawLong.decode(getFreshByteRange(row))
        case _ => Bytes.toString(row)
      }
    } else {
      val cellByteValue =
        r.getColumnLatestCell(columnDef.columnFamilyBytes, columnDef.qualifierBytes)
      if (cellByteValue == null) null
      else columnDef.columnSparkSqlType match {
        case IntegerType => rawInteger.decode(getFreshByteRange(cellByteValue.getValueArray,
          cellByteValue.getValueOffset, cellByteValue.getValueLength))
        case LongType => rawLong.decode(getFreshByteRange(cellByteValue.getValueArray,
          cellByteValue.getValueOffset, cellByteValue.getValueLength))
        case FloatType => rawFloat.decode(getFreshByteRange(cellByteValue.getValueArray,
          cellByteValue.getValueOffset, cellByteValue.getValueLength))
        case DoubleType => rawDouble.decode(getFreshByteRange(cellByteValue.getValueArray,
          cellByteValue.getValueOffset, cellByteValue.getValueLength))
        case StringType => Bytes.toString(cellByteValue.getValueArray,
          cellByteValue.getValueOffset, cellByteValue.getValueLength)
        case TimestampType => rawLong.decode(getFreshByteRange(cellByteValue.getValueArray,
          cellByteValue.getValueOffset, cellByteValue.getValueLength))
        case _ => Bytes.toString(cellByteValue.getValueArray,
          cellByteValue.getValueOffset, cellByteValue.getValueLength)
      }
    }
  }

  /**
   * This will convert the value from SparkSQL to be stored into HBase using the
   * right byte Type
   *
   * @param columnName              SparkSQL column name
   * @param schemaMappingDefinition Schema definition map
   * @param value                   String value from SparkSQL
   * @return                        Returns the byte array to go into HBase
   */
  def getByteValue(columnName: String,
                   schemaMappingDefinition:
                   java.util.HashMap[String, SchemaQualifierDefinition],
                   value: String): Array[Byte] = {

    val columnDef = schemaMappingDefinition.get(columnName)

    if (columnDef == null) {
      throw new IllegalArgumentException("Unknown column:" + columnName)
    } else {
      columnDef.columnSparkSqlType match {
        case IntegerType =>
          val result = new Array[Byte](Bytes.SIZEOF_INT)
          val localDataRange = getFreshByteRange(result)
          rawInteger.encode(localDataRange, value.toInt)
          localDataRange.getBytes
        case LongType =>
          val result = new Array[Byte](Bytes.SIZEOF_LONG)
          val localDataRange = getFreshByteRange(result)
          rawLong.encode(localDataRange, value.toLong)
          localDataRange.getBytes
        case FloatType =>
          val result = new Array[Byte](Bytes.SIZEOF_FLOAT)
          val localDataRange = getFreshByteRange(result)
          rawFloat.encode(localDataRange, value.toFloat)
          localDataRange.getBytes
        case DoubleType =>
          val result = new Array[Byte](Bytes.SIZEOF_DOUBLE)
          val localDataRange = getFreshByteRange(result)
          rawDouble.encode(localDataRange, value.toDouble)
          localDataRange.getBytes
        case StringType =>
          Bytes.toBytes(value)
        case TimestampType =>
          val result = new Array[Byte](Bytes.SIZEOF_LONG)
          val localDataRange = getFreshByteRange(result)
          rawLong.encode(localDataRange, value.toLong)
          localDataRange.getBytes

        case _ => Bytes.toBytes(value)
      }
    }
  }
}

class ExecutionRuleForUnitTesting(val columnFilterCollection: ColumnFilterCollection,
                                  val requiredQualifierDefinitionArray:
                                  mutable.MutableList[SchemaQualifierDefinition])
