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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.spark.datasources._
import org.apache.hadoop.hbase.types._
import org.apache.hadoop.hbase.util.{Bytes, PositionedByteRange, SimplePositionedMutableByteRange}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
 * DefaultSource for integration with Spark's dataframe datasources.
 * This class will produce a relationProvider based on input given to it from spark
 *
 * This class needs to stay in the current package 'org.apache.hadoop.hbase.spark'
 * for Spark to match the hbase data source name.
 *
 * In all this DefaultSource support the following datasource functionality
 * - Scan range pruning through filter push down logic based on rowKeys
 * - Filter push down logic on HBase Cells
 * - Qualifier filtering based on columns used in the SparkSQL statement
 * - Type conversions of basic SQL types.  All conversions will be
 *   Through the HBase Bytes object commands.
 */
@InterfaceAudience.Private
class DefaultSource extends RelationProvider  with CreatableRelationProvider with Logging {
  /**
   * Is given input from SparkSQL to construct a BaseRelation
    *
    * @param sqlContext SparkSQL context
   * @param parameters Parameters given to us from SparkSQL
   * @return           A BaseRelation Object
   */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]):
  BaseRelation = {
    new HBaseRelation(parameters, None)(sqlContext)
  }


  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val relation = HBaseRelation(parameters, Some(data.schema))(sqlContext)
    relation.createTable()
    relation.insert(data, false)
    relation
  }
}

/**
 * Implementation of Spark BaseRelation that will build up our scan logic
 * , do the scan pruning, filter push down, and value conversions
  *
  * @param sqlContext              SparkSQL context
 */
@InterfaceAudience.Private
case class HBaseRelation (
    @transient parameters: Map[String, String],
    userSpecifiedSchema: Option[StructType]
  )(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan  with InsertableRelation  with Logging {
  val timestamp = parameters.get(HBaseSparkConf.TIMESTAMP).map(_.toLong)
  val minTimestamp = parameters.get(HBaseSparkConf.TIMERANGE_START).map(_.toLong)
  val maxTimestamp = parameters.get(HBaseSparkConf.TIMERANGE_END).map(_.toLong)
  val maxVersions = parameters.get(HBaseSparkConf.MAX_VERSIONS).map(_.toInt)
  val encoderClsName = parameters.get(HBaseSparkConf.QUERY_ENCODER).getOrElse(HBaseSparkConf.DEFAULT_QUERY_ENCODER)

  @transient val encoder = JavaBytesEncoder.create(encoderClsName)

  val catalog = HBaseTableCatalog(parameters)
  def tableName = catalog.name
  val configResources = parameters.get(HBaseSparkConf.HBASE_CONFIG_LOCATION)
  val useHBaseContext =  parameters.get(HBaseSparkConf.USE_HBASECONTEXT).map(_.toBoolean).getOrElse(HBaseSparkConf.DEFAULT_USE_HBASECONTEXT)
  val usePushDownColumnFilter = parameters.get(HBaseSparkConf.PUSHDOWN_COLUMN_FILTER)
    .map(_.toBoolean).getOrElse(HBaseSparkConf.DEFAULT_PUSHDOWN_COLUMN_FILTER)

  // The user supplied per table parameter will overwrite global ones in SparkConf
  val blockCacheEnable = parameters.get(HBaseSparkConf.QUERY_CACHEBLOCKS).map(_.toBoolean)
    .getOrElse(
      sqlContext.sparkContext.getConf.getBoolean(
        HBaseSparkConf.QUERY_CACHEBLOCKS, HBaseSparkConf.DEFAULT_QUERY_CACHEBLOCKS))
  val cacheSize = parameters.get(HBaseSparkConf.QUERY_CACHEDROWS).map(_.toInt)
    .getOrElse(
      sqlContext.sparkContext.getConf.getInt(
      HBaseSparkConf.QUERY_CACHEDROWS, -1))
  val batchNum = parameters.get(HBaseSparkConf.QUERY_BATCHSIZE).map(_.toInt)
    .getOrElse(sqlContext.sparkContext.getConf.getInt(
    HBaseSparkConf.QUERY_BATCHSIZE,  -1))

  val bulkGetSize =  parameters.get(HBaseSparkConf.BULKGET_SIZE).map(_.toInt)
    .getOrElse(sqlContext.sparkContext.getConf.getInt(
    HBaseSparkConf.BULKGET_SIZE,  HBaseSparkConf.DEFAULT_BULKGET_SIZE))

  //create or get latest HBaseContext
  val hbaseContext:HBaseContext = if (useHBaseContext) {
    LatestHBaseContextCache.latest
  } else {
    val config = HBaseConfiguration.create()
    configResources.map(resource => resource.split(",").foreach(r => config.addResource(r)))
    new HBaseContext(sqlContext.sparkContext, config)
  }

  val wrappedConf = new SerializableConfiguration(hbaseContext.config)
  def hbaseConf = wrappedConf.value

  /**
   * Generates a Spark SQL schema objeparametersct so Spark SQL knows what is being
   * provided by this BaseRelation
   *
   * @return schema generated from the SCHEMA_COLUMNS_MAPPING_KEY value
   */
  override val schema: StructType = userSpecifiedSchema.getOrElse(catalog.toDataType)



  def createTable() {
    val numReg = parameters.get(HBaseTableCatalog.newTable).map(x => x.toInt).getOrElse(0)
    val startKey =  Bytes.toBytes(
      parameters.get(HBaseTableCatalog.regionStart)
        .getOrElse(HBaseTableCatalog.defaultRegionStart))
    val endKey = Bytes.toBytes(
      parameters.get(HBaseTableCatalog.regionEnd)
        .getOrElse(HBaseTableCatalog.defaultRegionEnd))
    if (numReg > 3) {
      val tName = TableName.valueOf(catalog.name)
      val cfs = catalog.getColumnFamilies

      val connection = HBaseConnectionCache.getConnection(hbaseConf)
      // Initialize hBase table if necessary
      val admin = connection.getAdmin
      try {
        if (!admin.isTableAvailable(tName)) {
          val tableDesc = new HTableDescriptor(tName)
          cfs.foreach { x =>
            val cf = new HColumnDescriptor(x.getBytes())
            logDebug(s"add family $x to ${catalog.name}")
            tableDesc.addFamily(cf)
          }
          val splitKeys = Bytes.split(startKey, endKey, numReg);
          admin.createTable(tableDesc, splitKeys)

        }
      }finally {
        admin.close()
        connection.close()
      }
    } else {
      logInfo(
        s"""${HBaseTableCatalog.newTable}
           |is not defined or no larger than 3, skip the create table""".stripMargin)
    }
  }

  /**
    *
    * @param data
    * @param overwrite
    */
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val jobConfig: JobConf = new JobConf(hbaseConf, this.getClass)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, catalog.name)
    var count = 0
    val rkFields = catalog.getRowKey
    val rkIdxedFields = rkFields.map{ case x =>
      (schema.fieldIndex(x.colName), x)
    }
    val colsIdxedFields = schema
      .fieldNames
      .partition( x => rkFields.map(_.colName).contains(x))
      ._2.map(x => (schema.fieldIndex(x), catalog.getField(x)))
    val rdd = data.rdd
    def convertToPut(row: Row) = {
      // construct bytes for row key
      val rowBytes = rkIdxedFields.map { case (x, y) =>
        Utils.toBytes(row(x), y)
      }
      val rLen = rowBytes.foldLeft(0) { case (x, y) =>
        x + y.length
      }
      val rBytes = new Array[Byte](rLen)
      var offset = 0
      rowBytes.foreach { x =>
        System.arraycopy(x, 0, rBytes, offset, x.length)
        offset += x.length
      }
      val put = timestamp.fold(new Put(rBytes))(new Put(rBytes, _))

      colsIdxedFields.foreach { case (x, y) =>
        val b = Utils.toBytes(row(x), y)
        put.addColumn(Bytes.toBytes(y.cf), Bytes.toBytes(y.col), b)
      }
      count += 1
      (new ImmutableBytesWritable, put)
    }
    rdd.map(convertToPut(_)).saveAsHadoopDataset(jobConfig)
  }

  def getIndexedProjections(requiredColumns: Array[String]): Seq[(Field, Int)] = {
    requiredColumns.map(catalog.sMap.getField(_)).zipWithIndex
  }


  /**
    * Takes a HBase Row object and parses all of the fields from it.
    * This is independent of which fields were requested from the key
    * Because we have all the data it's less complex to parse everything.
    *
    * @param row the retrieved row from hbase.
    * @param keyFields all of the fields in the row key, ORDERED by their order in the row key.
    */
  def parseRowKey(row: Array[Byte], keyFields: Seq[Field]): Map[Field, Any] = {
    keyFields.foldLeft((0, Seq[(Field, Any)]()))((state, field) => {
      val idx = state._1
      val parsed = state._2
      if (field.length != -1) {
        val value = Utils.hbaseFieldToScalaType(field, row, idx, field.length)
        // Return the new index and appended value
        (idx + field.length, parsed ++ Seq((field, value)))
      } else {
        field.dt match {
          case StringType =>
            val pos = row.indexOf(HBaseTableCatalog.delimiter, idx)
            if (pos == -1 || pos > row.length) {
              // this is at the last dimension
              val value = Utils.hbaseFieldToScalaType(field, row, idx, row.length)
              (row.length + 1, parsed ++ Seq((field, value)))
            } else {
              val value = Utils.hbaseFieldToScalaType(field, row, idx, pos - idx)
              (pos, parsed ++ Seq((field, value)))
            }
          // We don't know the length, assume it extends to the end of the rowkey.
          case _ => (row.length + 1, parsed ++ Seq((field, Utils.hbaseFieldToScalaType(field, row, idx, row.length))))
        }
      }
    })._2.toMap
  }

  def buildRow(fields: Seq[Field], result: Result): Row = {
    val r = result.getRow
    val keySeq = parseRowKey(r, catalog.getRowKey)
    val valueSeq = fields.filter(!_.isRowKey).map { x =>
      val kv = result.getColumnLatestCell(Bytes.toBytes(x.cf), Bytes.toBytes(x.col))
      if (kv == null || kv.getValueLength == 0) {
        (x, null)
      } else {
        val v = CellUtil.cloneValue(kv)
        (x, x.dt match {
          // Here, to avoid arraycopy, return v directly instead of calling hbaseFieldToScalaType
          case BinaryType => v
          case _ => Utils.hbaseFieldToScalaType(x, v, 0, v.length)
        })
      }
    }.toMap
    val unionedRow = keySeq ++ valueSeq
    // Return the row ordered by the requested order
    Row.fromSeq(fields.map(unionedRow.get(_).getOrElse(null)))
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

    val pushDownTuple = buildPushDownPredicatesResource(filters)
    val pushDownRowKeyFilter = pushDownTuple._1
    var pushDownDynamicLogicExpression = pushDownTuple._2
    val valueArray = pushDownTuple._3

    if (!usePushDownColumnFilter) {
      pushDownDynamicLogicExpression = null
    }

    logDebug("pushDownRowKeyFilter:           " + pushDownRowKeyFilter.ranges)
    if (pushDownDynamicLogicExpression != null) {
      logDebug("pushDownDynamicLogicExpression: " +
        pushDownDynamicLogicExpression.toExpressionString)
    }
    logDebug("valueArray:                     " + valueArray.length)

    val requiredQualifierDefinitionList =
      new mutable.MutableList[Field]

    requiredColumns.foreach( c => {
      val field = catalog.getField(c)
      requiredQualifierDefinitionList += field
    })

    //retain the information for unit testing checks
    DefaultSourceStaticUtils.populateLatestExecutionRules(pushDownRowKeyFilter,
      pushDownDynamicLogicExpression)

    val getList = new util.ArrayList[Get]()
    val rddList = new util.ArrayList[RDD[Row]]()

    //add points to getList
    pushDownRowKeyFilter.points.foreach(p => {
      val get = new Get(p)
      requiredQualifierDefinitionList.foreach( d => {
        if (d.isRowKey)
          get.addColumn(d.cfBytes, d.colBytes)
      })
      getList.add(get)
    })

    val pushDownFilterJava = if (usePushDownColumnFilter && pushDownDynamicLogicExpression != null) {
        Some(new SparkSQLPushDownFilter(pushDownDynamicLogicExpression,
          valueArray, requiredQualifierDefinitionList, encoderClsName))
    } else {
      None
    }
    val hRdd = new HBaseTableScanRDD(this, hbaseContext, pushDownFilterJava, requiredQualifierDefinitionList.seq)
    pushDownRowKeyFilter.points.foreach(hRdd.addPoint(_))
    pushDownRowKeyFilter.ranges.foreach(hRdd.addRange(_))

    var resultRDD: RDD[Row] = {
      val tmp = hRdd.map{ r =>
        val indexedFields = getIndexedProjections(requiredColumns).map(_._1)
        buildRow(indexedFields, r)

      }
      if (tmp.partitions.size > 0) {
        tmp
      } else {
        null
      }
    }

    if (resultRDD == null) {
      val scan = new Scan()
      scan.setCacheBlocks(blockCacheEnable)
      scan.setBatch(batchNum)
      scan.setCaching(cacheSize)
      requiredQualifierDefinitionList.foreach( d =>
        scan.addColumn(d.cfBytes, d.colBytes))

      val rdd = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan).map(r => {
        val indexedFields = getIndexedProjections(requiredColumns).map(_._1)
        buildRow(indexedFields, r._2)
      })
      resultRDD=rdd
    }
    resultRDD
  }

  def buildPushDownPredicatesResource(filters: Array[Filter]):
  (RowKeyFilter, DynamicLogicExpression, Array[Array[Byte]]) = {
    var superRowKeyFilter:RowKeyFilter = null
    val queryValueList = new mutable.MutableList[Array[Byte]]
    var superDynamicLogicExpression: DynamicLogicExpression = null

    filters.foreach( f => {
      val rowKeyFilter = new RowKeyFilter()
      val logicExpression = transverseFilterTree(rowKeyFilter, queryValueList, f)
      if (superDynamicLogicExpression == null) {
        superDynamicLogicExpression = logicExpression
        superRowKeyFilter = rowKeyFilter
      } else {
        superDynamicLogicExpression =
          new AndLogicExpression(superDynamicLogicExpression, logicExpression)
        superRowKeyFilter.mergeIntersect(rowKeyFilter)
      }

    })

    val queryValueArray = queryValueList.toArray

    if (superRowKeyFilter == null) {
      superRowKeyFilter = new RowKeyFilter
    }

    (superRowKeyFilter, superDynamicLogicExpression, queryValueArray)
  }

  /**
    * For some codec, the order may be inconsistent between java primitive
    * type and its byte array. We may have to  split the predicates on some
    * of the java primitive type into multiple predicates. The encoder will take
    * care of it and returning the concrete ranges.
    *
    * For example in naive codec,  some of the java primitive types have to be split into multiple
    * predicates, and union these predicates together to make the predicates be performed correctly.
    * For example, if we have "COLUMN < 2", we will transform it into
    * "0 <= COLUMN < 2 OR Integer.MIN_VALUE <= COLUMN <= -1"
    */

  def transverseFilterTree(parentRowKeyFilter:RowKeyFilter,
                                  valueArray:mutable.MutableList[Array[Byte]],
                                  filter:Filter): DynamicLogicExpression = {
    filter match {
      case EqualTo(attr, value) =>
        val field = catalog.getField(attr)
        if (field != null) {
          if (field.isRowKey) {
            parentRowKeyFilter.mergeIntersect(new RowKeyFilter(
              DefaultSourceStaticUtils.getByteValue(field,
                value.toString), null))
          }
          val byteValue =
            DefaultSourceStaticUtils.getByteValue(field, value.toString)
          valueArray += byteValue
        }
        new EqualLogicExpression(attr, valueArray.length - 1, false)

      /**
        * encoder may split the predicates into multiple byte array boundaries.
        * Each boundaries is mapped into the RowKeyFilter and then is unioned by the reduce
        * operation. If the data type is not supported, b will be None, and there is
        * no operation happens on the parentRowKeyFilter.
        *
        * Note that because LessThan is not inclusive, thus the first bound should be exclusive,
        * which is controlled by inc.
        *
        * The other predicates, i.e., GreaterThan/LessThanOrEqual/GreaterThanOrEqual follows
        * the similar logic.
        */
      case LessThan(attr, value) =>
        val field = catalog.getField(attr)
        if (field != null) {
          if (field.isRowKey) {
            val b = encoder.ranges(value)
            var inc = false
            b.map(_.less.map { x =>
              val r = new RowKeyFilter(null,
                new ScanRange(x.upper, inc, x.low, true)
              )
              inc = true
              r
            }).map { x =>
              x.reduce { (i, j) =>
                i.mergeUnion(j)
              }
            }.map(parentRowKeyFilter.mergeIntersect(_))
          }
          val byteValue = encoder.encode(field.dt, value)
          valueArray += byteValue
        }
        new LessThanLogicExpression(attr, valueArray.length - 1)
      case GreaterThan(attr, value) =>
        val field = catalog.getField(attr)
        if (field != null) {
          if (field.isRowKey) {
            val b = encoder.ranges(value)
            var inc = false
            b.map(_.greater.map{x =>
              val r = new RowKeyFilter(null,
                new ScanRange(x.upper, true, x.low, inc))
              inc = true
              r
            }).map { x =>
              x.reduce { (i, j) =>
                i.mergeUnion(j)
              }
            }.map(parentRowKeyFilter.mergeIntersect(_))
          }
          val byteValue = encoder.encode(field.dt, value)
          valueArray += byteValue
        }
        new GreaterThanLogicExpression(attr, valueArray.length - 1)
      case LessThanOrEqual(attr, value) =>
        val field = catalog.getField(attr)
        if (field != null) {
          if (field.isRowKey) {
            val b = encoder.ranges(value)
            b.map(_.less.map(x =>
              new RowKeyFilter(null,
                new ScanRange(x.upper, true, x.low, true))))
              .map { x =>
                x.reduce{ (i, j) =>
                  i.mergeUnion(j)
                }
              }.map(parentRowKeyFilter.mergeIntersect(_))
          }
          val byteValue = encoder.encode(field.dt, value)
          valueArray += byteValue
        }
        new LessThanOrEqualLogicExpression(attr, valueArray.length - 1)
      case GreaterThanOrEqual(attr, value) =>
        val field = catalog.getField(attr)
        if (field != null) {
          if (field.isRowKey) {
            val b = encoder.ranges(value)
            b.map(_.greater.map(x =>
              new RowKeyFilter(null,
                new ScanRange(x.upper, true, x.low, true))))
              .map { x =>
                x.reduce { (i, j) =>
                  i.mergeUnion(j)
                }
              }.map(parentRowKeyFilter.mergeIntersect(_))
          }
          val byteValue = encoder.encode(field.dt, value)
          valueArray += byteValue
        }
        new GreaterThanOrEqualLogicExpression(attr, valueArray.length - 1)
      case Or(left, right) =>
        val leftExpression = transverseFilterTree(parentRowKeyFilter, valueArray, left)
        val rightSideRowKeyFilter = new RowKeyFilter
        val rightExpression = transverseFilterTree(rightSideRowKeyFilter, valueArray, right)

        parentRowKeyFilter.mergeUnion(rightSideRowKeyFilter)

        new OrLogicExpression(leftExpression, rightExpression)
      case And(left, right) =>

        val leftExpression = transverseFilterTree(parentRowKeyFilter, valueArray, left)
        val rightSideRowKeyFilter = new RowKeyFilter
        val rightExpression = transverseFilterTree(rightSideRowKeyFilter, valueArray, right)
        parentRowKeyFilter.mergeIntersect(rightSideRowKeyFilter)

        new AndLogicExpression(leftExpression, rightExpression)
      case IsNull(attr) =>
        new IsNullLogicExpression(attr, false)
      case IsNotNull(attr) =>
        new IsNullLogicExpression(attr, true)
      case _ =>
        new PassThroughLogicExpression
    }
  }
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
@InterfaceAudience.Private
class ScanRange(var upperBound:Array[Byte], var isUpperBoundEqualTo:Boolean,
                var lowerBound:Array[Byte], var isLowerBoundEqualTo:Boolean)
  extends Serializable {

  /**
   * Function to merge another scan object through a AND operation
    *
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
    *
    * @param other Other scan object
   */
  def mergeUnion(other:ScanRange): Unit = {

    val upperBoundCompare = compareRange(upperBound, other.upperBound)
    val lowerBoundCompare = compareRange(lowerBound, other.lowerBound)

    upperBound = if (upperBoundCompare >0) upperBound else other.upperBound
    lowerBound = if (lowerBoundCompare <0) lowerBound else other.lowerBound

    isLowerBoundEqualTo = if (lowerBoundCompare == 0)
      isLowerBoundEqualTo || other.isLowerBoundEqualTo
    else if (lowerBoundCompare < 0) isLowerBoundEqualTo else other.isLowerBoundEqualTo

    isUpperBoundEqualTo = if (upperBoundCompare == 0)
      isUpperBoundEqualTo || other.isUpperBoundEqualTo
    else if (upperBoundCompare < 0) other.isUpperBoundEqualTo else isUpperBoundEqualTo
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
  def getOverLapScanRange(other:ScanRange): ScanRange = {

    var leftRange:ScanRange = null
    var rightRange:ScanRange = null

    // First identify the Left range
    // Also lower bound can't be null
    if (compareRange(lowerBound, other.lowerBound) < 0 ||
      compareRange(upperBound, other.upperBound) < 0) {
      leftRange = this
      rightRange = other
    } else {
      leftRange = other
      rightRange = this
    }

    if (hasOverlap(leftRange, rightRange)) {
      // Find the upper bound and lower bound
      if (compareRange(leftRange.upperBound, rightRange.upperBound) >= 0) {
        new ScanRange(rightRange.upperBound, rightRange.isUpperBoundEqualTo,
          rightRange.lowerBound, rightRange.isLowerBoundEqualTo)
      } else {
        new ScanRange(leftRange.upperBound, leftRange.isUpperBoundEqualTo,
          rightRange.lowerBound, rightRange.isLowerBoundEqualTo)
      }
    } else {
      null
    }
  }

  /**
    * The leftRange.upperBound has to be larger than the rightRange's lowerBound.
    * Otherwise, there is no overlap.
    *
    * @param left: The range with the smaller lowBound
    * @param right: The range with the larger lowBound
    * @return Whether two ranges have overlap.
    */

  def hasOverlap(left: ScanRange, right: ScanRange): Boolean = {
    compareRange(left.upperBound, right.lowerBound) >= 0
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

  /**
   *
   * @return
   */
  def containsPoint(point:Array[Byte]): Boolean = {
    val lowerCompare = compareRange(point, lowerBound)
    val upperCompare = compareRange(point, upperBound)

    ((isLowerBoundEqualTo && lowerCompare >= 0) ||
      (!isLowerBoundEqualTo && lowerCompare > 0)) &&
      ((isUpperBoundEqualTo && upperCompare <= 0) ||
        (!isUpperBoundEqualTo && upperCompare < 0))

  }
  override def toString:String = {
    "ScanRange:(upperBound:" + Bytes.toString(upperBound) +
      ",isUpperBoundEqualTo:" + isUpperBoundEqualTo + ",lowerBound:" +
      Bytes.toString(lowerBound) + ",isLowerBoundEqualTo:" + isLowerBoundEqualTo + ")"
  }
}

/**
 * Contains information related to a filters for a given column.
 * This can contain many ranges or points.
 *
 * @param currentPoint the initial point when the filter is created
 * @param currentRange the initial scanRange when the filter is created
 */
@InterfaceAudience.Private
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
        if (r.getOverLapScanRange(otherR) != null) {
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
        if (r.getOverLapScanRange(otherR) != null) {
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
@InterfaceAudience.Private
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
@InterfaceAudience.Private
object DefaultSourceStaticUtils {

  val rawInteger = new RawInteger
  val rawLong = new RawLong
  val rawFloat = new RawFloat
  val rawDouble = new RawDouble
  val rawString = RawString.ASCENDING

  val byteRange = new ThreadLocal[PositionedByteRange] {
    override def initialValue(): PositionedByteRange = {
      val range = new SimplePositionedMutableByteRange()
      range.setOffset(0)
      range.setPosition(0)
    }
  }

  def getFreshByteRange(bytes: Array[Byte]): PositionedByteRange = {
    getFreshByteRange(bytes, 0, bytes.length)
  }

  def getFreshByteRange(bytes: Array[Byte], offset: Int = 0, length: Int):
  PositionedByteRange = {
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
   * @param rowKeyFilter           The rowKey Filter logic used in the last query
   * @param dynamicLogicExpression The dynamicLogicExpression used in the last query
   */
  def populateLatestExecutionRules(rowKeyFilter: RowKeyFilter,
                                   dynamicLogicExpression: DynamicLogicExpression): Unit = {
    lastFiveExecutionRules.add(new ExecutionRuleForUnitTesting(
      rowKeyFilter, dynamicLogicExpression))
    while (lastFiveExecutionRules.size() > 5) {
      lastFiveExecutionRules.poll()
    }
  }

  /**
   * This method will convert the result content from HBase into the
   * SQL value type that is requested by the Spark SQL schema definition
   *
   * @param field              The structure of the SparkSQL Column
   * @param r                       The result object from HBase
   * @return                        The converted object type
   */
  def getValue(field: Field,
      r: Result): Any = {
    if (field.isRowKey) {
      val row = r.getRow

      field.dt match {
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
        r.getColumnLatestCell(field.cfBytes, field.colBytes)
      if (cellByteValue == null) null
      else field.dt match {
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
   * @param value                   String value from SparkSQL
   * @return                        Returns the byte array to go into HBase
   */
  def getByteValue(field: Field,
      value: String): Array[Byte] = {
    field.dt match {
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

/**
 * Contains information related to a filters for a given column.
 * This can contain many ranges or points.
 *
 * @param currentPoint the initial point when the filter is created
 * @param currentRange the initial scanRange when the filter is created
 */
@InterfaceAudience.Private
class RowKeyFilter (currentPoint:Array[Byte] = null,
                    currentRange:ScanRange =
                    new ScanRange(null, true, new Array[Byte](0), true),
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
  def mergeUnion(other:RowKeyFilter): RowKeyFilter = {
    other.points.foreach( p => points += p)

    other.ranges.foreach( otherR => {
      var doesOverLap = false
      ranges.foreach{ r =>
        if (r.getOverLapScanRange(otherR) != null) {
          r.mergeUnion(otherR)
          doesOverLap = true
        }}
      if (!doesOverLap) ranges.+=(otherR)
    })
    this
  }

  /**
   * This will allow us to merge filter logic that is joined to the existing filter
   * through a AND operator
   *
   * @param other Filter to merge
   */
  def mergeIntersect(other:RowKeyFilter): RowKeyFilter = {
    val survivingPoints = new mutable.MutableList[Array[Byte]]()
    val didntSurviveFirstPassPoints = new mutable.MutableList[Array[Byte]]()
    if (points == null || points.length == 0) {
      other.points.foreach( otherP => {
        didntSurviveFirstPassPoints += otherP
      })
    } else {
      points.foreach(p => {
        if (other.points.length == 0) {
          didntSurviveFirstPassPoints += p
        } else {
          other.points.foreach(otherP => {
            if (Bytes.equals(p, otherP)) {
              survivingPoints += p
            } else {
              didntSurviveFirstPassPoints += p
            }
          })
        }
      })
    }

    val survivingRanges = new mutable.MutableList[ScanRange]()

    if (ranges.length == 0) {
      didntSurviveFirstPassPoints.foreach(p => {
          survivingPoints += p
      })
    } else {
      ranges.foreach(r => {
        other.ranges.foreach(otherR => {
          val overLapScanRange = r.getOverLapScanRange(otherR)
          if (overLapScanRange != null) {
            survivingRanges += overLapScanRange
          }
        })
        didntSurviveFirstPassPoints.foreach(p => {
          if (r.containsPoint(p)) {
            survivingPoints += p
          }
        })
      })
    }
    points = survivingPoints
    ranges = survivingRanges
    this
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

@InterfaceAudience.Private
class ExecutionRuleForUnitTesting(val rowKeyFilter: RowKeyFilter,
                                  val dynamicLogicExpression: DynamicLogicExpression)
