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

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.sql.Timestamp
import java.util
import java.util.HashMap

import org.apache.avro.SchemaBuilder.BaseFieldTypeBuilder
import org.apache.avro.SchemaBuilder.BaseTypeBuilder
import org.apache.avro.SchemaBuilder.FieldAssembler
import org.apache.avro.SchemaBuilder.FieldDefault
import org.apache.avro.SchemaBuilder.RecordBuilder
import org.apache.avro.io._
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._

import org.apache.avro.{SchemaBuilder, Schema}
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData.{Record, Fixed}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericData, GenericRecord}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.immutable.Map


abstract class AvroException(msg: String) extends Exception(msg)
case class SchemaConversionException(msg: String) extends AvroException(msg)

/***
  * On top level, the converters provide three high level interface.
  * 1. toSqlType: This function takes an avro schema and returns a sql schema.
  * 2. createConverterToSQL: Returns a function that is used to convert avro types to their
  *    corresponding sparkSQL representations.
  * 3. convertTypeToAvro: This function constructs converter function for a given sparkSQL
  *    datatype. This is used in writing Avro records out to disk
  */
object SchemaConverters {

  case class SchemaType(dataType: DataType, nullable: Boolean)

  /**
    * This function takes an avro schema and returns a sql schema.
    */
  def toSqlType(avroSchema: Schema): SchemaType = {
    avroSchema.getType match {
      case INT => SchemaType(IntegerType, nullable = false)
      case STRING => SchemaType(StringType, nullable = false)
      case BOOLEAN => SchemaType(BooleanType, nullable = false)
      case BYTES => SchemaType(BinaryType, nullable = false)
      case DOUBLE => SchemaType(DoubleType, nullable = false)
      case FLOAT => SchemaType(FloatType, nullable = false)
      case LONG => SchemaType(LongType, nullable = false)
      case FIXED => SchemaType(BinaryType, nullable = false)
      case ENUM => SchemaType(StringType, nullable = false)

      case RECORD =>
        val fields = avroSchema.getFields.map { f =>
          val schemaType = toSqlType(f.schema())
          StructField(f.name, schemaType.dataType, schemaType.nullable)
        }

        SchemaType(StructType(fields), nullable = false)

      case ARRAY =>
        val schemaType = toSqlType(avroSchema.getElementType)
        SchemaType(
          ArrayType(schemaType.dataType, containsNull = schemaType.nullable),
          nullable = false)

      case MAP =>
        val schemaType = toSqlType(avroSchema.getValueType)
        SchemaType(
          MapType(StringType, schemaType.dataType, valueContainsNull = schemaType.nullable),
          nullable = false)

      case UNION =>
        if (avroSchema.getTypes.exists(_.getType == NULL)) {
          // In case of a union with null, eliminate it and make a recursive call
          val remainingUnionTypes = avroSchema.getTypes.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            toSqlType(remainingUnionTypes.get(0)).copy(nullable = true)
          } else {
            toSqlType(Schema.createUnion(remainingUnionTypes)).copy(nullable = true)
          }
        } else avroSchema.getTypes.map(_.getType) match {
          case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
            SchemaType(LongType, nullable = false)
          case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
            SchemaType(DoubleType, nullable = false)
          case other => throw new SchemaConversionException(
            s"This mix of union types is not supported: $other")
        }

      case other => throw new SchemaConversionException(s"Unsupported type $other")
    }
  }

  /**
    * This function converts sparkSQL StructType into avro schema. This method uses two other
    * converter methods in order to do the conversion.
    */
  private def convertStructToAvro[T](
                                      structType: StructType,
                                      schemaBuilder: RecordBuilder[T],
                                      recordNamespace: String): T = {
    val fieldsAssembler: FieldAssembler[T] = schemaBuilder.fields()
    structType.fields.foreach { field =>
      val newField = fieldsAssembler.name(field.name).`type`()

      if (field.nullable) {
        convertFieldTypeToAvro(field.dataType, newField.nullable(), field.name, recordNamespace)
          .noDefault
      } else {
        convertFieldTypeToAvro(field.dataType, newField, field.name, recordNamespace)
          .noDefault
      }
    }
    fieldsAssembler.endRecord()
  }

  /**
    * Returns a function that is used to convert avro types to their
    * corresponding sparkSQL representations.
    */
  def createConverterToSQL(schema: Schema): Any => Any = {
    schema.getType match {
      // Avro strings are in Utf8, so we have to call toString on them
      case STRING | ENUM => (item: Any) => if (item == null) null else item.toString
      case INT | BOOLEAN | DOUBLE | FLOAT | LONG => identity
      // Byte arrays are reused by avro, so we have to make a copy of them.
      case FIXED => (item: Any) => if (item == null) {
        null
      } else {
        item.asInstanceOf[Fixed].bytes().clone()
      }
      case BYTES => (item: Any) => if (item == null) {
        null
      } else {
        val bytes = item.asInstanceOf[ByteBuffer]
        val javaBytes = new Array[Byte](bytes.remaining)
        bytes.get(javaBytes)
        javaBytes
      }
      case RECORD =>
        val fieldConverters = schema.getFields.map(f => createConverterToSQL(f.schema))
        (item: Any) => if (item == null) {
          null
        } else {
          val record = item.asInstanceOf[GenericRecord]
          val converted = new Array[Any](fieldConverters.size)
          var idx = 0
          while (idx < fieldConverters.size) {
            converted(idx) = fieldConverters.apply(idx)(record.get(idx))
            idx += 1
          }
          Row.fromSeq(converted.toSeq)
        }
      case ARRAY =>
        val elementConverter = createConverterToSQL(schema.getElementType)
        (item: Any) => if (item == null) {
          null
        } else {
          try {
            item.asInstanceOf[GenericData.Array[Any]].map(elementConverter)
          } catch {
            case e: Throwable =>
              item.asInstanceOf[util.ArrayList[Any]].map(elementConverter)
          }
        }
      case MAP =>
        val valueConverter = createConverterToSQL(schema.getValueType)
        (item: Any) => if (item == null) {
          null
        } else {
          item.asInstanceOf[HashMap[Any, Any]].map(x => (x._1.toString, valueConverter(x._2))).toMap
        }
      case UNION =>
        if (schema.getTypes.exists(_.getType == NULL)) {
          val remainingUnionTypes = schema.getTypes.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            createConverterToSQL(remainingUnionTypes.get(0))
          } else {
            createConverterToSQL(Schema.createUnion(remainingUnionTypes))
          }
        } else schema.getTypes.map(_.getType) match {
          case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
            (item: Any) => {
              item match {
                case l: Long => l
                case i: Int => i.toLong
                case null => null
              }
            }
          case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
            (item: Any) => {
              item match {
                case d: Double => d
                case f: Float => f.toDouble
                case null => null
              }
            }
          case other => throw new SchemaConversionException(
            s"This mix of union types is not supported (see README): $other")
        }
      case other => throw new SchemaConversionException(s"invalid avro type: $other")
    }
  }

  /**
    * This function is used to convert some sparkSQL type to avro type. Note that this function won't
    * be used to construct fields of avro record (convertFieldTypeToAvro is used for that).
    */
  private def convertTypeToAvro[T](
                                    dataType: DataType,
                                    schemaBuilder: BaseTypeBuilder[T],
                                    structName: String,
                                    recordNamespace: String): T = {
    dataType match {
      case ByteType => schemaBuilder.intType()
      case ShortType => schemaBuilder.intType()
      case IntegerType => schemaBuilder.intType()
      case LongType => schemaBuilder.longType()
      case FloatType => schemaBuilder.floatType()
      case DoubleType => schemaBuilder.doubleType()
      case _: DecimalType => schemaBuilder.stringType()
      case StringType => schemaBuilder.stringType()
      case BinaryType => schemaBuilder.bytesType()
      case BooleanType => schemaBuilder.booleanType()
      case TimestampType => schemaBuilder.longType()

      case ArrayType(elementType, _) =>
        val builder = getSchemaBuilder(dataType.asInstanceOf[ArrayType].containsNull)
        val elementSchema = convertTypeToAvro(elementType, builder, structName, recordNamespace)
        schemaBuilder.array().items(elementSchema)

      case MapType(StringType, valueType, _) =>
        val builder = getSchemaBuilder(dataType.asInstanceOf[MapType].valueContainsNull)
        val valueSchema = convertTypeToAvro(valueType, builder, structName, recordNamespace)
        schemaBuilder.map().values(valueSchema)

      case structType: StructType =>
        convertStructToAvro(
          structType,
          schemaBuilder.record(structName).namespace(recordNamespace),
          recordNamespace)

      case other => throw new IllegalArgumentException(s"Unexpected type $dataType.")
    }
  }

  /**
    * This function is used to construct fields of the avro record, where schema of the field is
    * specified by avro representation of dataType. Since builders for record fields are different
    * from those for everything else, we have to use a separate method.
    */
  private def convertFieldTypeToAvro[T](
                                         dataType: DataType,
                                         newFieldBuilder: BaseFieldTypeBuilder[T],
                                         structName: String,
                                         recordNamespace: String): FieldDefault[T, _] = {
    dataType match {
      case ByteType => newFieldBuilder.intType()
      case ShortType => newFieldBuilder.intType()
      case IntegerType => newFieldBuilder.intType()
      case LongType => newFieldBuilder.longType()
      case FloatType => newFieldBuilder.floatType()
      case DoubleType => newFieldBuilder.doubleType()
      case _: DecimalType => newFieldBuilder.stringType()
      case StringType => newFieldBuilder.stringType()
      case BinaryType => newFieldBuilder.bytesType()
      case BooleanType => newFieldBuilder.booleanType()
      case TimestampType => newFieldBuilder.longType()

      case ArrayType(elementType, _) =>
        val builder = getSchemaBuilder(dataType.asInstanceOf[ArrayType].containsNull)
        val elementSchema = convertTypeToAvro(elementType, builder, structName, recordNamespace)
        newFieldBuilder.array().items(elementSchema)

      case MapType(StringType, valueType, _) =>
        val builder = getSchemaBuilder(dataType.asInstanceOf[MapType].valueContainsNull)
        val valueSchema = convertTypeToAvro(valueType, builder, structName, recordNamespace)
        newFieldBuilder.map().values(valueSchema)

      case structType: StructType =>
        convertStructToAvro(
          structType,
          newFieldBuilder.record(structName).namespace(recordNamespace),
          recordNamespace)

      case other => throw new IllegalArgumentException(s"Unexpected type $dataType.")
    }
  }

  private def getSchemaBuilder(isNullable: Boolean): BaseTypeBuilder[Schema] = {
    if (isNullable) {
      SchemaBuilder.builder().nullable()
    } else {
      SchemaBuilder.builder()
    }
  }
  /**
    * This function constructs converter function for a given sparkSQL datatype. This is used in
    * writing Avro records out to disk
    */
  def createConverterToAvro(
                             dataType: DataType,
                             structName: String,
                             recordNamespace: String): (Any) => Any = {
    dataType match {
      case BinaryType => (item: Any) => item match {
        case null => null
        case bytes: Array[Byte] => ByteBuffer.wrap(bytes)
      }
      case ByteType | ShortType | IntegerType | LongType |
           FloatType | DoubleType | StringType | BooleanType => identity
      case _: DecimalType => (item: Any) => if (item == null) null else item.toString
      case TimestampType => (item: Any) =>
        if (item == null) null else item.asInstanceOf[Timestamp].getTime
      case ArrayType(elementType, _) =>
        val elementConverter = createConverterToAvro(elementType, structName, recordNamespace)
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val sourceArray = item.asInstanceOf[Seq[Any]]
            val sourceArraySize = sourceArray.size
            val targetArray = new util.ArrayList[Any](sourceArraySize)
            var idx = 0
            while (idx < sourceArraySize) {
              targetArray.add(elementConverter(sourceArray(idx)))
              idx += 1
            }
            targetArray
          }
        }
      case MapType(StringType, valueType, _) =>
        val valueConverter = createConverterToAvro(valueType, structName, recordNamespace)
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val javaMap = new HashMap[String, Any]()
            item.asInstanceOf[Map[String, Any]].foreach { case (key, value) =>
              javaMap.put(key, valueConverter(value))
            }
            javaMap
          }
        }
      case structType: StructType =>
        val builder = SchemaBuilder.record(structName).namespace(recordNamespace)
        val schema: Schema = SchemaConverters.convertStructToAvro(
          structType, builder, recordNamespace)
        val fieldConverters = structType.fields.map(field =>
          createConverterToAvro(field.dataType, field.name, recordNamespace))
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val record = new Record(schema)
            val convertersIterator = fieldConverters.iterator
            val fieldNamesIterator = dataType.asInstanceOf[StructType].fieldNames.iterator
            val rowIterator = item.asInstanceOf[Row].toSeq.iterator

            while (convertersIterator.hasNext) {
              val converter = convertersIterator.next()
              record.put(fieldNamesIterator.next(), converter(rowIterator.next()))
            }
            record
          }
        }
    }
  }
}


object AvroSerdes {
  // We only handle top level is record or primary type now
  def serialize(input: Any, schema: Schema): Array[Byte]= {
    schema.getType match {
      case BOOLEAN => Bytes.toBytes(input.asInstanceOf[Boolean])
      case BYTES | FIXED=> input.asInstanceOf[Array[Byte]]
      case DOUBLE => Bytes.toBytes(input.asInstanceOf[Double])
      case FLOAT => Bytes.toBytes(input.asInstanceOf[Float])
      case INT => Bytes.toBytes(input.asInstanceOf[Int])
      case LONG => Bytes.toBytes(input.asInstanceOf[Long])
      case STRING => Bytes.toBytes(input.asInstanceOf[String])
      case RECORD =>
        val gr = input.asInstanceOf[GenericRecord]
        val writer2 = new GenericDatumWriter[GenericRecord](schema)
        val bao2 = new ByteArrayOutputStream()
        val encoder2: BinaryEncoder = EncoderFactory.get().directBinaryEncoder(bao2, null)
        writer2.write(gr, encoder2)
        bao2.toByteArray()
      case _ => throw new Exception(s"unsupported data type ${schema.getType}") //TODO
    }
  }

  def deserialize(input: Array[Byte], schema: Schema): GenericRecord = {
    val reader2: DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord](schema)
    val bai2 = new ByteArrayInputStream(input)
    val decoder2: BinaryDecoder = DecoderFactory.get().directBinaryDecoder(bai2, null)
    val gr2: GenericRecord = reader2.read(null, decoder2)
    gr2
  }
}
