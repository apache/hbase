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

import java.io.Serializable

import org.apache.yetus.audience.InterfaceAudience;

/**
 * This object will hold optional data for how a given column family's
 * writer will work
 *
 * @param compression       String to define the Compression to be used in the HFile
 * @param bloomType         String to define the bloom type to be used in the HFile
 * @param blockSize         The block size to be used in the HFile
 * @param dataBlockEncoding String to define the data block encoding to be used
 *                          in the HFile
 */
@InterfaceAudience.Public
class FamilyHFileWriteOptions( val compression:String,
                               val bloomType: String,
                               val blockSize: Int,
                               val dataBlockEncoding: String) extends Serializable
