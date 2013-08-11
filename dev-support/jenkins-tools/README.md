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

jenkins-tools
=============

A tool which pulls test case results from Jenkins server. It displays a union of failed test cases 
from the last 15(by default and actual number of jobs can be less depending on availablity) runs 
recorded in Jenkins sever and track how each of them are performed for all the last 15 runs(passed, 
not run or failed)

Pre-requirement(run under folder jenkins-tools)
       Please download jenkins-client from https://github.com/cosmin/jenkins-client
       1) git clone git://github.com/cosmin/jenkins-client.git
       2) make sure the dependency jenkins-client version in ./buildstats/pom.xml matches the 
          downloaded jenkins-client(current value is 0.1.6-SNAPSHOT)
       
Build command(run under folder jenkins-tools):

       mvn clean package

Usage are: 

       java -jar ./buildstats/target/buildstats.jar <Jenkins HTTP URL> <Job Name> [number of last most recent jobs to check]

Sample commands are:

       java -jar ./buildstats/target/buildstats.jar https://builds.apache.org HBase-TRUNK

Sample output(where 1 means "PASSED", 0 means "NOT RUN AT ALL", -1 means "FAILED"):

Failed Test Cases              3621 3622 3623 3624 3625 3626 3627 3628 3629 3630 3632 3633 3634 3635

org.apache.hadoop.hbase.catalog.testmetareadereditor.testretrying    1    1   -1    0    1    1    1    1   -1    0    1    1    1    1
org.apache.hadoop.hbase.client.testadmin.testdeleteeditunknowncolumnfamilyandortable    0    1    1    1   -1    0    1    1    0    1    1    1    1    1
org.apache.hadoop.hbase.client.testfromclientsidewithcoprocessor.testclientpoolthreadlocal    1    1    1    1    1    1    1    1    0    1    1   -1    0    1
org.apache.hadoop.hbase.client.testhcm.testregioncaching    1    1   -1    0    1    1   -1    0   -1    0   -1    0    1    1
org.apache.hadoop.hbase.client.testmultiparallel.testflushcommitswithabort    1    1    1    1    1    1    1    1   -1    0    1    1    1    1
org.apache.hadoop.hbase.client.testscannertimeout.test3686a    1    1    1    1    1    1    1    1   -1    0    1    1    1    1
org.apache.hadoop.hbase.coprocessor.example.testrowcountendpoint.org.apache.hadoop.hbase.coprocessor.example.testrowcountendpoint    0   -1    0   -1    0    0    0   -1    0    0    0    0    0    0
org.apache.hadoop.hbase.coprocessor.example.testzookeeperscanpolicyobserver.org.apache.hadoop.hbase.coprocessor.example.testzookeeperscanpolicyobserver    0   -1    0   -1    0    0    0   -1    0    0    0    0    0    0
org.apache.hadoop.hbase.master.testrollingrestart.testbasicrollingrestart    1    1    1    1   -1    0    1    1    1    1    1    1   -1    0
org.apache.hadoop.hbase.regionserver.testcompactionstate.testmajorcompaction    1    1   -1    0    1    1    1    1    1    1    1    1    1    1
org.apache.hadoop.hbase.regionserver.testcompactionstate.testminorcompaction    1    1   -1    0    1    1    1    1    1    1    1    1    1    1
org.apache.hadoop.hbase.replication.testreplication.loadtesting    1    1    1    1    1    1    1    1    1   -1    0    1    1    1
org.apache.hadoop.hbase.rest.client.testremoteadmin.org.apache.hadoop.hbase.rest.client.testremoteadmin    0    0    0    0    0    0    0    0   -1    0    0    0    0    0
org.apache.hadoop.hbase.rest.client.testremotetable.org.apache.hadoop.hbase.rest.client.testremotetable    0    0    0    0    0    0    0    0   -1    0    0    0    0    0
org.apache.hadoop.hbase.security.access.testtablepermissions.testbasicwrite    0    1    1    1    1    1    1    1    1    1    1    1    1   -1
org.apache.hadoop.hbase.testdrainingserver.testdrainingserverwithabort    1    1    1    1    1   -1    0    1    1    1    1    1   -1    0
org.apache.hadoop.hbase.util.testhbasefsck.testregionshouldnotbedeployed    1    1    1    1    1    1   -1    0   -1    0   -1   -1    0   -1


