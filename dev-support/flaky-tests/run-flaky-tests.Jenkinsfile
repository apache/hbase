// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
pipeline {
  agent {
    node {
      label 'Hadoop'
    }
  }
  triggers {
    cron('@hourly')
  }
  options {
    // this should roughly match how long we tell the flaky dashboard to look at
    buildDiscarder(logRotator(numToKeepStr: '80'))
    timeout (time: 2, unit: 'HOURS')
    timestamps()
  }
  parameters {
    booleanParam(name: 'DEBUG', defaultValue: false, description: 'Produce a lot more meta-information.')
  }
  tools {
    // this should match what the yetus nightly job for the branch will use
    maven 'Maven (latest)'
    jdk "JDK 1.8 (latest)"
  }
  stages {
    stage ('run flaky tests') {
      steps {
        sh '''#!/usr/bin/env bash
          set -e
          declare -a curl_args=(--fail)
          declare -a mvn_args=(--batch-mode -fn -Dbuild.id="${BUILD_ID}" -Dmaven.repo.local="${WORKSPACE}/local-repository")
          if [ "${DEBUG}" = "true" ]; then
            curl_args=("${curl_args[@]}" -v)
            mvn_args=("${mvn_args[@]}" -X)
            set -x
          fi
          ulimit -a
          rm -rf local-repository/org/apache/hbase
          curl "${curl_args[@]}" -o includes.txt "${JENKINS_URL}/job/HBase-Find-Flaky-Tests/job/${BRANCH_NAME}/lastSuccessfulBuild/artifact/includes"
          if [ -s includes.txt ]; then
            mvn clean package "${mvn_args[@]}" -Dtest="$(cat includes.txt)" -Dmaven.test.redirectTestOutputToFile=true -Dsurefire.firstPartForkCount=3 -Dsurefire.secondPartForkCount=3
          else
            echo "set of flaky tests is currently empty."
          fi
'''
      }
    }
  }
  post {
    always {
      junit testResults: "**/surefire-reports/*.xml", allowEmptyResults: true
      // TODO compress these logs
      archive 'includes.txt,**/surefire-reports/*,**/test-data/*'
    }
  }
}
