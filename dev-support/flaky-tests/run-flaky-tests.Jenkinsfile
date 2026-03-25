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
    dockerfile {
      dir 'dev-support/docker'
      label 'hbase'
      args '-v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro'
    }
  }
  triggers {
    cron('H H/12 * * *') // See https://jenkins.io/doc/book/pipeline/syntax/#cron-syntax
  }
  options {
    // this should roughly match how long we tell the flaky dashboard to look at
    buildDiscarder(logRotator(numToKeepStr: '50'))
    timeout (time: 2, unit: 'HOURS')
    timestamps()
  }
  environment {
    ASF_NIGHTLIES = 'https://nightlies.apache.org'
    JAVA_HOME = '/usr/lib/jvm/java-17'
  }
  parameters {
    booleanParam(name: 'DEBUG', defaultValue: false, description: 'Produce a lot more meta-information.')
  }
  stages {
    stage ('run flaky tests') {
      steps {
        sh '''#!/usr/bin/env bash
          set -e
          MVN="${MAVEN_HOME}/bin/mvn"
          # print the maven version and java version
          ${MVN} --version
          declare -a curl_args=(--fail)
          tmpdir=$(realpath target)
          declare -a mvn_args=(--batch-mode -fn -Dbuild.id="${BUILD_ID}" -Dmaven.repo.local="${WORKSPACE}/local-repository" -Djava.io.tmpdir=${tmpdir})
          if [ "${DEBUG}" = "true" ]; then
            curl_args=("${curl_args[@]}" -v)
            mvn_args=("${mvn_args[@]}" -X)
            set -x
          fi
          # need to build against hadoop 3.0 profile for branch-2 when using jdk 11+
          if [[ "${BRANCH_NAME}" == *"branch-2"* ]]; then
            mvn_args=("${mvn_args[@]}" -Dhadoop.profile=3.0)
          fi
          curl "${curl_args[@]}" -o includes.txt "${JENKINS_URL}/job/HBase-Find-Flaky-Tests/job/${BRANCH_NAME}/lastSuccessfulBuild/artifact/output/includes"
          if [ -s includes.txt ]; then
            rm -rf local-repository/org/apache/hbase
            ${MVN} clean "${mvn_args[@]}"
            rm -rf "target/machine" && mkdir -p "target/machine"
            if [ -x dev-support/gather_machine_environment.sh ]; then
              "./dev-support/gather_machine_environment.sh" "target/machine"
              echo "got the following saved stats in 'target/machine'"
              ls -lh "target/machine"
            else
              echo "Skipped gathering machine environment because we couldn't read the script to do so."
            fi
            ${MVN} -T0.25C package "${mvn_args[@]}" -Dtest="$(cat includes.txt)" -Dmaven.test.redirectTestOutputToFile=true -Dsurefire.firstPartForkCount=0.25C -Dsurefire.secondPartForkCount=0.25C
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
      sshPublisher(publishers: [
        sshPublisherDesc(configName: 'Nightlies',
          transfers: [
            sshTransfer(remoteDirectory: "hbase/${JOB_NAME}/${BUILD_NUMBER}",
              sourceFiles: "**/surefire-reports/*,**/test-data/*"
            )
          ]
        )
      ])
      sh '''#!/bin/bash -e
        ./dev-support/gen_redirect_html.py "${ASF_NIGHTLIES}/hbase/${JOB_NAME}/${BUILD_NUMBER}" > test_logs.html
      '''
      archiveArtifacts artifacts: 'includes.txt,test_logs.html,target/machine/*'
    }
  }
}
