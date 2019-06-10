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
    cron('@daily')
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '50'))
    timeout (time: 15, unit: 'MINUTES')
    timestamps()
  }
  parameters {
    booleanParam(name: 'DEBUG', defaultValue: false, description: 'Produce a lot more meta-information.')
  }
  stages {
    stage ('build flaky report') {
      steps {
        sh '''#!/usr/bin/env bash
          set -e
          if [ "${DEBUG}" = "true" ]; then
            set -x
          fi
          declare -a flaky_args
          flaky_args=("${flaky_args[@]}" --urls "${JENKINS_URL}/job/HBase%20Nightly/job/${BRANCH_NAME}" --is-yetus True --max-builds 5)
          flaky_args=("${flaky_args[@]}" --urls "${JENKINS_URL}/job/HBase-Flaky-Tests/job/${BRANCH_NAME}" --is-yetus False --max-builds 30)
          docker build -t hbase-dev-support dev-support
          docker run -v "${WORKSPACE}":/hbase --workdir=/hbase hbase-dev-support python dev-support/flaky-tests/report-flakies.py --mvn -v "${flaky_args[@]}"
'''
      }
    }
  }
  post {
    always {
      // Has to be relative to WORKSPACE.
      archive "includes,excludes,dashboard.html"
      publishHTML target: [
        allowMissing: true,
        keepAll: true,
        alwaysLinkToLastBuild: true,
        // Has to be relative to WORKSPACE
        reportDir: ".",
        reportFiles: 'dashboard.html',
        reportName: 'Flaky Test Report'
      ]
    }
  }
}
