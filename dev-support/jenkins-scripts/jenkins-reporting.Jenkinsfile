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
      label 'master'
    }
  }
  triggers {
    cron('@daily')
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '30'))
    timeout (time: 1, unit: 'HOURS')
    timestamps()
    skipDefaultCheckout()
    disableConcurrentBuilds()
  }
  stages {
    stage ('generate hbase website') {
      steps {
        dir('hbase') {
          checkout scm
        }
        sh '''#!/usr/bin/env bash
          set -e
          printenv
          bash hbase/dev-support/jenkins-scripts/jenkins-reporting.sh --working-dir "${WORKSPACE}" --output report.html --base-dir "${WORKSPACE}/../.."
'''
      }
    }
  }
  post {
    success {
      publishHTML target: [
        allowMissing: false,
        keepAll: true,
        alwaysLinkToLastBuild: true,
        // Has to be relative to WORKSPACE
        reportDir: ".",
        reportFiles: 'report.html',
        reportName: 'Build Usage Report'
      ]
    }
//     failure {
//       mail to: 'dev@hbase.apache.org', replyTo: 'dev@hbase.apache.org', subject: "Failure: HBase Build Reporting", body: """
// Build status: ${currentBuild.currentResult}
// 
// The HBase build reporting failed to complete properly.
// 
// See ${env.BUILD_URL}console
// """
//     }
    cleanup {
      deleteDir()
    }
  }
}
