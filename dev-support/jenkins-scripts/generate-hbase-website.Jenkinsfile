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
      label 'git-websites'
    }
  }
  triggers {
    pollSCM('@daily')
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '30'))
    timeout (time: 1, unit: 'HOURS')
    timestamps()
    skipDefaultCheckout()
    disableConcurrentBuilds()
  }
  parameters {
    booleanParam(name: 'DEBUG', defaultValue: false, description: 'Produce a lot more meta-information.')
    booleanParam(name: 'FORCE_FAIL', defaultValue: false, description: 'force a failure to test notifications.')
  }
  stages {
    stage ('generate hbase website') {
      tools {
        maven 'maven_latest'
        // this needs to be set to the jdk that ought to be used to build releases on the branch the Jenkinsfile is stored in.
        jdk "jdk_17_latest"
      }
      steps {
        dir('hbase') {
          script {
            checkout([
              $class: 'GitSCM',
              branches: [[name: '*/master']], // 指定分支
              doGenerateSubmoduleConfigurations: false,
              extensions: [
                [$class: 'CloneOption', 
                  noTags: true,
                  shallow: true,
                  depth: 1
                ],
                [$class: 'CheckoutOption', timeout: 20]
              ],
              userRemoteConfigs: [[url: 'https://github.com/apache/hbase']]
            ])
          }
        }
        sh '''#!/usr/bin/env bash
          set -e
          if [ "${DEBUG}" = "true" ]; then
            set -x
          fi
          if [ "${FORCE_FAIL}" = "true" ]; then
            false
          fi
          bash hbase/dev-support/jenkins-scripts/generate-hbase-website.sh --working-dir "${WORKSPACE}" --publish hbase
'''
      }
    }
  }
  post {
    always {
      // Has to be relative to WORKSPACE.
      archiveArtifacts artifacts: '*.patch.zip,hbase-*.txt'
    }
    failure {
      mail to: 'dev@hbase.apache.org', replyTo: 'dev@hbase.apache.org', subject: "Failure: HBase Generate Website", body: """
Build status: ${currentBuild.currentResult}

The HBase website has not been updated to incorporate recent HBase changes.

See ${env.BUILD_URL}console
"""
    }
    cleanup {
      deleteDir()
    }
  }
}
