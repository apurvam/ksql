#!/usr/bin/env groovy

dockerfile {
    slackChannel = '#ksql-eng'
    upstreamProjects = 'confluentinc/schema-registry'
    dockerRepos = ['confluentinc/ksql-examples', 'confluentinc/ksql-cli', 'confluentinc/ksql-clickstream-demo', 'confluentinc/cp-ksql-server']
    extraBuildArgs = '-Ddocker.skip=false'
    extraDeployArgs = '-Ddocker.skip=true'
}
