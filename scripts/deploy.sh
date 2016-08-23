#!/bin/bash

echo "TRAVIS_BRANCH=$TRAVIS_BRANCH"

if [[ "$TRAVIS_PULL_REQUEST" = "false" && "$TRAVIS_BRANCH" = "master" ]]; then 
  mvn deploy --settings settings.xml -DperformRelease=true -DskipTests=true 
else 
  echo 'Pull request, skipping deploy.';
fi

#- if [[ "$TRAVIS_PULL_REQUEST" = "false" ]]; then mvn deploy --settings settings.xml
#  -DperformRelease=true -DskipTests=true; mvn javadoc:javadoc scm-publish:publish-scm; else echo 'Pull request, skipping deploy.';
#  fi
