#!/bin/bash

./s4 deploy -appName=twitter-counter -c=cluster1 -s4r=`pwd`/test-apps/twitter-counter/build/libs/twitter-counter.s4r
./s4 deploy -appName=twitter-counter -c=cluster3 -s4r=`pwd`/test-apps/twitter-counter/build/libs/twitter-counter.s4r
./s4 deploy -appName=twitter-adapter -c=cluster2 -s4r=`pwd`/test-apps/twitter-adapter/build/libs/twitter-adapter.s4r -p=s4.adapter.output.stream=RawStatus
