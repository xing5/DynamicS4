#!/bin/bash

rm -rf test-apps/twitter-*/build/
./s4 s4r -b=`pwd`/test-apps/twitter-adapter/build.gradle -appClass=org.apache.s4.example.twitter.TwitterInputAdapter twitter-adapter &&  ./s4 s4r -b=`pwd`/test-apps/twitter-counter/build.gradle -appClass=org.apache.s4.example.twitter.TwitterCounterApp twitter-counter

