#!/bin/bash

#rm -rf test-apps/twitter-*/build/
git pull
./s4 s4r -b=`pwd`/test-apps/movies-reco/build.gradle -appClass=ca.concordia.ece.sac.s4app.moviesreco.RecoApp reco-app && ./s4 s4r -b=`pwd`/test-apps/movies-reco/build.gradle -appClass=ca.concordia.ece.sac.s4app.moviesreco.RatingStreamApp ratingstream-app

