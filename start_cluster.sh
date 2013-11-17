#!/bin/bash

./s4 newCluster -c=cluster1 -nbTasks=2 -flp=12000; ./s4 newCluster -c=cluster2 -nbTasks=1 -flp=13000

./s4 newCluster -c=cluster3 -nbTasks=1 -flp=14000
