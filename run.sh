#!/bin/sh
mvn clean install
#java -jar target/microbenchmarks.jar -tu s -f 2 -wi 1 -i 4 -rf json -rff results.json -bm thrpt ".*MultiNodeMapBenchmark.*" -prof stack
java -jar target/microbenchmarks.jar -tu s -f 2 -wi 2 -i 10 -bm thrpt ".*ClassLoadingBenchmark.*"

