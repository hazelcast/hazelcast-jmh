#!/bin/sh
mvn clean install
#java -jar target/microbenchmarks.jar -tu s -f 2 -wi 1 -i 4 -rf json -rff results.json -bm thrpt ".*MultiNodeMapBenchmark.*" -prof stack
#java -jar target/microbenchmarks.jar -tu s -f 2 -wi 2 -i 10 -bm thrpt ".*OnheapSlabBenchmark.*"
java -jar target/microbenchmarks.jar -jvmArgs "-Xms2G -Xmx2G" -tu ms -f 1 -t 12 -w 1s -wi 3 -i 10 -r 3s ".*UtfSerializationBenchmark.*"
