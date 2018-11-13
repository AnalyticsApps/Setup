#!/bin/sh

rm -rf src/tmp/*
rm -rf src/bin/*
rm -rf src/lib/*

# Move the package to resources
cp ClusterSetup/target/universal/cluster-setup-1.0-SNAPSHOT.zip src/tmp
cp BigSQLSetup/target/universal/bigsql-setup-1.0-SNAPSHOT.zip src/tmp 

unzip src/tmp/cluster-setup-1.0-SNAPSHOT.zip -d src/tmp
unzip src/tmp/bigsql-setup-1.0-SNAPSHOT.zip -d src/tmp

cp -R src/tmp/cluster-setup-1.0-SNAPSHOT/bin src/
cp -R src/tmp/cluster-setup-1.0-SNAPSHOT/lib src/

cp -R src/tmp/bigsql-setup-1.0-SNAPSHOT/bin/* src/bin
cp src/tmp/bigsql-setup-1.0-SNAPSHOT/lib/com.createCluster.bigsql-setup-1.0-SNAPSHOT.jar src/lib
rm -rf src/bin/*.bat

rm -rf src/tmp/*
