rm -rf pigeon40_distribution

mkdir pigeon40_distribution
mkdir pigeon40_distribution/server
cp -R pigeon-server/target/pigeonserver.4.0-SNAPSHOT-distribution/* pigeon40_distribution/server

mkdir pigeon40_distribution/pigeon-admin
cp  pigeon-admin/target/pigeonadmin.4.0-distribution/* pigeon40_distribution/pigeon-admin
cp pigeon-admin/src/configs/* pigeon40_distribution/pigeon-admin
cp pigeon-admin/src/main/shellscripts/* pigeon40_distribution/pigeon-admin

mkdir pigeon40_distribution/pigeon-client
cp pigeon-client/target/*.jar pigeon40_distribution/pigeon-client