#!/bin/bash -eux

echo "deb [arch=amd64] https://archive.cloudera.com/${CDH}/ubuntu/precise/amd64/cdh precise-cdh${CDH_VERSION} contrib
deb-src https://archive.cloudera.com/${CDH}/ubuntu/precise/amd64/cdh precise-cdh${CDH_VERSION} contrib" | sudo tee /etc/apt/sources.list.d/cloudera.list
sudo apt-get update

sudo apt-get install -y oracle-java8-installer python-dev g++ libsasl2-dev maven
sudo update-java-alternatives -s java-8-oracle

#
# LDAP
#
sudo apt-get -y --no-install-suggests --no-install-recommends --force-yes install ldap-utils slapd
sudo mkdir -p /tmp/slapd
sudo slapd -f $(dirname $0)/ldap_config/slapd.conf -h ldap://localhost:3389 &
sleep 10
sudo ldapadd -h localhost:3389 -D cn=admin,dc=example,dc=com -w test -f $(dirname $0)/../pyhive/tests/ldif_data/base.ldif
sudo ldapadd -h localhost:3389 -D cn=admin,dc=example,dc=com -w test -f $(dirname $0)/../pyhive/tests/ldif_data/INITIAL_TESTDATA.ldif

#
# Hive
#

sudo apt-get install -y --force-yes hive
sudo mkdir -p /user/hive
sudo chown hive:hive /user/hive
sudo cp $(dirname $0)/travis-conf/hive/hive-site.xml /etc/hive/conf/hive-site.xml
sudo apt-get install -y --force-yes hive-metastore hive-server2

sleep 5

sudo -Eu hive $(dirname $0)/make_test_tables.sh

#
# Presto
#

sudo apt-get install -y python # Use python2 for presto server

mvn org.apache.maven.plugins:maven-dependency-plugin:3.0.0:copy \
    -Dartifact=com.facebook.presto:presto-server:${PRESTO}:tar.gz \
    -DoutputDirectory=.
tar -x -v -z -f presto-server-*.tar.gz
rm -rf presto-server
mv presto-server-*/ presto-server

cp -r $(dirname $0)/travis-conf/presto presto-server/etc

/usr/bin/python2.7 presto-server/bin/launcher.py start

#
# Python
#

pip install $SQLALCHEMY
pip install -e .
pip install -r dev_requirements.txt
