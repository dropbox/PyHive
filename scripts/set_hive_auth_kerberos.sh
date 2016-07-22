sudo cp $(dirname $0)/travis-conf/hive/hive-site-kerberos.xml /etc/hive/conf/hive-site.xml

sudo service hive-server2 restart