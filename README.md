# ozone_test_client
Sample demonstrating the problem with writing data to apache/ozone storage

### Steps to reproduce

#### clone and build latest apache/ozone (1.4.0-SNAPSHOT)
1. git clone https://github.com/apache/ozone.git
2. run ```mvn clean install -DskipTests```

   
#### start pseudo cluster in docker
```
cd hadoop-ozone/dist/target/ozone-*-SNAPSHOT/compose/ozone
OZONE_REPLICATION_FACTOR=3 ./run.sh -d
```

#### open ozone_test_client project
1. Tun main class
