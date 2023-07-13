#!/bin/sh

# Actualizamos instancia
sudo apt update && sudo apt upgrade -y

# Instalamos la versión de Java requerida para Kafka
sudo apt install -y openjdk-8-jdk

# Instalamos Python
sudo apt install -y python3

# Librería para Kafka
pip3 install kafka-python==2.0.2

# Descargamos Kafka y descomprimimos el paquete
wget https://dlcdn.apache.org/kafka/3.4.1/kafka_2.13-3.4.1.tgz
tar -xvf kafka_2.13-3.4.1.tgz

# Renombramos el directorio de Kafka
rm -f kafka_2.13-3.4.1.tgz && mv kafka_2.13-3.4.1/ kafka

cd kafka
mkdir -p data/kafka
mkdir -p data/zookeeper

echo 'listeners=PLAINTEXT://:9092' >> config/server.properties
echo 'advertised.listeners=PLAINTEXT://4.150.185.5:9092' >> config/server.properties

export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
export PATH="/usr/local/bin/scala-2.13.3/bin:/usr/local/bin/kafka/bin":$PATH

nohup bin/zookeeper-server-start.sh config/zookeeper.properties &

sleep 20

nohup bin/kafka-server-start.sh config/server.properties &

sleep 10

nohup bin/kafka-topics.sh --create --partitions 1 --replication-factor 1 --topic comfama.beneficiarios --bootstrap-server localhost:9092 &

sleep 10

nohup bin/kafka-topics.sh --create --partitions 1 --replication-factor 1 --topic comfama.empresas --bootstrap-server localhost:9092 &

sleep 10

nohup bin/kafka-topics.sh --create --partitions 1 --replication-factor 1 --topic comfama.titulares --bootstrap-server localhost:9092 &

sleep 10