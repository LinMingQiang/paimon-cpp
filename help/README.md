# 1. 构建镜像
docker build -t paimon-hdfs:v1 -f Dockerfile .

DOCKER_BUILDKIT=0 docker build -t paimon-hdfs:v8 -f Dockerfile .


# 2. 运行容器
这个主要是为了，容器之间可以共享这个 hdfs 数据，这样容器删了也没事，数据都还在
-v /Users/john.wick/CLionProjects/share/paimon-cpp-dev/data:/data/hdfs

docker run -d --platform linux/amd64 \
--name paimon-dev-hdfs \
--hostname hdfs-master \
-p 9870:9870 \
-p 9000:9000 \
-p 2222:22 \
-p 8020:8020 \
-v /Users/john.wick/CLionProjects/share:/share \
-v /Users/john.wick/CLionProjects/share/data:/data/hdfs \
paimon-hdfs:v8


docker run -d --platform linux/amd64 \
--name paimon-dev-hdfs \
--hostname hdfs-master \
-p 9970:9870 \
-p 9900:9000 \
-p 2222:22 \
-p 8020:8020 \
-v /Users/hunter/CLionProjects/share:/share \
-v /Users/hunter/CLionProjects/share/paimon-cpp-debug:/share/paimon-cpp-debug \
-v /Users/hunter/CLionProjects/share/data:/data/hdfs \
paimon-hdfs:v1


# 3. 两种镜像
1：给源码开发的镜像，不安装 arrow 等
2：给类似当前项目的，需要安装 arrow，lz4，parquet 等