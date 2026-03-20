FROM paimon-cpp-dev-remote:v3

ENV HADOOP_VERSION=3.4.3
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$JAVA_HOME/lib/server:$LD_LIBRARY_PATH

USER root


# 安装 JDK 和必要工具
RUN apt-get update && apt-get install -y \
    ca-certificates \
    lsb-release \
    wget \
    ssh \
    openjdk-8-jdk \
    && rm -rf /var/lib/apt/lists/*

RUN wget https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && apt-get update \
    && apt-get install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && apt-get update \
    && apt-get install -y -V libparquet-dev \
    && rm apache-arrow-apt-source-latest-*.deb \
    && rm -rf /var/lib/apt/lists/*

# 下载并解压 Hadoop（使用 Apache CDN）
RUN wget -q https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ \
    && mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} \
    && rm hadoop-${HADOOP_VERSION}.tar.gz


# 配置 SSH 无密码登录
RUN mkdir -p /root/.ssh && \
    ssh-keygen -t rsa -P '' -f /root/.ssh/id_rsa && \
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys && \
    chmod 0600 /root/.ssh/authorized_keys && \
    echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config

# 创建持久化数据目录
RUN mkdir -p /data/hdfs/namenode \
    /data/hdfs/datanode \
    /data/hdfs/namesecondary

# 配置 core-site.xml
RUN echo '<configuration>' > ${HADOOP_HOME}/etc/hadoop/core-site.xml && \
    echo '  <property>' >> ${HADOOP_HOME}/etc/hadoop/core-site.xml && \
    echo '    <name>fs.defaultFS</name>' >> ${HADOOP_HOME}/etc/hadoop/core-site.xml && \
    echo '    <value>hdfs://localhost:9000</value>' >> ${HADOOP_HOME}/etc/hadoop/core-site.xml && \
    echo '  </property>' >> ${HADOOP_HOME}/etc/hadoop/core-site.xml && \
    echo '</configuration>' >> ${HADOOP_HOME}/etc/hadoop/core-site.xml

# 配置 hdfs-site.xml
RUN echo '<?xml version="1.0" encoding="UTF-8"?>' > ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '<configuration>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '  <property>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '    <name>dfs.namenode.name.dir</name>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '      <value>file:///data/hdfs/namenode</value>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '  </property>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '  <property>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '    <name>dfs.datanode.data.dir</name>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '      <value>file:///data/hdfs/datanode</value>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '  </property>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '  <property>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '    <name>dfs.replication</name>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '    <value>1</value>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '  </property>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '  <property>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '    <name>dfs.permissions.enabled</name>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '    <value>false</value>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '  </property>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '</configuration>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml

# 配置 workers 文件
RUN echo "localhost" > ${HADOOP_HOME}/etc/hadoop/workers

# 配置全局环境变量到 /etc/profile
RUN echo "" >> /etc/profile && \
    echo "# Hadoop environment variables" >> /etc/profile && \
    echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> /etc/profile && \
    echo "export HADOOP_HOME=/opt/hadoop" >> /etc/profile && \
    echo "export PATH=\${PATH}:\${HADOOP_HOME}/bin:\${HADOOP_HOME}/sbin" >> /etc/profile && \
    echo "export HDFS_NAMENODE_USER=root" >> /etc/profile && \
    echo "export HDFS_DATANODE_USER=root" >> /etc/profile && \
    echo "export HDFS_SECONDARYNAMENODE_USER=root" >> /etc/profile

# 配置 hadoop-env.sh
RUN echo "" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh && \
    echo "# Custom configuration for container" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh && \
    echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_NAMENODE_USER=root" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_DATANODE_USER=root" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_SECONDARYNAMENODE_USER=root" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh

RUN echo "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH" >> /etc/bash.bashrc && \
    echo "export CLASSPATH=\$($HADOOP_HOME/bin/hadoop classpath):\$CLASSPATH" >> /etc/bash.bashrc

RUN CLASSPATH_VALUE=$($HADOOP_HOME/bin/hadoop classpath) && \
    echo "LD_LIBRARY_PATH=\"$LD_LIBRARY_PATH\"" >> /etc/environment && \
    echo "CLASSPATH=\"$CLASSPATH_VALUE\"" >> /etc/environment

EXPOSE 9870 9000 22

CMD ["bash", "-c", "\
    source /etc/profile && \
    service ssh start && \
    sleep 2 && \
    if [ ! -d '/data/hdfs/namenode/current' ]; then \
        echo '=== First time startup: Formatting NameNode ===' && \
        hdfs namenode -format -force -nonInteractive; \
    else \
        echo '=== NameNode already formatted, skipping format ==='; \
    fi && \
    ${HADOOP_HOME}/sbin/start-dfs.sh && \
    echo '=== HDFS Cluster Started Successfully ===' && \
    jps && \
    tail -f /dev/null \
"]