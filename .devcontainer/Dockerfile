FROM --platform=linux/amd64 docker.m.daocloud.io/library/ubuntu:24.04

ENV HADOOP_VERSION=3.4.3
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$JAVA_HOME/lib/server

USER root

# 安装 JDK 和必要工具
RUN apt-get update && apt-get install -y \
    wget \
    ssh \
    rsync \
    gdb \
    openjdk-8-jdk \
    libgtest-dev libfmt-dev \
    && rm -rf /var/lib/apt/lists/*

# Install necessary packages
RUN apt update && \
    apt install -y \
    bash-completion \
    build-essential \
    ccache \
    cmake \
    curl \
    gcc \
    g++ \
    git \
    htop \
    libboost-all-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    lsb-release \
    meson \
    ninja-build \
    pkg-config \
    python3 \
    python3-pip \
    vim \
    wget \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# 安装 openssh-server
RUN apt-get update && apt-get install -y openssh-server && \
    # 创建 sshd 运行目录（Ubuntu 必须）
    mkdir /var/run/sshd && \
    # 允许 root 登录
    echo "PermitRootLogin yes" >> /etc/ssh/sshd_config && \
    # 修改加密配置（可选，确保旧版 CLion 能连上）
    echo "PasswordAuthentication yes" >> /etc/ssh/sshd_config && \
    # 设置 root 密码
    echo "root:123456" | chpasswd && \
    # 清理缓存
    rm -rf /var/lib/apt/lists/*

# 下载并解压 Hadoop（使用 Apache CDN）
RUN wget -q https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ \
    && mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} \
    && rm hadoop-${HADOOP_VERSION}.tar.gz

# 安装 libhdfs。
RUN ln -s ${HADOOP_HOME}/include/hdfs.h /usr/local/include/hdfs.h && \
    ln -s ${HADOOP_HOME}/lib/native/libhdfs.so /usr/local/lib/libhdfs.so && \
    echo "${JAVA_HOME}/lib/server" > /etc/ld.so.conf.d/openjdk.conf && \
    ldconfig

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
    echo '    <value>hdfs://0.0.0.0:9000</value>' >> ${HADOOP_HOME}/etc/hadoop/core-site.xml && \
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
	echo "JAVA_HOME=\"$JAVA_HOME\"" >> /etc/environment && \
	echo "HADOOP_HOME=\"$HADOOP_HOME\"" >> /etc/environment && \
    echo "LD_LIBRARY_PATH=\"$LD_LIBRARY_PATH\"" >> /etc/environment && \
    echo "CLASSPATH=\"$CLASSPATH_VALUE\"" >> /etc/environment

EXPOSE 9870 9000 22

CMD ["bash", "-c", "\
    # 1. 确保 SSH 服务真正启动（Hadoop 免密登录的前提）
    mkdir -p /var/run/sshd && /usr/sbin/sshd && \
    sleep 2 && \
    # 2. 显式加载环境（确保 hdfs 命令在 PATH 中）
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 && \
    export HADOOP_HOME=/opt/hadoop && \
    export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin && \
    # 3. 格式化判断
    if [ ! -d '/data/hdfs/namenode/current' ]; then \
        echo '=== First time startup: Formatting NameNode ===' && \
        hdfs namenode -format -force -nonInteractive; \
    else \
        echo '=== NameNode already formatted, skipping format ==='; \
    fi && \
    # 4. 启动 HDFS (内部会通过 SSH 调用 localhost)
    ${HADOOP_HOME}/sbin/start-dfs.sh && \
    echo '=== HDFS Cluster Started Successfully ===' && \
    jps && \
    # 5. 保持进程前台运行
    tail -f /dev/null \
"]