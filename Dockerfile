FROM apache/spark:3.5.7-scala2.12-java11-python3-r-ubuntu

USER root
RUN rm -rf /root/.ivy2 /root/.m2 /root/.cache
RUN apt-get update && apt-get install -y wget unzip python3-pip && rm -rf /var/lib/apt/lists/*
RUN pip3 install --upgrade pip \
    && pip3 install pyspark minio

# MinIo Client
RUN wget https://dl.min.io/client/mc/release/linux-amd64/mc -O /usr/bin/mc && chmod +x /usr/bin/mc

# Baixar Hadoop AWS e dependências
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P $SPARK_HOME/jars/ \
    && wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -P $SPARK_HOME/jars/

# Criar diretório /workspace antes de mudar permissões
RUN mkdir -p /workspace

# Criação de usuário não-root
ARG USERNAME=dev
ARG USER_UID=1001
ARG USER_GID=1001
RUN groupadd --gid $USER_GID $USERNAME && \
    useradd --uid $USER_UID --gid $USER_GID -m $USERNAME && \
    chown -R $USER_UID:$USER_GID /workspace

# Definir usuário não-root
USER $USERNAME
WORKDIR /workspace
CMD ["bash"]