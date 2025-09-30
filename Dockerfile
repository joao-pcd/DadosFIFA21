FROM apache/spark:4.1.0-preview2-scala2.13-java21-python3-r-ubuntu

USER root
RUN apt-get update && apt-get install -y wget unzip python3-pip && rm -rf /var/lib/apt/lists/*
RUN pip3 install pyspark
# MinIo Client
RUN wget https://dl.min.io/client/mc/release/linux-amd64/mc -O /usr/bin/mc && chmod +x /usr/bin/mc

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