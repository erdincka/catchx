FROM maprtech/pacc:8.0.0_10.0.0_ubuntu20_yarn_hbase_hive_spark_streams_fuse

# --- Build for dfclient
ARG MAPR_REPO=https://package.ezmeral.hpe.com/releases

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# ENV MAPR_USER=mapr \
#     MAPR_GROUP=mapr \
#     MAPR_PASSWORD=mapr \
#     MAPR_HOME=/opt/mapr \
#     MAPR_UID=5000

ENV DEBIAN_FRONTEND=noninteractive

# RUN apt update && apt -qq upgrade -y && apt -qq install -y ca-certificates locales syslinux syslinux-utils tzdata
# ENV SHELL=/bin/bash \
#     LC_ALL=en_US.UTF-8 \
#     LANG=en_US.UTF-8 \
#     LANGUAGE=en_US.UTF-8
# RUN locale-gen $LC_ALL

RUN apt update && apt -qq install -y --no-install-recommends gnupg2 python3-dev \
    gcc openjdk-17-jdk openssh-client nfs-common build-essential netbase curl
  # lsb-core libcurl3-gnutls

# ## Ensure user
# RUN useradd -u ${MAPR_UID} -U -m -d ${MAPR_HOME} -s /bin/bash -G sudo ${MAPR_USER}
# RUN echo "${MAPR_USER}:${MAPR_PASSWORD}" | chpasswd
# RUN echo "root:${MAPR_PASSWORD}" | chpasswd

# # Install the mapr packages
# # COPY mapr.list /etc/apt/sources.list.d/mapr.list
# RUN echo "deb ${MAPR_REPO}/v8.1.0/ubuntu/ binary bionic" > /etc/apt/sources.list.d/mapr.list
# RUN echo "deb ${MAPR_REPO}/MEP/MEP-10.1.0/ubuntu/ binary bionic" >> /etc/apt/sources.list.d/mapr.list
# # Replace repo
# # RUN sed -i "s|https://package.ezmeral.hpe.com/releases|${MAPR_REPO}|g" /etc/apt/sources.list.d/mapr.list
# RUN curl -L -o /tmp/mapr.key "${MAPR_REPO}/pub/maprgpg.key"
# RUN apt-key add /tmp/mapr.key
RUN apt update && apt -qq install -y sshpass git
# Cleanup
RUN rm -rf /var/lib/apt/lists/*

# # Enable sudo for mapr
# RUN echo "${MAPR_USER} ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers.d/mapr

# # Enable streams and db clients
# RUN mkdir /mapr

# RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV LD_LIBRARY_PATH=/opt/mapr/lib
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV CFLAGS=-I/opt/mapr/include
ENV LDFLAGS=-L/opt/mapr/lib

# # Clean local repo
# RUN rm -f /tmp/mapr.key # /etc/apt/sources.list.d/mapr.list
# RUN apt clean

WORKDIR /app
COPY . /app/
# COPY maprdb_python_client-1.1.7-py3-none-any.whl /
RUN uv venv
RUN uv pip install ./maprdb_python_client-1.1.7-py3-none-any.whl
RUN uv pip install mapr-streams-python

# --- App specific

RUN uv pip install -r /app/requirements.txt
RUN uv pip install 'protobuf==3.20.*'

ENV LD_LIBRARY_PATH=/opt/mapr/lib
EXPOSE 8000

ENTRYPOINT [ "/bin/bash" ]
CMD ["-c", "/app/.venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000 --reload"]
# CMD ["/app/.venv/bin/uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
