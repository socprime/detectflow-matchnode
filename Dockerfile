FROM flink:2.2.0-scala_2.12-java17


USER root

################################################################################
# Install system dependencies
################################################################################
RUN set -ex; \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        python3-dev \
        python3-pip \
        python3-venv \
        gcc \
        g++ \
        curl \
        wget \
        git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && ln -sf /usr/bin/pip3 /usr/bin/pip \
    && pip install --no-cache-dir --break-system-packages --upgrade "setuptools>=82.0.0" \
    && rm -f /usr/lib/python3.*/EXTERNALLY-MANAGED

################################################################################
# Set JAVA_HOME for pemja (auto-detect architecture for ARM64/AMD64)
################################################################################
RUN ARCH=$(dpkg --print-architecture) && \
    ln -sf /usr/lib/jvm/java-17-openjdk-${ARCH} /usr/lib/jvm/java-17
ENV JAVA_HOME=/usr/lib/jvm/java-17

################################################################################
# Install uv package manager
################################################################################
COPY --from=ghcr.io/astral-sh/uv:0.5.24 /uv /uvx /bin/

################################################################################
# Install Python dependencies
################################################################################
# Copy only dependency files first for better layer caching
WORKDIR /opt/flink/usrlib
COPY pyproject.toml uv.lock ./

# Install dependencies system-wide (no venv needed in container)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system --no-cache -r pyproject.toml

################################################################################
# Install Flink Kafka Connector
################################################################################
# Copy Kafka connector to Flink classpath (automatically loaded by Flink)
COPY lib/flink-sql-connector-kafka-4.0.1-2.0.jar /opt/flink/lib/

################################################################################
# Copy application code
################################################################################
# Copy application code last for optimal cache invalidation
COPY app /opt/flink/usrlib/app
COPY log4j2.properties /opt/flink/usrlib/log4j2.properties

################################################################################
# Create directories and set permissions
################################################################################
RUN mkdir -p \
        /opt/flink/checkpoints \
        /opt/flink/log \
        /opt/flink/usrlib/logs \
    && chown -R flink:flink \
        /opt/flink/checkpoints \
        /opt/flink/log \
        /opt/flink/usrlib

################################################################################
# Set environment variables
################################################################################
ENV PYTHONPATH="/opt/flink/usrlib:$PYTHONPATH"
ENV PYTHONUNBUFFERED=1

################################################################################
# Switch to flink user for security
################################################################################
USER flink

# Set working directory to Flink home
WORKDIR /opt/flink

# Expose Flink WebUI and RPC ports
EXPOSE 8081 6123

################################################################################
# Note: Kubernetes Operator manages the entrypoint
# No CMD or ENTRYPOINT needed - operator uses PythonDriver
################################################################################
