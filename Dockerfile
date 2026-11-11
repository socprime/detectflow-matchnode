FROM flink:2.2.0-scala_2.12-java17

# Note: Version is read from pyproject.toml via importlib.metadata at runtime

USER root


RUN set -ex; apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    python3-pip \
    curl \
    wget \
    git \
    libjemalloc2 \
    libpython3.12-dev \
    gcc && \
    jemalloc_path="$(find /usr/lib -name libjemalloc.so.2 -print -quit)" && \
    ln -sf "${jemalloc_path}" /usr/lib/libjemalloc.so.2 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN set -ex; \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    ln -sf /usr/bin/pip3 /usr/bin/pip && \
    pip install --no-cache-dir --break-system-packages "setuptools==84.2.1" && \
    rm -f /usr/lib/python3.*/EXTERNALLY-MANAGED

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

# Install locked versions from uv.lock into the system Python
# (--inexact: keep apt-installed packages that are not in the lock)
ENV UV_PROJECT_ENVIRONMENT=/usr
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --inexact --no-dev --extra kubernetes --no-install-project

COPY app /opt/flink/usrlib/app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --inexact --no-dev --extra kubernetes --no-editable
# Remove build-time dependencies after Python packages are installed.
# Keep the libpython3.12 RUNTIME package: thread mode (Pemja) dlopens
# libpython at TaskManager startup; --auto-remove of the -dev package alone
# would drop the runtime .so and surface as a misleading
# NoSuchFieldException(loadedLibraryNames) on JDK 17.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libpython3.12 \
    && apt-get purge -y --auto-remove \
        libpython3.12-dev \
        gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# PyFlink 2.2.0 embedded-mode (thread) bug: in the KEYED_CO_BROADCAST_PROCESS
# branch on_timer is the only path NOT wrapped in process_func(), so
# side-output tuples (OutputTag, value) yielded from on_timer leak into the
# main output and fail with ClassCastException (Object[] -> String).
# Fixed here at image build; RuntimeContext TaskInfo patch stays in
# app/operators/_pyflink_compat.py.
RUN sed -i \
    's/^            yield from on_timer(timestamp, timer_context)$/            yield from process_func(on_timer(timestamp, timer_context))/' \
    /usr/local/lib/python3.12/dist-packages/pyflink/fn_execution/datastream/embedded/operations.py \
    && grep -q "process_func(on_timer" \
       /usr/local/lib/python3.12/dist-packages/pyflink/fn_execution/datastream/embedded/operations.py

################################################################################
# Install Flink Kafka Connector
################################################################################
# Copy Kafka connector to Flink classpath (automatically loaded by Flink)
COPY lib/flink-sql-connector-kafka-4.0.1-2.0.jar /opt/flink/lib/

################################################################################
# Copy additional files
################################################################################
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
# polars[rtcompat] installs both builds, and Polars prefers compat when unset.
# Keep the AVX2 build unless a deployment overrides this (old CPUs: compat).
ENV POLARS_FORCE_PKG=32

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
