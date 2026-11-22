ARG TUGRAPH_IMAGE=tugraph/tugraph-runtime-arm64v8-centos7:4.5.1
FROM ${TUGRAPH_IMAGE} AS tugraph
FROM falkordb/falkordb:6.0.4 AS falkordb
FROM neo4j:2026.10.2 AS neo4j
FROM memgraph/memgraph:3.14.0

USER root
ARG VELR_ARCH
ARG VELR_SHA256
ENV JAVA_HOME=/opt/java/openjdk \
    SHOOTOUT_NEO4J_HOME=/opt/neo4j \
    SHOOTOUT_MEMGRAPH_BIN=/usr/lib/memgraph/memgraph \
    SHOOTOUT_REDIS_BIN=/opt/shootout/bin/redis-server \
    SHOOTOUT_FALKORDB_MODULE=/opt/shootout/lib/falkordb.so \
    SHOOTOUT_TUGRAPH_BIN=/opt/shootout/bin/lgraph_server \
    SHOOTOUT_TUGRAPH_IMPORT_BIN=/opt/shootout/bin/lgraph_import \
    VELR_ADMIN_BIN=/opt/shootout/bin/velr-admin \
    PYTHONDONTWRITEBYTECODE=1
ENV PATH=/opt/shootout/venv/bin:/opt/java/openjdk/bin:${PATH}

COPY --from=neo4j /opt/java/openjdk/ /opt/java/openjdk/
COPY --from=falkordb /usr/local/bin/redis-server /opt/shootout/bin/redis-server
COPY --from=falkordb /var/lib/falkordb/bin/falkordb.so /opt/shootout/lib/falkordb.so
COPY --from=tugraph /usr/local/bin/lgraph_server /opt/tugraph/bin/lgraph_server
COPY --from=tugraph /usr/local/bin/lgraph_import /opt/tugraph/bin/lgraph_import
COPY --from=tugraph /usr/local/lib64/lgraph/ /opt/tugraph/lib/
COPY requirements*.txt /opt/shootout/

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates curl libgfortran5 libgomp1 python3-venv \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m venv /opt/shootout/venv \
    && python -m pip install --no-cache-dir --only-binary=:all: \
        -r /opt/shootout/requirements.txt -r /opt/shootout/requirements-data.txt \
        -r /opt/shootout/requirements-candidates.txt \
    && python -m pip install --no-cache-dir -r /opt/shootout/requirements-server.txt falkordb-bulk-loader==1.2.0

RUN set -eu; \
    velr_archive="velr-v0.2.61-linux-${VELR_ARCH}.tar.gz"; \
    curl -fLsS --retry 3 "https://github.com/velr-ai/velr/releases/download/v0.2.61/$velr_archive" -o "/tmp/$velr_archive"; \
    printf '%s  %s\n' "$VELR_SHA256" "/tmp/$velr_archive" | sha256sum -c -; \
    tar -xzf "/tmp/$velr_archive" -C /opt/shootout/bin --strip-components=1 "${velr_archive%.tar.gz}/velr-admin"; \
    rm "/tmp/$velr_archive"; \
    mkdir -p "$SHOOTOUT_NEO4J_HOME"; \
    curl -fLsS --retry 3 https://dist.neo4j.org/neo4j-community-2026.10.2-unix.tar.gz -o /tmp/neo4j.tar.gz; \
    printf '%s  %s\n' fcbccee832175fd8b873ca7d8228dbde4cb5cce43f9053833cc7517dc4dd6f31 /tmp/neo4j.tar.gz | sha256sum -c -; \
    tar -xzf /tmp/neo4j.tar.gz -C "$SHOOTOUT_NEO4J_HOME" --strip-components=1; \
    rm /tmp/neo4j.tar.gz; \
    for tool in lgraph_server lgraph_import; do \
        printf '#!/bin/sh\nexport LD_LIBRARY_PATH=/opt/tugraph/lib\nexec /opt/tugraph/bin/%s "$@"\n' "$tool" > "/opt/shootout/bin/$tool"; \
        chmod 755 "/opt/shootout/bin/$tool"; \
    done

ENTRYPOINT ["python", "-m", "shootout.container", "--inside"]
