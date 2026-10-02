# syntax=docker/dockerfile:1
# Pondra in a container (ADR-041): the binary the build workflow made and tested (Linux, glibc 2.17),
# never compiled again here, on a minimal base: glibc, libgcc, CA certificates, no shell, a user
# that isn't root. The context holds a folder per architecture, `amd64/` and `arm64/`, each with
# `pondra` and the wheel (`tools/image.py` makes it from a build, a release or a binary of yours):
#
#   docker buildx build -f deploy/docker/Dockerfile --platform linux/amd64,linux/arm64 ctx
#   docker buildx build -f deploy/docker/Dockerfile --target python ctx     # Python functions too
#
# Everything a node keeps is under /data, one volume: the lake (/data/lake by default), the SSD
# tier and replica files of a lake in a bucket (/data/cache), the key that seals secrets
# (/data/.pondra) and the console's settings (/data/.config).

# (/data, owned by the image's user: a new volume mounted there takes its owner from it)
FROM --platform=$BUILDPLATFORM busybox:1.37 AS home
RUN mkdir -p /data/lake /data/.pondra && chown -R 65532:65532 /data && chmod 700 /data/.pondra

FROM gcr.io/distroless/cc-debian13:nonroot AS pondra
ARG TARGETARCH
COPY --from=home --chown=65532:65532 /data /data
COPY --chmod=0755 ${TARGETARCH}/pondra /usr/local/bin/pondra
ENV HOME=/data PONDRA_CACHE_DIR=/data/cache
WORKDIR /data
VOLUME /data
# HTTP and the console; Postgres, Flight and Kafka when their flags are given
EXPOSE 8080 5432 8815 9092
ENTRYPOINT ["pondra"]
CMD ["serve", "lake", "--addr", "0.0.0.0:8080"]

# The same, with Python for `LANGUAGE python` functions and procedures (`--python auto`, which
# needs a token: --admin-token or PONDRA_ADMIN_TOKEN). tini reaps what Python's code starts.
FROM python:3.13-slim-trixie AS python
ARG TARGETARCH
RUN --mount=type=bind,source=${TARGETARCH},target=/ctx \
    pip install --no-cache-dir --no-compile /ctx/*.whl pyarrow \
    && apt-get update && apt-get install -y --no-install-recommends tini && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 65532 --home-dir /data --no-create-home --shell /usr/sbin/nologin nonroot
COPY --from=home --chown=65532:65532 /data /data
ENV HOME=/data PONDRA_CACHE_DIR=/data/cache
USER 65532:65532
WORKDIR /data
VOLUME /data
EXPOSE 8080 5432 8815 9092
ENTRYPOINT ["tini", "--", "pondra"]
CMD ["serve", "lake", "--addr", "0.0.0.0:8080", "--python", "auto"]

# (the default target: the image without Python)
FROM pondra
