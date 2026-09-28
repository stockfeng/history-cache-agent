# Multi-stage build for the history-cache-agent sidecar.
# Uses the same rockylinux:9 base as cloud_gateway_v2 for VPS consistency.

FROM rockylinux:9 AS builder

RUN dnf install -y epel-release && \
    dnf config-manager --set-enabled crb && \
    dnf install -y --setopt=install_weak_deps=False \
    gcc-c++ \
    cmake \
    make \
    pkgconf-pkg-config \
    python3 \
    openssl-devel \
    libcurl-devel \
    libzstd-devel \
    json-devel \
    && dnf clean all

WORKDIR /src
COPY include include
COPY src src
COPY tools tools
COPY tests tests
COPY CMakeLists.txt .

RUN cmake -B build -DCMAKE_BUILD_TYPE=Release -DHISTORY_CACHE_CURL=ON \
    && cmake --build build --target history-cache-agent -j"$(nproc)"

FROM rockylinux:9

RUN dnf install -y --setopt=install_weak_deps=False \
    libstdc++ \
    openssl-libs \
    libcurl \
    libzstd \
    && dnf clean all \
    && mkdir -p /run/history-cache

COPY --from=builder /src/build/history-cache-agent /opt/history-cache/bin/history-cache-agent

ENTRYPOINT ["/opt/history-cache/bin/history-cache-agent"]
CMD ["--socket", "/run/history-cache/agent.sock"]
