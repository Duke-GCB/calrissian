FROM rockylinux/rockylinux:10.2-minimal AS builder

RUN microdnf -y update && \
    microdnf -y install ca-certificates python3 python3-pip python3-devel gcc gcc-c++ make git && \
    microdnf clean all

# Use the concrete interpreter path so venv can resolve sys._base_executable
# when Kaniko runs the layer command.
RUN /usr/bin/python3.12 -m venv /opt/build-venv
ENV PATH="/opt/build-venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
# Hatch creates additional virtualenvs for isolated builds.
ENV VIRTUALENV_PIP=26.2.2

COPY scripts/bootstrap-pip.sh /tmp/bootstrap-pip.sh
RUN /bin/sh /tmp/bootstrap-pip.sh

ARG HATCH_VERSION=1.18.0
RUN python -m pip install --no-cache-dir "certifi>=2026.10.19" "uv>=0.13.4" "hatch==${HATCH_VERSION}"

WORKDIR /src
COPY . /src

# Build the application and collect dependency wheels for the runtime image.
RUN hatch build -t wheel && \
    python -m pip wheel --no-cache-dir --wheel-dir /wheels "pip==26.2.2" /src/dist/*.whl

FROM rockylinux/rockylinux:10.2-minimal AS runtime

# Use the updated OS trust store instead of Node's bundled CA snapshot.
ENV NODE_OPTIONS="--use-openssl-ca"

# cwltool uses Node.js to evaluate CWL JavaScript expressions.
RUN microdnf -y update && \
    microdnf -y install ca-certificates python3 python3-pip nodejs git libstdc++ sqlite-libs zlib shadow-utils && \
    microdnf clean all

ENV HOME=/home/calrissian
RUN /usr/sbin/useradd -u 1001 -r -g 0 -m -d ${HOME} -s /sbin/nologin \
        -c "Default Calrissian User" calrissian && \
    mkdir -p /app /prod && \
    chown -R 1001:0 /app && \
    chmod g+rwx ${HOME} /app

USER calrissian
WORKDIR /app

ENV VIRTUAL_ENV=/app/venv
ENV PATH="${VIRTUAL_ENV}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
RUN /usr/bin/python3.12 -m venv "${VIRTUAL_ENV}"

COPY --from=builder --chown=1001:0 /wheels/ /app/dist/
RUN python -m pip install --no-cache-dir --no-index --find-links=/app/dist "pip==26.2.2" && \
    python -m pip install --no-cache-dir --no-index --find-links=/app/dist calrissian && \
    python -m pip check && \
    calrissian --help && \
    node --version && \
    python -m pip uninstall --yes pip && \
    rm -rf /app/dist

CMD ["calrissian"]
