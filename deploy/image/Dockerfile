# syntax=docker/dockerfile:1
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
ARG CHANNEL
ARG INPUTS
# The payload is bound for this step only, so no layer of the published image keeps a copy
RUN --mount=type=bind,source=dist/payload,target=/tmp/bugnet-payload \
    /tmp/bugnet-payload/setup-image.sh build --target oci \
        --channel "${CHANNEL}" --inputs "${INPUTS}"
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
USER root
WORKDIR /root
