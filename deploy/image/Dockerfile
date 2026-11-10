# syntax=docker/dockerfile:1
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
ARG CHANNEL
ARG INPUTS
COPY dist/payload/ /usr/local/lib/bugnet/payload/
RUN /usr/local/lib/bugnet/payload/setup-image.sh build --target oci \
        --channel "${CHANNEL}" --inputs "${INPUTS}" \
    && rm -rf /usr/local/lib/bugnet/payload
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
USER root
WORKDIR /root
