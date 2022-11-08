# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
ARG base=amd64/ubuntu:22.04
# Set a default timezone, can be overriden via ARG
ARG tz="Europe/Madrid"

FROM ${base} AS velox

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt update && \
      apt install -y sudo git

RUN git clone https://github.com/facebookincubator/velox.git

# TZ and DEBIAN_FRONTEND="noninteractive"
# are required to avoid tzdata installation
# to prompt for region selection.
ENV DEBIAN_FRONTEND="noninteractive" TZ=${tz}
RUN /velox/scripts/setup-ubuntu.sh

WORKDIR /velox
RUN git submodule sync --recursive
RUN git submodule update --init --recursive

ENV EXTRA_CMAKE_FLAGS="-DVELOX_ENABLE_SUBSTRAIT=ON -DVELOX_ENABLE_ARROW=ON -DVELOX_ENABLE_PARQUET=ON"
RUN make

WORKDIR /velox/_build/release

FROM velox

ENV VELOX_ROOT=/velox

WORKDIR /

ADD Makefile /Makefile
ADD *.cpp /
ADD *.h /

ENV EXTRA_LIBS="-lunwind"
ENV PYTHON_VERSION="3.10"
RUN make
