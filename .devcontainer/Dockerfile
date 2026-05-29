ARG VARIANT=1.94-bookworm
FROM rust:${VARIANT}

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    # Remove imagemagick due to https://security-tracker.debian.org/tracker/CVE-2019-10131
    && apt-get purge -y imagemagick imagemagick-6-common \
    && apt-get install -y sudo jq bat curl git cmake zsh locales libssl-dev libz-dev \
    && echo "en_US.UTF-8 UTF-8" > /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && rm -rf /var/lib/apt/lists/*

ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

RUN useradd -u 1000 -G sudo -U -m -s /usr/bin/zsh dev \
    && echo "dev ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers \
    && mkdir -p /home/dev && chown -R dev:dev /home/dev

ENV HOME=/home/dev
ENV PATH="/home/dev/.local/bin:${PATH}"

ENV CARGO_HOME=/home/dev/.cargo
RUN mkdir -p /home/dev/.cargo && chown -R dev:dev /home/dev/.cargo

COPY --from=ghcr.io/j178/prek:v0.3.2 /prek /usr/local/bin/prek

RUN sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /usr/local/bin
ADD .taskfile .taskfile
COPY Taskfile.yml ./Taskfile.yml
RUN task devtools:install

RUN cargo install rust-script

USER dev

RUN curl -fsSL https://claude.ai/install.sh | bash

RUN sh -c "$(wget -O- https://github.com/deluan/zsh-in-docker/releases/download/v1.2.0/zsh-in-docker.sh)" -- \
    -t lambda
