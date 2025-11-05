ARG VARIANT=1.85.0-bookworm
FROM rust:${VARIANT}

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    # Remove imagemagick due to https://security-tracker.debian.org/tracker/CVE-2019-10131
    && apt-get purge -y imagemagick imagemagick-6-common \
    && apt-get install -y jq bat curl git zsh \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user and give necessary permissions
# We use the UID 1000 since it is the default UID for the first user on most systems
RUN useradd -u 1000 -G sudo -U -m -s /usr/bin/zsh dev \
    && echo "dev ALL=(ALL) NOPASSWD: /bin/chown" >> /etc/sudoers

# Install Taskfile
RUN sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /usr/local/bin
COPY Taskfile.yml /home/dev/Taskfile.yml
# Install dev tools
RUN task -d /home/dev install-devtools

# Set the environment variable in the shell's profile to support login shells
# having the virtual environment activated by default
RUN echo 'export PATH=/opt/venv/bin:$PATH' >> /home/dev/.profile

# Switch to the non-root user
USER dev

RUN sh -c "$(wget -O- https://github.com/deluan/zsh-in-docker/releases/download/v1.2.0/zsh-in-docker.sh)"
