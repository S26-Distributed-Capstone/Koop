# Use the plain Debian Bookworm base
FROM mcr.microsoft.com/devcontainers/base:bookworm

# Clean up the unverified Yarn repo to prevent 'apt-get update' failures
RUN rm -f /etc/apt/sources.list.d/yarn.list \
    && rm -f /usr/share/keyrings/yarnkey.gpg