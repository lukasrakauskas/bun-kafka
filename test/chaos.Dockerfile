FROM redpandadata/redpanda:v25.2.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 \
  && rm -rf /var/lib/apt/lists/*
