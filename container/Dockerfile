FROM debian:bookworm-slim

# Install pixi
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && curl -fsSL https://pixi.sh/install.sh | bash \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.pixi/bin:$PATH"

WORKDIR /app
COPY pixi.toml /app/pixi.toml

# Install the environment (creates .pixi/envs/default)
RUN pixi install

COPY entrypoint.py /app/entrypoint.py

# Listens on the port declared as `defaultPort` in the Worker's Container class.
EXPOSE 8080
CMD ["pixi", "run", "python", "/app/entrypoint.py"]
