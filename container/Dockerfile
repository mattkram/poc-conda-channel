FROM condaforge/miniforge3:latest

RUN mamba install -y -n base \
      conda-index \
      conda-package-streaming \
      python=3.12 \
      boto3 \
    && mamba clean -afy

WORKDIR /app
COPY entrypoint.py /app/entrypoint.py

# Listens on the port declared as `defaultPort` in the Worker's Container class.
EXPOSE 8080
CMD ["python", "/app/entrypoint.py"]
