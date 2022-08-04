ARG IMAGE_NAME
FROM ${IMAGE_NAME}

USER root
RUN apt-get update -y && apt-get install -y git
RUN apt-get install -y --no-install-recommends \
        build-essential \
        libsasl2-2 \
        libsasl2-dev \
        libsasl2-modules \
        gnupg \
        unzip

# install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install;

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
&& curl https://packages.microsoft.com/config/debian/11/prod.list >> /etc/apt/sources.list.d/mssql-release.list \
&& apt update && ACCEPT_EULA=Y apt install -y msodbcsql18

COPY setup.cfg ${AIRFLOW_HOME}/astronomer_providers/setup.cfg
COPY pyproject.toml ${AIRFLOW_HOME}/astronomer_providers/pyproject.toml


RUN pip install -e "${AIRFLOW_HOME}/astronomer_providers[all,tests,mypy]"
USER astro
