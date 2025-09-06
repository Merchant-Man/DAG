FROM apache/airflow:2.10.5

# Adding installation for `apt` pacakges. Please uncomment the following three lines:
USER root
# Installing gcc required by crypto packages
RUN sudo apt-get update \
    && apt-get install -y gcc make autoconf \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# Adding dependencies here to extend the default.
COPY requirements.txt /
# Stating the apache-airflow version to prevent upgrade/downgrade issues by pip.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt