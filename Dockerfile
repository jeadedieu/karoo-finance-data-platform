# One image for every container in the stack: Airflow 3.4 + the pipeline's own Python
# environment. The pipeline's packages (pandas, dbt, ...) live in a SEPARATE virtual
# environment (/opt/karoo-venv) so they can never clash with Airflow's own packages.
FROM apache/airflow:3.4.0-python3.12

USER root
RUN mkdir -p /opt/karoo-venv && chown airflow: /opt/karoo-venv

USER airflow
COPY requirements.txt /tmp/karoo-requirements.txt
RUN python -m venv /opt/karoo-venv \
 && /opt/karoo-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/karoo-venv/bin/pip install --no-cache-dir -r /tmp/karoo-requirements.txt
