FROM apache/airflow:2.9.1-python3.11

COPY requirements.txt /tmp/requirements.txt
# The constraints file keeps our packages compatible with Airflow's own pinned versions
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.11.txt"
