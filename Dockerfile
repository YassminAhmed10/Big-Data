FROM python:3.11-slim

RUN pip install --no-cache-dir \
    pandas \
    numpy \
    matplotlib \
    seaborn \
    scikit-learn \
    scipy \
    requests

RUN mkdir -p /app/pipeline

COPY ingest.py        /app/pipeline/
COPY preprocess.py    /app/pipeline/
COPY analytics.py     /app/pipeline/
COPY visualize.py     /app/pipeline/
COPY cluster.py       /app/pipeline/
COPY summary.sh       /app/pipeline/

COPY CLD_XHAS_SEX_AGE_GEO_NB_A-filtered-2026-03-19.csv /app/pipeline/
COPY CLD_XHAN_SEX_AGE_GEO_NB_A-filtered-2026-03-19.csv /app/pipeline/
COPY 4909ea2f-5255-49a2-811e-5583974af6ab_Data.csv      /app/pipeline/

WORKDIR /app/pipeline

CMD ["/bin/bash"]
