FROM apify/actor-python:3.12@sha256:7d4c77aacd58c740b432d24a9e1b3a0ef06c9300a37328aa775f17a5524cedfb

COPY requirements.lock ./

RUN python3 --version \
    && pip install --no-cache-dir -r requirements.lock \
    && pip check \
    && pip freeze

COPY common/ ./common/
COPY src/ ./src/

ENV PYTHONPATH=/usr/src/app:/usr/src/app/src
ENV PYTHONUNBUFFERED=1

RUN python3 -c "import common, norway_brreg_company_change_monitor; print('import check ok')"

CMD ["python3", "-m", "norway_brreg_company_change_monitor"]
