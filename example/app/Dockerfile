FROM python

WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY boot.sh ./boot.sh
RUN chmod +x ./boot.sh

# Lazy way to add the test `nodes` and the `flowpipe_celery_adapter` to the path
ENV PYTHONPATH "${PYTHONPATH}:/app"

CMD ["sh", "./boot.sh"]


