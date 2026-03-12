FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir tqdm

COPY convert_to_m4b.py /app/convert_to_m4b.py

WORKDIR /data
ENTRYPOINT ["python3", "/app/convert_to_m4b.py"]
CMD ["/data"]
