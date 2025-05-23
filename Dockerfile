FROM python:3.13

RUN useradd -m -U app

WORKDIR /home/app
USER app

COPY --chown=app:app . .

RUN pip install --no-cache-dir .
ENV PYTHONPATH=/home/app

USER app

CMD ["python", "-m", "skydriver"]
