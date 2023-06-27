FROM python:3.11

RUN useradd -m -U app

WORKDIR /home/app
USER app

COPY --chown=app:app . .

RUN pip install --no-cache-dir .
ENV PYTHONPATH=/home/app

# clientmanager needs GCP for GKE
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-cli -y
RUN sudo apt-get install google-cloud-sdk-gke-gcloud-auth-plugin

CMD ["python", "-m", "skydriver"]
