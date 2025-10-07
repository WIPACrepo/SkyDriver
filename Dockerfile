FROM python:3.13

RUN useradd -m -U app

WORKDIR /home/app
USER app

# NOTE:
#  - During pip-install, no additional files are written to package
#  - No 'COPY .' because we don't want to copy extra files (especially '.git/')
#  - Mounting source files prevents unnecessary file duplication
#  - Mounting '.git/' allows the Python project to build with 'setuptools-scm'
RUN --mount=type=bind,source=.git,target=.git,ro \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml,ro \
    --mount=type=bind,source=ewms_init_container,target=ewms_init_container,ro \
    --mount=type=bind,source=s3_sidecar,target=s3_sidecar,ro \
    --mount=type=bind,source=skydriver,target=skydriver,ro \
    pip install --no-cache .

ENV PYTHONPATH=/home/app

USER app

CMD ["python", "-m", "skydriver"]
