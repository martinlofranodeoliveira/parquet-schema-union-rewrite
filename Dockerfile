FROM python:3.13-slim-bookworm@sha256:281d39ff02e3b389401586134ff9e6572954687822a58055ec07a60ded4e0b7f

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=0 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1

WORKDIR /app
COPY requirements.lock /app/requirements.lock
# Explicit dependency preparation. Runtime commands never install anything.
RUN python -m pip install --no-cache-dir --only-binary=:all: --require-hashes -r requirements.lock
COPY . /app
# Build from the real sdist, install its wheel, and also exercise a source install.
RUN mkdir /opt/sdist /opt/wheels /opt/source-install \
    && python -c 'from setuptools.build_meta import build_sdist; build_sdist("/opt/sdist")' \
    && python -m pip wheel --no-index --no-deps --no-build-isolation --wheel-dir /opt/wheels /opt/sdist/*.tar.gz \
    && python -m pip install --no-index --no-deps /opt/wheels/*.whl \
    && python -m pip install --no-index --no-deps --no-build-isolation --target /opt/source-install /opt/sdist/*.tar.gz \
    && useradd --uid 1000 --create-home runner
USER 1000:1000
CMD ["python", "-m", "pytest", "-q"]
