# Define base image
FROM continuumio/miniconda3


# Override default shell and use bash
WORKDIR /APP
# Create Conda environment from the YAML file
COPY environment.yml /APP
COPY requirements.txt /APP
COPY ./ssh /root/.ssh 
COPY ./entrypoint.sh /APP

RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    build-essential \
    libreadline-dev \
    liblua5.3-dev \
    libpq5 \
    git \
    libsqlite3-dev \ 
    libtiff5-dev \  
    pkg-config \
    cmake \
    postgresql-client \
    libpq-dev \
    libgeos-dev \
    htop \
    postgis && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    chmod +x /APP/entrypoint.sh

RUN cd /APP && conda env create -f environment.yml && \
    apt-get update && \
    apt-get install -y libpq-dev libpq5 libffi-dev screen && \
    apt-get update && apt-get upgrade -y && \
    apt-get install -y --reinstall libp11-kit0 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*  && \
    conda run pip install -r requirements.txt 

RUN    cd /tmp && \
    git clone https://github.com/icesat-2UT/PhoREAL.git &&\
    cd PhoREAL && \
    python setup.py build && \
    python setup.py install 

# Override default shell and use bash
SHELL ["conda", "run", "-n", "env", "/bin/bash", "-c"]


ENV LD_LIBRARY_PATH=/lib:/usr/lib:/usr/local/lib:/lib/x86_64-linux-gnu/:/usr/lib/x86_64-linux-gnu \
    SQLALCHEMY_WARN_20=1 \
    SQLALCHEMY_SILENCE_UBER_WARNING=1



CMD ["python", "Icesat2/loop.py"]

 

