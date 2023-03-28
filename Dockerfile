# Define base image
FROM continuumio/miniconda3
 

WORKDIR /APP
# Create Conda environment from the YAML file

RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    build-essential \
    libreadline-dev \
    liblua5.3-dev \
    git && \
    apt-get clean && rm -rf /var/lib/apt/lists/* 

RUN cd /APP && conda env create -f environment.yml && conda run pip install sliderule
# Install PhoREAL
RUN  cd /tmp && \
    git clone https://github.com/icesat-2UT/PhoREAL.git &&\
    cd PhoREAL && \
    python setup.py build && \
    python setup.py install

 
# Override default shell and use bash
SHELL ["conda", "run", "-n", "env", "/bin/bash", "-c"]
 