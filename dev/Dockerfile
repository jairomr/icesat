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
    libpq-dev \
    libpq5 \
    git && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    chmod +x /APP/entrypoint.sh

RUN cd /APP && conda env create -f environment.yml && conda run pip install -r requirements.txt
# Install PhoREAL
RUN  cd /tmp && \
    git clone https://github.com/icesat-2UT/PhoREAL.git &&\
    cd PhoREAL && \
    python setup.py build && \
    python setup.py install



# Override default shell and use bash
SHELL ["conda", "run", "-n", "env", "/bin/bash", "-c"]

CMD ["python", "Icesat2/loop.py"]

 

