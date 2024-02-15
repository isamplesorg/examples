FROM quay.io/jupyter/minimal-notebook:2024-02-06
# FROM jupyter/minimal-notebook:2023-10-20
# FROM jupyter/minimal-notebook:2023-06-13
# FROM jupyter/scipy-notebook:2023-06-06
# 2023-04-24
# 2023-02-28

# https://www.phind.com/search?cache=225c8894-dc96-4e39-8f12-494486109003

# Set environment variables to avoid prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Make sure the contents of our repo are in ${HOME}
COPY . ${HOME}
USER root
RUN chown -R ${NB_UID} ${HOME}
# USER ${NB_USER}

# Update package list and install required dependencies
RUN apt-get update && \
    apt-get install -y software-properties-common

# Install system dependencies
# add-apt-repository -y ppa:bitcoin/bitcoin
RUN apt-get update && \
    apt-get install -y libdb-dev && \
    apt-get install -y libzmq3-dev curl libssl-dev && \
    apt-get install -y zlib1g-dev && \
    apt-get install -y jq && \
    apt-get install -y jupyter-console && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install the required Python packages
# RUN pip install git+https://github.com/rdhyee/noid-1.git@master#egg=noid \
#     click==8.0.3 \
#     colorama==0.4.4 \
#     pytest \
#     git+https://github.com/rdhyee/ezid-client-tools.git@installable#egg=ezid_client_tools \
#     git+https://github.com/rdhyee/noidy.git@pip-package#egg=noidy

# Install the required Python packages
RUN pip install -r requirements.in



VOLUME ["/home/jovan/work", "/data"]