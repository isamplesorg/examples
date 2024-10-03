FROM quay.io/jupyter/minimal-notebook:2024-07-15
# FROM quay.io/jupyter/minimal-notebook:2024-02-06
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
# Install pipx
RUN python -m pip install --user pipx && \
    python -m pipx ensurepath

# Add pipx to PATH
ENV PATH="/root/.local/bin:$PATH"

# Use pipx to install Poetry
RUN pipx install poetry

# Copy pyproject.toml and poetry.lock if it exists
COPY pyproject.toml poetry.lock* ./

# Set the PATH again to ensure it's available in the current shell
RUN export PATH="/root/.local/bin:$PATH" && \
    poetry install --no-root

# Install dependencies from requirements.in
COPY requirements.in ./
RUN pip install -r requirements.in

VOLUME ["/home/jovan/work", "/data"]
