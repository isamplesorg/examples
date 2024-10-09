FROM quay.io/jupyter/minimal-notebook:2024-10-03

# Set environment variables to avoid prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Make sure the contents of our repo are in ${HOME}
COPY . ${HOME}
USER root
RUN chown -R ${NB_UID} ${HOME}

# Update package list and install required dependencies
RUN apt-get update && \
    apt-get install -y software-properties-common libdb-dev libzmq3-dev curl libssl-dev zlib1g-dev jq jupyter-console && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install pipx and add its binary directory to PATH
RUN pip install pipx && \
    pipx ensurepath
ENV PATH="/home/jovyan/.local/bin:$PATH"

# Use pipx to install Poetry
RUN pipx install poetry

# Copy pyproject.toml and poetry.lock if it exists
COPY pyproject.toml poetry.lock* ./

# Install project dependencies using Poetry
RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi --with examples

# Install dependencies from requirements.in if it exists
# COPY requirements.in ./
# RUN if [ -f requirements.in ]; then pip install --upgrade -r requirements.in; fi

# Create necessary directories and set permissions
RUN mkdir -p /home/jovyan/.local/share/jupyter && \
    chown -R jovyan:users /home/jovyan/.local

VOLUME ["/home/jovyan/work", "/data"]

# Switch back to jovyan to avoid accidental container runs as root
USER ${NB_UID}

# Verify permissions
RUN ls -la /home/jovyan/.local/share
