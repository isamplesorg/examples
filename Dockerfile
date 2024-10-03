FROM quay.io/jupyter/minimal-notebook:2024-07-15

# Set environment variables to avoid prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Make sure the contents of our repo are in ${HOME}
COPY . ${HOME}
USER root
RUN chown -R ${NB_UID} ${HOME}

# Update package list and install required dependencies
RUN apt-get update && \
    apt-get install -y software-properties-common

# Install system dependencies
RUN apt-get update && \
    apt-get install -y libdb-dev libzmq3-dev curl libssl-dev zlib1g-dev jq jupyter-console && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install pipx
RUN pip install pipx
RUN pipx ensurepath

# Add pipx binary directory to PATH
ENV PATH="/home/jovyan/.local/bin:$PATH"

# Verify pipx installation
RUN echo "Pipx path: $(which pipx)"

# Use pipx to install Poetry
RUN pipx install poetry

# Verify Poetry installation
RUN echo "Poetry path: $(which poetry)"

# Copy pyproject.toml and poetry.lock if it exists
COPY pyproject.toml poetry.lock* ./

# Install project dependencies using Poetry
RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi

# Install dependencies from requirements.in if it exists
COPY requirements.in ./
RUN if [ -f requirements.in ]; then pip install -r requirements.in; fi

VOLUME ["/home/jovyan/work", "/data"]

# Switch back to jovyan to avoid accidental container runs as root
USER ${NB_UID}