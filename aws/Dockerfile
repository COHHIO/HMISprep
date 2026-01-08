FROM --platform=linux/amd64 rocker/tidyverse:latest

ARG GITHUB_PAT
ENV GITHUB_PAT=${GITHUB_PAT}

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    git \
    libcurl4-openssl-dev \
    libfontconfig1-dev \
    libfreetype6-dev \
    libfribidi-dev \
    libgit2-dev \
    libharfbuzz-dev \
    libicu-dev \
    libjpeg-dev \
    libpng-dev \
    libssh2-1-dev \
    libssl-dev \
    libtiff5-dev \
    libxml2-dev \
    make \
    pandoc \
    zlib1g-dev \
    && apt-get clean

# Install required R packages
RUN R -e "install.packages(c('pak', 'logger', 'aws.s3', 'dplyr', 'stringr'))"

# Install duckplyr with pak (note: using tidyverse/duckplyr as you suggested)
RUN R -e "pak::pak('tidyverse/duckplyr')"

# Verify duckplyr is installed
RUN R -e "if('duckplyr' %in% rownames(installed.packages())) { print('duckplyr is installed') } else { print('duckplyr is NOT installed'); quit(status=1) }"

# Installing from GitHub:
RUN R -e "options(pak.no_prompt = TRUE);pak::pak('yogat3ch/UU')"
RUN R -e "options(pak.no_prompt = TRUE);pak::pak('COHHIO/HMIS')"
RUN R -e "options(pak.no_prompt = TRUE);pak::pak('COHHIO/HMISdata')"
RUN R -e "options(pak.no_prompt = TRUE);pak::pak('COHHIO/HMISprep')"

# Verify HMISprep is installed
RUN R -e "if('HMISprep' %in% rownames(installed.packages())) { print('HMISprep is installed') } else { print('HMISprep is NOT installed'); quit(status=1) }"

# Copy the processing script
COPY process_hmis_data.R /app/
WORKDIR /app

# Make script executable
RUN chmod +x process_hmis_data.R

# Run the script when container starts
ENTRYPOINT ["/app/process_hmis_data.R"]

