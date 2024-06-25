# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# Builds the MWAA or Airflow Operator and outputs it to the out folder. This can be used to upload to MWAA.

# End script if one subcommand fails
set -e

# Defining colours for output
GREEN='\033[1;32m'
OFF='\033[0m'

# Check if finch is installed, if not fall back to docker
printf "${GREEN}Checking if finch is installed...${OFF}\n"
if ! command -v finch &> /dev/null
then
    finch=docker
else
    finch=finch
fi

# Build Apache Xtable
printf "${GREEN}Building...${OFF}\n"
$finch build -f Dockerfile -o type=tar,dest=out.tar .

# Copy other helper files to the out directory
printf "${GREEN}Copying build artifacts and other helper files to the out directory...${OFF}\n"
mkdir -p ./out/ \
    && mv ./out.tar ./out/out.tar \
    && cp ./files/requirements.txt ./out/ \
    && cp ./files/DAG.py ./out/ \
    && cp ./files/startup.sh ./out/ \
    && cp ./files/.airflowignore ./out/

# Extract docker output
cd out/ \
    && tar xf out.tar \
    && rm ./out.tar
