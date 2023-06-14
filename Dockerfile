# Use a Python base image
FROM public.ecr.aws/docker/library/python:3.9
ARG CODEARTIFACT_AUTH_TOKEN
ENV CODEARTIFACT_AUTH_TOKEN="$CODEARTIFACT_AUTH_TOKEN"
RUN echo CODEARTIFACT_AUTH_TOKEN
RUN echo $CODEARTIFACT_AUTH_TOKEN
# Set the working directory in the container
WORKDIR /app

# Copy the requirements.txt file to the container
#COPY requirements.txt .

# Set the AWS CodeArtifact configuration
RUN echo "[global]" >> /etc/pip.conf \
    && echo "index-url = https://aws:$CODEARTIFACT_AUTH_TOKEN@nda-846214067917.d.codeartifact.us-east-1.amazonaws.com/pypi/pypi-store/simple/" >> /etc/pip.conf

# Install project dependencies
#RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project to the container
COPY . .

# Build and push the Python project to CodeArtifact
RUN python setup.py sdist \
    && twine upload --repository codeartifact dist/*

# Remove build artifacts
RUN rm -rf dist build

# Clean up unnecessary files and folders
RUN apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*


