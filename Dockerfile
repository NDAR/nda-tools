FROM public.ecr.aws/docker/library/python:3.9

ARG CODEARTIFACT_AUTH_TOKEN
ARG TWINE_USERNAME
ARG TWINE_PASSWORD
ARG TWINE_REPOSITORY_URL

RUN echo $CODEARTIFACT_AUTH_TOKEN

WORKDIR /app

# Set environment variables
ENV TWINE_USERNAME=$TWINE_USERNAME
ENV TWINE_PASSWORD=$TWINE_PASSWORD
ENV TWINE_REPOSITORY_URL=$TWINE_REPOSITORY_URL

# Copy the project files to the working directory
COPY . .

# Install dependencies and build the package
RUN pip install wheel requests
RUN python setup.py sdist

# Install Twine
RUN pip3 install twine

# Publish the package using Twine
RUN twine upload --repository-url $TWINE_REPOSITORY_URL --username $TWINE_USERNAME --password $TWINE_PASSWORD dist/*

# Cleanup
RUN rm -rf dist

# Set the default command
CMD ["/bin/bash"]
