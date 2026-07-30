FROM public.ecr.aws/docker/library/python:3.10


ARG CODEARTIFACT_AUTH_TOKEN
ARG TWINE_USERNAME
ARG TWINE_PASSWORD
ARG TWINE_REPOSITORY_URL

WORKDIR /app

# Set environment variables
ENV TWINE_USERNAME=$TWINE_USERNAME
ENV TWINE_PASSWORD=$TWINE_PASSWORD
ENV TWINE_REPOSITORY_URL=$TWINE_REPOSITORY_URL

# Copy the project files to the working directory
COPY . .

# Install dependencies, build the package, install Twine, Publish the package using Twine, 
RUN pip3 install wheel requests && \
 pytest && python setup.py sdist && \
 pip3 install twine && \
 twine upload --repository-url $TWINE_REPOSITORY_URL --username $TWINE_USERNAME --password $TWINE_PASSWORD dist/* && \
  rm -rf dist

# Set the default command
CMD ["/bin/bash"]
