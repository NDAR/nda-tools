# Use a Python base image
FROM public.ecr.aws/amazonlinux/amazonlinux:latest
ARG TWINE_PROD_USERNAME
ARG TWINE_PROD_PASSWORD
ARG PROD="false"
ENV TWINE_PROD_USERNAME="$TWINE_PROD_USERNAME"
ENV TWINE_PROD_PASSWORD="$TWINE_PROD_PASSWORD"
# Set the working directory in the container
WORKDIR /app

# Copy the requirements.txt file to the container
# COPY requirements.txt .

# Install project dependencies
# RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project to the container
COPY . .
# Build and push the Python project to CodeArtifact
RUN yum update -y && yum install python3 -y
RUN python3 -m ensurepip --upgrade && python3 -m pip install requests twine awscli && python3 setup.py sdist
RUN if [ "$PROD" = "false" ] ; then \
      pass=`aws codeartifact get-authorization-token --domain nda --domain-owner 846214067917 --region us-east-1 --query authorizationToken --output text`; \
      devurl=`aws codeartifact get-repository-endpoint --domain nda --domain-owner 846214067917 --repository pypi-store --region us-east-1 --format pypi --query repositoryEndpoint --output text` ;\
      echo "uploading to $devurl with pass $pass"; \
      twine upload --repository-url "$devurl" --username aws --password "$pass" dist/* ; \
    else \
      echo "deploying to prod pypi..." ;  \
      twine upload --username "$TWINE_PROD_USERNAME" --password "$TWINE_PROD_PASSWORD" dist/* ;  \
    fi