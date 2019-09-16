FROM python:3.7
LABEL maintainer="dan.leehr@duke.edu"

# cwltool requires nodejs
RUN apt-get update && apt-get install -y nodejs

RUN mkdir -p /app
COPY . /app
RUN pip install /app
WORKDIR /app

# Create a default user and home directory
ENV HOME=/home/calrissian
# home dir is created by useradd with group (g=0) to comply with
# https://docs.openshift.com/container-platform/3.11/creating_images/guidelines.html#openshift-specific-guidelines
RUN useradd -u 1001 -r -g 0 -m -d ${HOME} -s /sbin/nologin \
      -c "Default Calrissian User" calrissian && \
  chown -R 1001:0 /app && \
  chmod g+rwx ${HOME}

USER calrissian
CMD ["calrissian"]
