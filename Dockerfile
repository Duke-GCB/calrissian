FROM python:3.10.0-slim-buster
LABEL maintainer="dan.leehr@duke.edu"

# cwltool requires nodejs
RUN apt-get update && apt-get install -y nodejs

RUN mkdir -p /app

# Create a default user and home directory
ENV HOME=/home/calrissian
# home dir is created by useradd with group (g=0) to comply with
# https://docs.openshift.com/container-platform/3.11/creating_images/guidelines.html#openshift-specific-guidelines
RUN useradd -u 1001 -r -g 0 -m -d ${HOME} -s /sbin/nologin \
      -c "Default Calrissian User" calrissian && \
  chown -R 1001:0 /app && \
  chmod g+rwx ${HOME}

USER calrissian
RUN pip install hatch 
ENV PATH="${HOME}/.local/bin:${PATH}"

COPY . /app
WORKDIR /app

ENV VIRTUAL_ENV=/app/envs/calrissian
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN hatch env prune && \
    hatch env create prod

CMD ["calrissian"]
