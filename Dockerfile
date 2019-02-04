FROM python:3.6
LABEL maintainer="dan.leehr@duke.edu"

# cwltool requires nodejs
RUN apt-get update && apt-get install -y nodejs

RUN mkdir -p /app
COPY . /app
RUN pip install /app

CMD ["calrissian"]
