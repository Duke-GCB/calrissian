FROM python:3.6
LABEL maintainer="dan.leehr@duke.edu"

RUN mkdir -p /app
COPY . /app
RUN pip install /app

CMD ["calrissian"]
