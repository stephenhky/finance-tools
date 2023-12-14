FROM python:3.11

ADD . /code

WORKDIR /code

RUN apt-get update && \
  apt-get install -y \
  g++ \
  libc6 \
  gfortran \
  make \
  cmake \
  unzip \
  libcurl4-openssl-dev

RUN pip install -U pip
RUN pip install -r requirements.txt
RUN pip install awslambdaric boto3

ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]
CMD [ "fitlppl.lambda_handler" ]
