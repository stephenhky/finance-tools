FROM python:3.8

ADD . /code

WORKDIR /code

RUN pip install -U pip
RUN pip install -r requirements.txt
RUN pip install awslambdaric boto3

ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]
CMD [ "symbolcorrelation.symbolcorr_handler" ]