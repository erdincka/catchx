FROM --platform=linux/amd64 erdincka/maprclient

RUN pip install nicegui protobuf==3.20.* requests importlib_resources
RUN pip install faker pyiceberg[hive,pandas,s3fs]

RUN pip install geopy country_converter pycountry country_converter minio deltalake
RUN apt install sshpass

EXPOSE 3000

COPY . /workspace
WORKDIR /workspace

CMD [ "sleep infinity" ]
