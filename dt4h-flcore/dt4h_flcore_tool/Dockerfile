FROM ubuntu:22.04
RUN apt-get update && apt-get install -y python3 python3-pip
RUN pip install requests pyyaml
RUN mkdir /home/workdir
WORKDIR /home
COPY dt4h_flcore.py /home
COPY flcore_params.py /home
COPY fem_api_client.py /home
ENTRYPOINT ["/usr/bin/python3", "/home/dt4h_flcore.py"]
