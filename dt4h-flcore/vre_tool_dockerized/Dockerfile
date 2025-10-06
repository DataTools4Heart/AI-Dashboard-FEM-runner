# INSTALL TOOL
FROM ubuntu:22.04

RUN mkdir /home/workdir
#COPY dt4h_flcore.py /home
#COPY flcore_params.py /home
#COPY fem_api_client.py /home
#COPY generate_flcore_report.py /home
WORKDIR /home/workdir

#---------- DO NOT EDIT BELOW HERE ------------------------------------------------------------# 
#INSTALL openVRE DEPENDENCIES
#Entrypoint requirements
RUN apt update -y && apt install -y  bindfs sudo
RUN apt-get update -qq ; apt-get install -y git vim python3-pip python3-venv
ARG DEBIAN_FRONTEND=noninteractive
COPY ./entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

#OpenVRE runner installation
WORKDIR /home
RUN git clone --single-branch --branch dockerized https://github.com/inab/vre_template_tool.git
WORKDIR /home/vre_template_tool

RUN pip3 install --upgrade wheel
RUN pip3 install -r requirements.txt
RUN pip3 install --upgrade pip

#Overwrite cloned template with tool specific libs
COPY ./vre_template_tool/tool/ /home/vre_template_tool/tool/
COPY ./vre_template_tool/tests/ /home/vre_template_tool/tests/
RUN pip3 install -r tool/requirements_app.txt
RUN chmod -R 777 /home/vre_template_tool
RUN  chmod -R a+rwx /home/vre_template_tool

#Execute entrypoint 
ENTRYPOINT ["/entrypoint.sh"]

