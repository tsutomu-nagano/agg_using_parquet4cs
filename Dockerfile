FROM ubuntu:18.04

RUN apt-get -y update
RUN apt-get install -y tzdata && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV TZ Asia/Tokyo

RUN apt-get -y update && apt-get install -y wget

RUN wget https://packages.microsoft.com/config/ubuntu/18.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
RUN dpkg -i packages-microsoft-prod.deb
RUN rm packages-microsoft-prod.deb

RUN apt-get -y update && apt-get install -y dotnet-sdk-7.0

RUN apt-get update && apt-get install -y language-pack-ja && \
    update-locale LANG=ja_JP.UTF-8 && rm -rf /var/lib/apt/lists/*
ENV LANG ja_JP.UTF-8
ENV LANGUAGE ja_JP:ja
ENV LC_ALL ja_JP.UTF-8


WORKDIR /workspaces

