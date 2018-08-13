FROM debian:jessie

ARG buildn

ARG stage
ARG int

ENV BUILD_NUMBER=$buildn
ENV SOLR_VERSION=7.7.2


ENV STAGE=$stage
ENV INT=$int


RUN echo $SOLR_VERSION
RUN echo $BUILD_NUMBER

USER root

RUN echo "deb http://http.debian.net/debian jessie-backports main" | \
     tee --append /etc/apt/sources.list.d/jessie-backports.list > /dev/null && \ 
     apt-get update -y && \
     apt-get install -y \
     wget \
     curl \
     ruby \
     ruby-dev \
     rubygems \
     build-essential \
     git-core && \
     apt-get install -y -t jessie-backports openjdk-8-jdk && \
     update-java-alternatives -s java-1.8.0-openjdk-amd64 && \
     apt-get upgrade -y

RUN export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64

RUN gem install fpm
RUN fpm --version

#Installing Apache Ant
RUN wget http://archive.apache.org/dist/ant/binaries/apache-ant-1.10.5-bin.tar.gz
RUN wget https://www.apache.org/dist/ant/KEYS
RUN wget https://www.apache.org/dist/ant/binaries/apache-ant-1.10.5-bin.tar.gz.asc

#Verify download signature
RUN gpg --import KEYS
RUN gpg --verify apache-ant-1.10.5-bin.tar.gz.asc

#Unpack
RUN tar xvfvz apache-ant-1.10.5-bin.tar.gz

RUN mv apache-ant-1.10.5 /opt/ant
RUN echo ANT_HOME=/opt/ant >> /etc/environment
RUN ln -s /opt/ant/bin/ant /usr/bin/ant

#Installing Apache Ivy from git repository
RUN git clone https://git-wip-us.apache.org/repos/asf/ant-ivy.git
WORKDIR /ant-ivy
RUN ant jar
WORKDIR /ant-ivy/build/artifact/jars
RUN scp ivy.jar /opt/ant/lib

WORKDIR /usr/local
ADD . bw-lucene-solr/
WORKDIR /usr/local/bw-lucene-solr/solr
RUN ant ivy-bootstrap && \
   ant clean compile dist package


WORKDIR /usr/local/bw-lucene-solr/solr/package


RUN fpm --description "Brandwatch Solr distribution" --name solr -v $SOLR_VERSION-SNAPSHOT-bwbuild${BUILD_NUMBER} --prefix /opt -s tar -t deb solr-$SOLR_VERSION-SNAPSHOT.tgz
RUN curl http://apt.service0.btn1.bwcom.net/packages -u $INT -F my_file=@solr_$SOLR_VERSION-SNAPSHOT-bwbuild${BUILD_NUMBER}_amd64.deb -F name=solr_$SOLR_VERSION-SNAPSHOT-bwbuild${BUILD_NUMBER}_amd64.deb
RUN curl https://aptly.stage.brandwatch.net/packages -u $STAGE -F my_file=@solr_$SOLR_VERSION-SNAPSHOT-bwbuild${BUILD_NUMBER}_amd64.deb -F name=solr_$SOLR_VERSION-SNAPSHOT-bwbuild${BUILD_NUMBER}_amd64.deb