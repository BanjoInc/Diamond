FROM centos:7.2.1511
MAINTAINER "Gregory G. Tseng" <gregory@teambanjo.com>

ENV LANG en_US.UTF-8

# Package dependencies for various tooling
RUN yum install -y epel-release
RUN yum update -y
RUN yum groupinstall "Development Tools" -y

# Packages for Diamond
RUN yum install -y python-configobj
RUN yum install -y python-setuptools
