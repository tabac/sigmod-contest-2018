FROM ubuntu:17.10

# Declare `localuser` as build arg.
ARG localuser

# Create host user.
RUN useradd -mU $localuser

# Create user with home directory.
RUN useradd -mU sut
RUN useradd -mG sut contest

# Fix missing apt-utils.
RUN apt-get update && apt-get install -y --no-install-recommends apt-utils

# Update packages.
RUN apt-get update && apt-get dist-upgrade -y

# Install allowed packages.
RUN apt-get install -y curl sudo \
                       gdb \
                       valgrind \
                       autoconf=2.69-11 \
                       automake=1:1.15-6ubuntu1 \
                       cmake=3.9.1-1 \
                       golang-1.8-go=1.8.3-2ubuntu1 golang-go=2:1.8~1ubuntu1 \
                       ant=1.9.9-4 \
                       maven=3.5.0-6 \
                       nodejs=6.11.4~dfsg-1ubuntu1 \
                       python=2.7.14-2ubuntu1 \
                       python3=3.6.3-0ubuntu2 \
                       gcc=4:7.2.0-1ubuntu1 \
                       gccgo=4:7.2.0-1ubuntu1 \
                       libjemalloc-dev=3.6.0-10 \
					   libboost-dev=1.62.0.1 \
					   clang-5.0=1:5.0-3 \
                       libtbb-dev=2017~U7-6 \
					   python-pip=9.0.1-2 \
					   build-essential=12.4ubuntu1 

# Install Oracle Java 9.
RUN apt-get install -y software-properties-common
RUN add-apt-repository -y ppa:webupd8team/java
RUN apt-get update
RUN echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections
RUN echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections
RUN apt-get install -y oracle-java9-installer

# Install ruby.
RUN export DEBIAN_FRONTEND=noninteractive DEBCONF_NONINTERACTIVE_SEEN=true
RUN echo "tzdata tzdata/Areas select Etc" | debconf-set-selections
RUN echo "tzdata tzdata/Zones/Etc select UTC" | debconf-set-selections
RUN apt-get install -y tzdata ruby-full=1:2.3.3

# Install rust.
RUN curl -sSf https://static.rust-lang.org/rustup.sh -o rustup.sh && sh rustup.sh --yes --revision=1.23.0 && rm rustup.sh

# Create code folder.
RUN mkdir /code
RUN chown -R $localuser:$localuser /code

# Change user.
# USER contest
