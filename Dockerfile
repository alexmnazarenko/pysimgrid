FROM python:3.6.15-slim as base

ENV LATEST_VERSION v3_13
ARG VERSION=${LATEST_VERSION}
ARG URL=https://framagit.org/simgrid/simgrid/-/archive/${VERSION}/simgrid-${VERSION}.tar.gz

ARG SCRIPT_DIR=/home/pysimgrid
ARG OPT_ROOT=$SCRIPT_DIR/opt
ARG PKG_ROOT=$OPT_ROOT/SimGrid

RUN apt update -q && apt install -q wget curl build-essential cmake libboost-context-dev libboost-program-options-dev libboost-filesystem-dev doxygen graphviz-dev libgraphviz-dev -y

RUN useradd -ms /bin/bash pysimgrid
WORKDIR /home/pysimgrid/


RUN mkdir -p $PKG_ROOT/src
RUN wget -P  $PKG_ROOT/src $URL --no-check-certificate
RUN cd $PKG_ROOT/src && tar xzf $PKG_ROOT/src/simgrid-${VERSION}.tar.gz


RUN cd $PKG_ROOT/src/simgrid-${VERSION} && cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=$PKG_ROOT .
RUN cd $PKG_ROOT/src/simgrid-${VERSION} && make
RUN cd $PKG_ROOT/src/simgrid-${VERSION} && make install


RUN pip3 install Cython==0.29.36 numpy==1.19.0 setuptools==44.1.0 networkx==2.4


COPY . $SCRIPT_DIR

RUN cd $SCRIPT_DIR && python3 setup.py build_ext --inplace

RUN cd $SCRIPT_DIR && python3 run_tests.py

RUN chown -R pysimgrid /home/pysimgrid
 
USER pysimgrid

CMD ["bash"]
