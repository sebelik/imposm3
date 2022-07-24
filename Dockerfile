# ====================================
# Build image
# ====================================
FROM debian:8-slim AS build

WORKDIR /var/imposm

# Directories
ENV BUILD_BASE /var/imposm
ENV PREFIX $BUILD_BASE/local
ENV SRC $BUILD_BASE/src
ENV GOPATH $BUILD_BASE/gopath
ENV PATH $PATH:$BUILD_BASE/go/bin
ENV GOROOT $BUILD_BASE/go
ENV BUILD_TMP $BUILD_BASE/imposm-build
ENV IMPOSM_SRC $GOPATH/src/github.com/omniscale/imposm3

# Versions
ENV REVISION ${REVISION:-master}
ENV GEOS_VERSION 3.6.2

# Build flags
ENV CGO_CFLAGS -I$PREFIX/include
ENV CGO_LDFLAGS -L$PREFIX/lib
ENV LD_LIBRARY_PATH $PREFIX/lib

# cURL alias
ENV CURL "curl --silent --show-error --location"

# Make build directories
RUN mkdir -p $SRC && \
		mkdir -p $PREFIX/lib && \
		mkdir -p $PREFIX/include && \
		mkdir -p $GOPATH

# Install packages
RUN apt-get update -y && \
		apt-get install -y build-essential unzip autoconf libtool git chrpath curl


# Install hyperleveldb as leveldb
WORKDIR $SRC
RUN $CURL https://github.com/rescrv/HyperLevelDB/archive/master.zip -O && \
    	unzip master.zip

WORKDIR $SRC/HyperLevelDB-master
RUN autoreconf -i && \
        ./configure --prefix=$PREFIX && \
        make -j4 && \
        make install

# Link hyperleveldb as leveldb
WORKDIR $PREFIX/lib
RUN ln -sf libhyperleveldb.a libleveldb.a && \
		ln -sf libhyperleveldb.la libleveldb.la && \
		ln -sf libhyperleveldb.so libleveldb.so && \
		ln -s $PREFIX/include/hyperleveldb $PREFIX/include/leveldb

# Install Protobuf
WORKDIR $SRC
RUN $CURL https://github.com/google/protobuf/releases/download/v2.6.1/protobuf-2.6.1.tar.bz2 -O && \
        tar jxf protobuf-2.6.1.tar.bz2

WORKDIR $SRC/protobuf-2.6.1/
RUN ./configure --prefix=$PREFIX &&  \
        make -j2 && \
        make install

# Install GEOS
WORKDIR $SRC
RUN $CURL http://download.osgeo.org/geos/geos-$GEOS_VERSION.tar.bz2 -O &&  \
		tar jxf geos-$GEOS_VERSION.tar.bz2

WORKDIR $SRC/geos-$GEOS_VERSION/
RUN ./configure --prefix=$PREFIX && \
        make -j2 && \
        make install

# Install Go
WORKDIR $SRC
RUN $CURL https://storage.googleapis.com/golang/go1.17.3.linux-amd64.tar.gz -O && \
    	tar xzf go1.17.3.linux-amd64.tar.gz -C $BUILD_BASE/

# Prepare build temp dir
RUN rm -rf $BUILD_TMP && \
		mkdir -p $BUILD_TMP && \
		mkdir -p $BUILD_TMP/lib

# Copy dependency libraries to build temp dir
WORKDIR $PREFIX/lib
RUN cp libgeos_c.so $BUILD_TMP/lib && \
		ln -s libgeos_c.so $BUILD_TMP/lib/libgeos_c.so.1 && \
		cp libgeos.so $BUILD_TMP/lib && \
		ln -s libgeos.so $BUILD_TMP/lib/libgeos-$GEOS_VERSION.so && \
		cp libhyperleveldb.so $BUILD_TMP/lib && \
        ln -s libhyperleveldb.so $BUILD_TMP/lib/libhyperleveldb.so.0 && \
        ln -s libhyperleveldb.so $BUILD_TMP/lib/libleveldb.so.1

WORKDIR $BUILD_TMP/lib
RUN chrpath libgeos_c.so -r '${ORIGIN}'

# Add the source code
WORKDIR $IMPOSM_SRC
COPY . .

# Compile the imposm binary
RUN make build && \
		cp imposm $BUILD_TMP

# Make dist package from build temp dir
WORKDIR $BUILD_BASE
ENV VERSION `$BUILD_TMP/imposm version`-linux-x86-64
RUN rm -rf dist && \
		cp -R imposm-build dist && \
		chmod 777 dist/imposm