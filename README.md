[![master](https://img.shields.io/travis/emc-mongoose/mongoose-storage-driver-s3/master.svg)](https://travis-ci.org/emc-mongoose/mongoose-storage-driver-s3)
[![downloads](https://img.shields.io/github/downloads/emc-mongoose/mongoose-storage-driver-s3/total.svg)](https://github.com/emc-mongoose/mongoose-storage-driver-s3/releases)
[![release](https://img.shields.io/github/release/emc-mongoose/mongoose-storage-driver-s3.svg)]()
[![Docker Pulls](https://img.shields.io/docker/pulls/emcmongoose/mongoose-storage-driver-s3.svg)](https://hub.docker.com/r/emcmongoose/mongoose-storage-driver-s3/)

[Mongoose](https://github.com/emc-mongoose/mongoose-base)'s driver
generic Amazon S3 cloud storage.

# Introduction

The storage driver extends the Mongoose's [Abstract HTTP Storage Driver](https://github.com/emc-mongoose/mongoose-base/wiki/v3.6-Extensions#231-http-storage-driver)

# Features

* API version: 2006-03-01
* Authentification:
    * Uid/secret key pair to sign each request
* SSL/TLS
* Item types:
    * `data`
    * `path`
* Automatic destination path creation on demand
* Path listing input (with XML response payload)
* Data item operation types:
    * `create`
        * copy
        * Multipart Upload
    * `read`
        * full
        * random byte ranges
        * fixed byte ranges
        * content verification
    * `update`
        * full (overwrite)
        * random byte ranges
        * fixed byte ranges (with append mode)
    * `delete`
    * `noop`
* Path item operation types:
    * `create`
    * `read`
    * `delete`
    * `noop`

# Usage

Latest stable pre-built jar file is available at:
https://github.com/emc-mongoose/mongoose-storage-driver-s3/releases/download/latest/mongoose-storage-driver-s3.jar
This jar file may be downloaded manually and placed into the `ext`
directory of Mongoose to be automatically loaded into the runtime.

```bash
java -jar mongoose-<VERSION>/mongoose.jar \
    --storage-driver-type=s3 \
    ...
```

## Notes

* A **bucket** may be specified with `item-input-path` either `item-output-path` configuration option
* Multipart upload should be enabled using the `item-data-ranges-threshold` configuration parameter

## Docker

### Standalone

```bash
docker run \
    --network host \
    --entrypoint mongoose \
    emcmongoose/mongoose-storage-driver-s3 \
    -jar /opt/mongoose/mongoose.jar \
    --storage-type=s3 \
    ...
```

### Distributed

#### Drivers

```bash
docker run \
    --network host \
    --expose 1099 \
    emcmongoose/mongoose-storage-driver-service-s3
```

#### Controller

```bash
docker run \
    --network host \
    --entrypoint mongoose \
    emcmongoose/mongoose-base \
    -jar /opt/mongoose/mongoose.jar \
    --storage-driver-remote \
    --storage-driver-addrs=<ADDR1,ADDR2,...> \
    --storage-driver-type=s3 \
    ...
```

## Advanced

### Sources

```bash
git clone https://github.com/emc-mongoose/mongoose-storage-driver-s3.git
cd mongoose-storage-driver-s3
```

### Test

```
./gradlew clean test
```

### Build

```bash

./gradlew clean jar
```

### Embedding

```groovy
compile group: 'com.github.emc-mongoose', name: 'mongoose-storage-driver-s3', version: '<VERSION>'
```


