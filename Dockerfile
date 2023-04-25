FROM golang:1.20-alpine3.17

RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories

RUN apk add --no-cache \
        --virtual=.build-dependencies \
        build-base \
        autoconf \
    && apk add --no-cache \
        git \
    && GO111MODULE=off go get github.com/sillydong/goose/cmd/goose github.com/go-sql-driver/mysql github.com/lib/pq \
    && go clean -cache \
    && apk del .build-dependencies \
    && apk del git \
    && rm -rf /tmp/*