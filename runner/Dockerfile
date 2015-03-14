FROM google/golang

RUN mkdir -p /gopath/src/github.com/lavab/worker
ADD . /gopath/src/github.com/lavab/worker
RUN go get github.com/lavab/worker/runner

CMD []
ENTRYPOINT ["/gopath/bin/runner"]
