FROM google/golang

RUN mkdir -p /gopath/src/github.com/lavab/worker
ADD . /gopath/src/github.com/lavab/worker
RUN go get github.com/lavab/worker/scheduler

CMD []
ENTRYPOINT ["/gopath/bin/scheduler"]
