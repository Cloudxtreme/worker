FROM google/golang

ENV PATH /gopath/bin:$PATH

RUN mkdir -p /gopath/src/github.com/lavab/worker
ADD . /gopath/src/github.com/lavab/worker

RUN go get github.com/lavab/worker/runner
RUN go get github.com/lavab/worker/scheduler
