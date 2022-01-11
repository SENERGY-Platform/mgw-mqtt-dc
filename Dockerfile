#TODO
#replace rc with stable tag
FROM golang:1.18-rc AS builder

COPY . /go/src/app
WORKDIR /go/src/app

ENV GO111MODULE=on

RUN CGO_ENABLED=0 GOOS=linux go build -o app

RUN git log -1 --oneline > version.txt
RUN go version -m app >> version.txt

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /go/src/app/app .
COPY --from=builder /go/src/app/config.json .
COPY --from=builder /go/src/app/version.txt .

RUN mkdir -m 666 -p topicdescriptions/userdefined topicdescriptions/generated

EXPOSE 8080

ENTRYPOINT ["./app"]