#TODO
# remove after release of go 1.18
FROM golang:1.17 AS goenv
RUN go install golang.org/dl/gotip@latest
RUN gotip download


#TODO
# replace 'FROM goenv AS builder' with 'FROM golang:1.18 AS builder' after release of go 1.18
FROM goenv AS builder

COPY . /go/src/app
WORKDIR /go/src/app

ENV GO111MODULE=on

#TODO
# replace 'gotip' with 'go' on release of go 1.18
RUN CGO_ENABLED=0 GOOS=linux gotip build -o app

RUN git log -1 --oneline > version.txt

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /go/src/app/app .
COPY --from=builder /go/src/app/config.json .
COPY --from=builder /go/src/app/version.txt .

EXPOSE 8080

ENTRYPOINT ["./app"]