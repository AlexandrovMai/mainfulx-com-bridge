FROM golang:1.16 as build
WORKDIR /go/src/maiflux-msg-bridge/
COPY . .
RUN go mod download
RUN go build -o ./ntk ./nats-to-kafka/
RUN go build -o ./ktn ./kafka-to-nats/


FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=build /go/src/maiflux-msg-bridge/ntk ./
CMD ["./ntk"]

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=build /go/src/maiflux-msg-bridge/ktn ./
CMD ["./ktn"]