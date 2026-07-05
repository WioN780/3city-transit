FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /out/watchdog .

FROM alpine:3.22
COPY --from=build /out/watchdog /usr/local/bin/watchdog
ENTRYPOINT ["/usr/local/bin/watchdog"]
