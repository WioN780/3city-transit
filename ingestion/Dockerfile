FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /out/ingestion .

FROM alpine:3.22
COPY --from=build /out/ingestion /usr/local/bin/ingestion
EXPOSE 8091
ENTRYPOINT ["/usr/local/bin/ingestion"]
