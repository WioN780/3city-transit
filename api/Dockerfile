FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -o /out/api .

# go-duckdb links DuckDB's static libs against glibc + a dynamic libstdc++
# (see duckdb-go-bindings' cgo_static.go: "-lstdc++ -lm -ldl") -- musl-based
# Alpine can't satisfy that, hence Debian instead of the alpine base the
# rest of this repo's Go services use.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libstdc++6 ca-certificates \
	&& rm -rf /var/lib/apt/lists/*
COPY --from=build /out/api /usr/local/bin/api
EXPOSE 8090
ENTRYPOINT ["/usr/local/bin/api"]
