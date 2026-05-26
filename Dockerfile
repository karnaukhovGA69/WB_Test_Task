FROM golang:1.23-alpine AS build

WORKDIR /src

RUN apk add --no-cache ca-certificates git

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o /out/search-trends ./cmd/app

FROM alpine:3.20

WORKDIR /app

RUN apk add --no-cache ca-certificates

COPY --from=build /out/search-trends /app/search-trends
COPY config.example.yaml /app/config/config.yaml
COPY config.docker.yaml /app/config/config.docker.yaml

EXPOSE 9090 2112

ENTRYPOINT ["/app/search-trends"]
CMD ["-config", "/app/config/config.yaml"]
