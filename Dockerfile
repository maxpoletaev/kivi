FROM golang:1.18.4-alpine AS build
ENV CGO_ENABLED 0
ENV GOOS linux
WORKDIR /src
ADD go.mod go.sum ./
RUN go mod download
ADD . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    go build github.com/maxpoletaev/kv/cmd/server


FROM alpine:3.16
WORKDIR /app
ENV PATH /app
COPY --from=build /src/server .
EXPOSE 3000
