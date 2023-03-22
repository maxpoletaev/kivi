FROM golang:1.19.5-alpine AS build
ENV CGO_ENABLED 0
ENV GOOS linux
WORKDIR /src
ADD go.mod go.sum ./
RUN go mod download
ADD . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    go build github.com/maxpoletaev/kivi/cmd/server


FROM alpine:3.17.1
WORKDIR /app
ENV PATH /app
COPY --from=build /src/server .
EXPOSE 3000
