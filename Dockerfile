FROM golang:1.21.4-alpine AS build
ENV CGO_ENABLED 0
ENV GOOS linux
WORKDIR /src
ADD go.mod go.sum ./
RUN go mod download
ADD . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    go build ./cmd/server


FROM scratch
WORKDIR /app
COPY --from=build /src/server .
EXPOSE 3000 8000
CMD ["/app/server"]
