FROM golang:1.19.5-alpine AS build
ENV CGO_ENABLED 0
ENV GOOS linux
WORKDIR /src
ADD go.mod go.sum ./
RUN go mod download
ADD . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    go build github.com/maxpoletaev/kivi/cmd/kivi-server


FROM scratch
WORKDIR /app
ENV PATH /app
COPY --from=build /src/kivi-server .
EXPOSE 3000 8000
CMD ["/app/kivi-server"]
