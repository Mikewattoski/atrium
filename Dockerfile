# Build any service with:  docker build --build-arg SERVICE=api -t atrium-api .
ARG GO_VERSION=1.24

FROM golang:${GO_VERSION}-alpine AS build
ARG SERVICE
WORKDIR /src

# dependencies first, so this layer is cached until go.mod/go.sum change
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN test -d ./cmd/${SERVICE} || (echo "unknown SERVICE '${SERVICE}'" && exit 1)
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
