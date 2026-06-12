FROM golang:1.26 AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=${VERSION}" -o /out/sluice ./cmd/sluice

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/sluice /sluice
EXPOSE 8080
ENTRYPOINT ["/sluice"]
