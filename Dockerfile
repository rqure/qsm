# Build the application from source
FROM golang:1.22 AS build-stage

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -o /qapp

# Deploy the application binary into a lean image
FROM debian:bullseye-slim AS build-release-stage

WORKDIR /

COPY --from=build-stage /qapp /qapp

# Add a non-root user and group
RUN addgroup --system docker && adduser --system --ingroup docker nonroot

# Switch to the non-root user
USER nonroot:docker

ENTRYPOINT ["/qapp"]
