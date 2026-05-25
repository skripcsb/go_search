FROM golang:1.22-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go env -w GOPROXY=https://proxy.golang.org,direct
RUN go mod download
COPY . .
RUN go build -o /out/searchtrends ./cmd/searchtrends
RUN go build -o /out/publisher ./cmd/publisher

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /out/searchtrends /app/searchtrends
COPY --from=build /out/publisher /app/publisher
EXPOSE 8080
CMD ["/app/searchtrends"]