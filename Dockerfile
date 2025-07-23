FROM golang:1.24-alpine
RUN apk add --no-cache ca-certificates tzdata git
WORKDIR /root/app
RUN go install github.com/air-verse/air@latest
# COPY ./go.mod ./go.sum ./
COPY ./go.mod ./
RUN go mod download
COPY . .
CMD ["air"]
