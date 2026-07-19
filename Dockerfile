FROM golang:1.24 AS build
WORKDIR /src
COPY . .
ARG SERVICE
RUN CGO_ENABLED=0 go build -trimpath -o /out/app ./services/${SERVICE}/

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]