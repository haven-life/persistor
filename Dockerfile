FROM node:8.3-alpine

EXPOSE 3001
RUN apk update && apk add --no-cache bash

WORKDIR /app