FROM node:8.3-alpine

EXPOSE 3001
RUN apk update && apk add --no-cache bash git

# WORKDIR /app
COPY ./wait-for-it.sh .
COPY ./ ./
# COPY ./node_modules /app/node_modules
# RUN npm install --production=false
# RUN npm install supertype@github:haven-life/supertype#bug/fixing-decorator-supertypeclass

# RUN ls /app/node_modules/supertype