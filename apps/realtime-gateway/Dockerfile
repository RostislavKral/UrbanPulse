FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json /app/
COPY tsconfig.json /app/tsconfig.json
RUN npm ci

COPY src /app/src
RUN npm run build

EXPOSE 3000
CMD ["npm", "start"]
