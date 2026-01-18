FROM node:20-alpine

WORKDIR /app

COPY package.json /app/package.json
COPY tsconfig.json /app/tsconfig.json
RUN npm install

COPY src /app/src
RUN npm run build

EXPOSE 3000
CMD ["npm", "start"]
