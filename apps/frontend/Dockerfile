FROM node:20-alpine

WORKDIR /app

COPY package.json /app/package.json
RUN npm install

COPY tsconfig.json /app/tsconfig.json
COPY tsconfig.node.json /app/tsconfig.node.json
COPY vite.config.ts /app/vite.config.ts
COPY index.html /app/index.html
COPY src /app/src

EXPOSE 5173
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "5173"]
