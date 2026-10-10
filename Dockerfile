FROM node:20-alpine
WORKDIR /app
COPY package.json ./
COPY src/ ./src/
COPY tests/ ./tests/
RUN node tests/eventflux.test.js
EXPOSE 8080
CMD ["node", "tests/eventflux.test.js"]
