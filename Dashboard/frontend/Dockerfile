FROM node:18-alpine AS builder

WORKDIR /app

COPY package*.json ./

RUN npm install

COPY . .

RUN npm run build

# Production stage - serve with http-server
FROM node:18-alpine

WORKDIR /app

# Install http-server globally
RUN npm install -g http-server

# Copy built files from builder
COPY --from=builder /app/build ./build

EXPOSE 3000

CMD ["http-server", "build", "-p", "3000"]
