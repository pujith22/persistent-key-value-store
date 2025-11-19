# Build stage
FROM ubuntu:22.04 AS builder

# Install dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy source files
COPY . .

# Build the application
# Using pg_config to find include and lib paths
RUN g++ -std=c++17 server.cpp main_server.cpp persistence_adapter.cpp \
    -I include -I third_party \
    -I$(pg_config --includedir) \
    -L$(pg_config --libdir) \
    -lpq \
    -o kv_server

# Runtime stage
FROM ubuntu:22.04

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/kv_server .
# Copy assets
COPY --from=builder /app/assets ./assets

# Expose port
EXPOSE 2222

# Run the server
CMD ["./kv_server", "--json-logs"]
