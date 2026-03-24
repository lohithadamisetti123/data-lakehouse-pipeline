# Build stage for React UI
FROM node:20-alpine AS ui-build

WORKDIR /app

# Install UI dependencies
COPY explorer/ui/package.json explorer/ui/package-lock.json ./
RUN npm install

# Copy UI source and build
COPY explorer/ui ./
ARG REACT_APP_API_BASE_URL=http://localhost:8000
ENV REACT_APP_API_BASE_URL=${REACT_APP_API_BASE_URL}
RUN npm run build


# Final image with Python + Flask + built UI
FROM python:3.11-slim AS runtime

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    build-essential \
 && rm -rf /var/lib/apt/lists/*

# Avoid pip hash checking / strict secure-installs issues in this image[web:38]
ENV PIP_NO_VERIFY=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_ROOT_USER_ACTION=ignore

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend code
COPY pipeline ./pipeline
COPY explorer/api ./explorer/api
COPY docs ./docs

# Copy built UI as static files
COPY --from=ui-build /app/build ./explorer/ui-build

# Copy entrypoint
COPY docker-entrypoint.sh ./docker-entrypoint.sh
RUN chmod +x ./docker-entrypoint.sh

# Environment
ENV FLASK_APP=explorer.api.app
ENV FLASK_ENV=production
ENV FLASK_HOST=0.0.0.0
ENV FLASK_PORT=8000
ENV LAKEHOUSE_BASE_PATH=/app/data/lakehouse

EXPOSE 8000

CMD ["./docker-entrypoint.sh"]
