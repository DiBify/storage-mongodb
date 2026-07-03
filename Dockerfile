FROM php:8.2-cli

# System deps for building the mongodb extension and running composer
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        unzip \
        libssl-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

# ext-mongodb ^2.1 (see composer.json)
RUN pecl install mongodb \
    && docker-php-ext-enable mongodb

# Composer
COPY --from=composer:2 /usr/bin/composer /usr/bin/composer

WORKDIR /app

CMD ["vendor/bin/phpunit"]
