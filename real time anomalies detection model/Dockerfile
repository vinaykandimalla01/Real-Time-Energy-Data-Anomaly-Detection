FROM python:3.9-slim-bullseye

# Install Java and procps
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jre-headless \
    procps && \
    rm -rf /var/lib/apt/lists/*

# Configure JAVA_HOME
RUN arch=$(dpkg --print-architecture) && \
    if [ "$arch" = "amd64" ]; then \
        JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"; \
    elif [ "$arch" = "arm64" ]; then \
        JAVA_HOME="/usr/lib/jvm/java-11-openjdk-arm64"; \
    else \
        echo "Unsupported architecture: $arch"; exit 1; \
    fi && \
    ln -s $JAVA_HOME /usr/lib/jvm/default-java

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$PATH:$JAVA_HOME/bin"

# Install Python packages
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    dash \
    dash-bootstrap-components \
    pandas \
    plotly \
    confluent-kafka \
    scikit-learn

# Working directory
WORKDIR /app

# Copy the application
COPY real_time_model.py .

# Expose Dash port
EXPOSE 8050

# Start the application
CMD ["python", "real_time_model.py"]