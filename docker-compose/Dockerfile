FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy app and requirements
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run the Python script
CMD ["python", "app.py"]
#CMD ["ping", "jasaserver01"]
