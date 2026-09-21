FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Ensure logs directory exists
RUN mkdir -p /app/logs

COPY start.sh /start.sh
RUN chmod +x /start.sh

CMD ["/start.sh"]