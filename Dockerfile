# Use official Python image
FROM python:3.11-slim

WORKDIR /app/follow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8002

CMD ["uvicorn", "main:router", "--host", "0.0.0.0", "--port", "8002"]