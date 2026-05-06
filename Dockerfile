FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

CMD ["functions-framework", "--target=cyberindex_entry_point", "--port=8080", "--source=main.py"]
