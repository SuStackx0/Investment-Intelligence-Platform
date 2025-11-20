
FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8010

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8010"]

# docker run -d \                                                                
#   --name data_ingestion_service \
#   -p 8010:8010 \
#   -v "/Users/sumanthg/Documents/sug/projects/Intelligent-investement-platform/Backend/services/data_ingestion/logs:/app/logs" \
#   -v "/Users/sumanthg/Documents/sug/projects/Intelligent-investement-platform/Backend/services/data_ingestion/outputs/company_data:/app/outputs/company_data" \
#   -v "/Users/sumanthg/Documents/sug/projects/Intelligent-investement-platform/Backend/services/data_ingestion/outputs/news:/app/outputs/news" \
#   -v "/Users/sumanthg/Documents/sug/projects/Intelligent-investement-platform/Backend/services/data_ingestion/outputs/stock_data:/app/outputs/stock_data" \
#   -v "/Users/sumanthg/Documents/sug/projects/Intelligent-investement-platform/Backend/services/data_ingestion/outputs/merged_source:/app/outputs/merged_source" \
#   data_ingestion_service