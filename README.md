# Investment-Intelligence-Platform

Folder structue 
```
Investment-Intelligence-Platform/
│
├── Backend/
│   ├── services/
│   │   ├── data_ingestion/
│   │   │   ├── app.py
│   │   │   ├── fetch_news.py
│   │   │   ├── fetch_stocks.py
│   │   │   ├── Dockerfile
│   │   │   └── requirements.txt
│   │   │
│   │   ├── data_cleaning/
│   │   │   ├── app.py
│   │   │   ├── clean_merge.py
│   │   │   ├── Dockerfile
│   │   │   └── requirements.txt
│   │   │
│   │   ├── embedding/
│   │   │   ├── app.py
│   │   │   ├── embed_utils.py
│   │   │   ├── Dockerfile
│   │   │   └── requirements.txt
│   │   │
│   │   ├── retrieval/
│   │   │   ├── app.py
│   │   │   ├── query_utils.py
│   │   │   ├── Dockerfile
│   │   │   └── requirements.txt
│   │   │
│   │   ├── llm/
│   │   │   ├── app.py
│   │   │   ├── openai_integration.py
│   │   │   ├── Dockerfile
│   │   │   └── requirements.txt
│   │
│   ├── api_gateway/
│   │   ├── app.py
│   │   ├── router.py
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   ├── shared_utils/
│   │   ├── config.py
│   │   ├── logging_utils.py
│   │   └── constants.py
│   │
│   ├── docker-compose.yml
│   └── .env
│
└── Frontend/
    ├── src/
    └── package.json

```