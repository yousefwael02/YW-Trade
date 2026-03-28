# EGX Stocks Streamlit App

This repository is cleaned to a Streamlit-first layout.

## 1) Install Dependencies

```powershell
Set-Location "d:/Projects/EGX Stocks/backend"
& "d:/Projects/EGX Stocks/backend/env/Scripts/python.exe" -m pip install -r requirements.txt
```

## 2) Run Dashboard

```powershell
Set-Location "d:/Projects/EGX Stocks"
& "d:/Projects/EGX Stocks/backend/env/Scripts/python.exe" -m streamlit run backend/egx_stocks.py
```

Open `http://localhost:8501`.

## Notes

- Daily CSV refresh script: `daily_fetch.py`
- Streamlit entrypoint: `backend/egx_stocks.py`
- Symbol source file: `backend/EGX100.xlsx`
- Output CSV file: `EGX_latest_close_52week.csv`
