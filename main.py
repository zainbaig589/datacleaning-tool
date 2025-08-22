from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse, JSONResponse
import pandas as pd
import io

app = FastAPI()
cleaned_df = None

def auto_clean(df: pd.DataFrame) -> pd.DataFrame:
    # Deduplicate
    df = df.drop_duplicates()
    # Trim whitespace
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    # Handle missing values
    for col in df.columns:
        if df[col].dtype in ['float64','int64']:
            df[col] = df[col].fillna(df[col].median())
        else:
            df[col] = df[col].fillna("Missing")
    # Type conversion
    for col in df.columns:
        try:
            df[col] = pd.to_datetime(df[col])
        except: 
            try:
                df[col] = pd.to_numeric(df[col])
            except:
                pass
    return df

@app.post("/clean")
async def clean(file: UploadFile = File(...)):
    global cleaned_df
    content = await file.read()
    if file.filename.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(content))
    elif file.filename.endswith(".xlsx"):
        df = pd.read_excel(io.BytesIO(content))
    else:
        df = pd.read_json(io.BytesIO(content))

    cleaned_df = auto_clean(df)
    return JSONResponse({"preview": cleaned_df.head(20).to_dict(orient="records")})

@app.get("/export")
async def export():
    global cleaned_df
    buf = io.StringIO()
    cleaned_df.to_csv(buf, index=False)
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv", headers={
        "Content-Disposition": "attachment; filename=cleaned_data.csv"
    })