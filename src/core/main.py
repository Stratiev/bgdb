import uvicorn

if __name__ == "__main__":
    uvicorn.run("src.api.api:app", port=8948, log_level="info")
