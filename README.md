# BDGB
## Overview
BDGB is a database session management tool intended to run as a daemon and keep track of database sessions.

The purpose is to make the it simpler to create good database plugins that don't need to deal with the unnecessary complications of database session management.

Furthermore, I'd like to create a tool that allows me to handle databases without being forced into a clunky GUI.
## Interface
The API supports the following endpoints:

```
@app.post("/create_session")
async def create_session(db_config: DBConfig) -> str:

@app.get("/get_sessions")
async def get_sessions() -> list[DBSession]:

@app.get("/delete_session/{session_id}")
async def delete_session(session_id: str) -> str:

@app.post("/query")
async def query(q: Query) -> Optional[str]:
```
