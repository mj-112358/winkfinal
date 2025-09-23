import uvicorn, os
uvicorn.run('src.dashboard.web_server:app', host='0.0.0.0', port=int(os.getenv('PORT','8000')))
