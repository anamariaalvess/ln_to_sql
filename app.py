import logging
from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from index import askGpt, description_tables

app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/question")
async def question(request: Request, input_text: str = Form(default=None)):
    try:
        if not input_text:
            raise ValueError("Por favor, forneça uma entrada válida.")

        sql_query = askGpt(input_text)
        return templates.TemplateResponse(
            "index.html",
            {"request": request, "input_text": input_text,  "sql_query": sql_query},
        )
    except Exception as e:
        error_message = f"Ocorreu um erro: {str(e)}"
        return templates.TemplateResponse(
            "index.html",
            {"request": request, "input_text": input_text, "sql_query": ""},
        )

@app.post("/answer")
async def answer(request: Request, input_text: str = Form(default=None)):
    result, sql_query = askGpt(input_text)
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "input_text": input_text, "sql_query": sql_query},
    )

