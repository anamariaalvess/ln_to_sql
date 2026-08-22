from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from index import gerar_sql


app = FastAPI(
    title="Natural Language to SQL",
    description="Aplicação para geração de consultas SQL a partir de linguagem natural.",
)


# Arquivos estáticos
app.mount("/static", StaticFiles(directory="static"), name="static")


# Templates HTML
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """
    Renderiza a página inicial da aplicação.
    """
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "input_text": "",
            "sql_query": "",
            "error_message": "",
        },
    )


@app.post("/question", response_class=HTMLResponse)
async def question(
    request: Request,
    input_text: str = Form(...),
):
    """
    Recebe uma pergunta em linguagem natural e gera
    a consulta SQL correspondente.
    """

    input_text = input_text.strip()

    if not input_text:
        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context={
                "input_text": "",
                "sql_query": "",
                "error_message": "Informe uma pergunta antes de gerar a consulta.",
            },
        )

    try:
        sql_query = gerar_sql(input_text)

        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context={
                "input_text": input_text,
                "sql_query": sql_query,
                "error_message": "",
            },
        )

    except Exception as error:
        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context={
                "input_text": input_text,
                "sql_query": "",
                "error_message": f"Não foi possível gerar a consulta SQL: {error}",
            },
        )