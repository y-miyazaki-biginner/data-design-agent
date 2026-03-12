"""FastAPI サーバー - データ設計エージェントのWebアプリケーション"""

import io
import json
import os
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(str(Path(__file__).parent / ".env"), override=True)

from fastapi import FastAPI, File, Form, UploadFile, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from excel_parser import parse_multiple_files
from agent import generate_proposal, apply_feedback, confirm_generation
from csv_exporter import export_all_csv
from excel_exporter import export_excel
from data_catalog import search_tables, get_columns_for_tables, list_all_data_sources
from knowledge_db import (
    create_project,
    list_projects,
    get_project,
    get_project_history,
    get_generation,
    init_db,
)

app = FastAPI(title="データ設計エージェント")

# Static files
STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Templates
TEMPLATE_DIR = Path(__file__).parent / "templates"

# セッション内のアップロードデータを保持
_session_data: dict = {}


@app.on_event("startup")
async def startup():
    init_db()


@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = TEMPLATE_DIR / "index.html"
    return html_path.read_text(encoding="utf-8")


# === 案件管理 ===

@app.get("/api/projects")
async def api_list_projects():
    return list_projects()


@app.post("/api/projects")
async def api_create_project(
    name: str = Form(...),
    description: str = Form(""),
    client_name: str = Form(""),
):
    pid = create_project(name, description, client_name)
    return {"id": pid, "name": name}


# === Redash検索 ===

@app.post("/api/search")
async def api_search(query: str = Form(...)):
    """自然言語クエリでデータソースを検索"""
    result = search_tables(query)

    # 検索結果のテーブルのカラムデータをセッションに保持
    table_names = [r["name"] for r in result.get("results", [])]
    if table_names:
        parsed = get_columns_for_tables(table_names)
        _session_data["parsed"] = parsed

    return result


@app.get("/api/catalog")
async def api_catalog():
    """全データソース一覧を返す"""
    return list_all_data_sources()


@app.post("/api/catalog/select")
async def api_catalog_select(table_names: str = Form(...)):
    """手動選択されたテーブルのカラム情報をセッションにセット"""
    names = json.loads(table_names)
    parsed = get_columns_for_tables(names)
    _session_data["parsed"] = parsed
    return {
        "data_files": parsed["data_files"],
        "total_columns": parsed["total_columns"],
        "total_active": parsed["total_active"],
        "total_deleted": parsed["total_deleted"],
        "all_columns": parsed["all_columns"],
        "errors": parsed["errors"],
    }


# === Excelアップロード（レガシー、非表示） ===

@app.post("/api/upload")
async def api_upload(files: list[UploadFile] = File(...)):
    file_list = []
    for f in files:
        content = await f.read()
        file_list.append((f.filename, io.BytesIO(content)))

    result = parse_multiple_files(file_list)
    _session_data["parsed"] = result

    response = {
        "data_files": result["data_files"],
        "total_columns": result["total_columns"],
        "total_active": result["total_active"],
        "total_deleted": result["total_deleted"],
        "errors": result["errors"],
    }
    response["all_columns"] = result["all_columns"]
    return response


# === 生成 ===

@app.post("/api/generate/propose")
async def api_generate_propose(
    project_id: int = Form(...),
    requirement: str = Form(...),
    selected_tables: str = Form(...),  # JSON配列文字列
):
    parsed = _session_data.get("parsed")
    if not parsed:
        raise HTTPException(400, "先にデータ検索を実行してください")

    tables = json.loads(selected_tables)

    try:
        result = generate_proposal(
            project_id=project_id,
            requirement=requirement,
            columns=parsed["all_columns"],
            selected_tables=tables,
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, f"生成エラー: {str(e)}")

    # パースエラーがあっても結果は返す（UIで表示可能な範囲で）
    _session_data["last_generation"] = result
    return result


@app.post("/api/generate/feedback")
async def api_generate_feedback(
    generation_id: int = Form(...),
    project_id: int = Form(...),
    feedback_text: str = Form(...),
    selected_tables: str = Form(...),
):
    parsed = _session_data.get("parsed")
    if not parsed:
        raise HTTPException(400, "先にデータ検索を実行してください")

    tables = json.loads(selected_tables)

    try:
        result = apply_feedback(
            generation_id=generation_id,
            project_id=project_id,
            feedback_text=feedback_text,
            columns=parsed["all_columns"],
            selected_tables=tables,
        )
    except Exception as e:
        raise HTTPException(500, f"修正エラー: {str(e)}")

    _session_data["last_generation"] = result
    return result


@app.post("/api/generate/confirm")
async def api_confirm(generation_id: int = Form(...)):
    result = confirm_generation(generation_id)
    if not result:
        raise HTTPException(404, "生成結果が見つかりません")
    return result


# === CSV出力 ===

@app.get("/api/download-csv/{generation_id}")
async def api_download_csv(generation_id: int):
    gen = get_generation(generation_id)
    if not gen:
        raise HTTPException(404, "生成結果が見つかりません")

    result = {
        "data_design": gen["result_design"],
        "implementation_steps": gen["result_steps"],
    }
    csv_content = export_all_csv(result)

    # BOM付きUTF-8
    bom = "\ufeff"
    return StreamingResponse(
        io.BytesIO((bom + csv_content).encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=data_design_{generation_id}.csv"},
    )


@app.get("/api/download-excel/{generation_id}")
async def api_download_excel(generation_id: int):
    gen = get_generation(generation_id)
    if not gen:
        raise HTTPException(404, "生成結果が見つかりません")

    result = {
        "data_design": gen["result_design"],
        "implementation_steps": gen["result_steps"],
        "data_flow": gen.get("result_flow", []),
    }
    excel_bytes = export_excel(result)

    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=data_design_{generation_id}.xlsx"},
    )


@app.get("/api/download-csv-session")
async def api_download_csv_session():
    """セッション内の最新結果をCSVダウンロード"""
    last = _session_data.get("last_generation")
    if not last:
        raise HTTPException(404, "生成結果がありません")

    csv_content = export_all_csv(last)
    bom = "\ufeff"
    return StreamingResponse(
        io.BytesIO((bom + csv_content).encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=data_design.csv"},
    )


# === 履歴 ===

@app.get("/api/history/{project_id}")
async def api_project_history(project_id: int):
    return get_project_history(project_id)


@app.get("/api/generation/{generation_id}")
async def api_get_generation(generation_id: int):
    gen = get_generation(generation_id)
    if not gen:
        raise HTTPException(404, "生成結果が見つかりません")
    return gen


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
