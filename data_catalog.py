"""データカタログ - Redash風のデータソース検索（デモ用：事前準備データから検索）"""

import json
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(str(Path(__file__).parent / ".env"), override=True)

from anthropic import Anthropic

# カタログキャッシュ
_catalog_cache = None

CATALOG_PATH = Path(__file__).parent / "data" / "catalog.json"

client = Anthropic()
SEARCH_MODEL = "claude-haiku-4-5-20251001"


def load_catalog() -> dict:
    """カタログJSONを読み込みキャッシュする"""
    global _catalog_cache
    if _catalog_cache is not None:
        return _catalog_cache
    with open(CATALOG_PATH, "r", encoding="utf-8") as f:
        _catalog_cache = json.load(f)
    return _catalog_cache


def get_catalog_summary() -> str:
    """カタログの概要テキストを生成（AI検索プロンプト用）"""
    catalog = load_catalog()
    lines = []
    for ds in catalog["data_sources"]:
        col_names = [c["logical_name"] for c in ds["columns"]]
        lines.append(
            f"- ID: {ds['id']} | {ds['name']} ({ds['record_count']:,}件) | {ds['description']}"
        )
        lines.append(f"  カラム: {', '.join(col_names)}")
        lines.append(f"  タグ: {', '.join(ds['tags'])}")
    return "\n".join(lines)


def search_tables(query: str) -> dict:
    """自然言語クエリから関連テーブルをAI検索する"""
    catalog = load_catalog()
    summary = get_catalog_summary()

    system_prompt = f"""あなたはデータ分析アシスタントです。
ユーザーの自然言語の要件に基づいて、以下のデータソースカタログから関連するテーブルを特定してください。

【データソースカタログ】
{summary}

【出力形式】
必ず以下のJSON形式のみを出力してください。マークダウンのコードブロックは使わないでください。

{{
  "results": [
    {{
      "data_source_id": "DS001",
      "name": "テーブル名",
      "relevance_score": 0.95,
      "reason": "このテーブルが必要な理由（日本語）",
      "matched_columns": ["関連するカラムの論理名リスト"]
    }}
  ]
}}

【ルール】
- relevance_scoreは0.0〜1.0で、0.3以上のもののみ返す
- relevance_scoreの降順でソートする
- JOINに必要なマスタテーブルも含める（例：購買集計なら顧客マスタも必要）
- reasonは日本語で簡潔に書く"""

    try:
        response = client.messages.create(
            model=SEARCH_MODEL,
            max_tokens=2000,
            system=system_prompt,
            messages=[
                {"role": "user", "content": f"以下の要件に関連するデータソースを特定してください。\n\n{query}"}
            ],
        )

        result_text = response.content[0].text.strip()
        if result_text.startswith("```"):
            lines = result_text.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            result_text = "\n".join(lines)

        try:
            search_result = json.loads(result_text)
        except json.JSONDecodeError:
            start = result_text.find("{")
            end = result_text.rfind("}") + 1
            if start >= 0 and end > start:
                search_result = json.loads(result_text[start:end])
            else:
                return _fallback_search(query)

        # カタログから詳細情報を付与
        enriched = []
        for r in search_result.get("results", []):
            ds = _get_data_source(r["data_source_id"])
            if ds:
                enriched.append({
                    **r,
                    "description": ds["description"],
                    "record_count": ds["record_count"],
                    "tags": ds["tags"],
                    "column_count": len(ds["columns"]),
                    "columns": [
                        {"logical_name": c["logical_name"], "physical_name": c["physical_name"], "data_type": c["data_type"]}
                        for c in ds["columns"]
                    ],
                })

        return {"query": query, "results": enriched}

    except Exception as e:
        print(f"AI検索エラー: {e}, フォールバック検索に切り替え")
        return _fallback_search(query)


def _fallback_search(query: str) -> dict:
    """キーワードマッチングによるフォールバック検索"""
    catalog = load_catalog()
    query_upper = query.upper()

    results = []
    for ds in catalog["data_sources"]:
        score = 0.0
        matched = []

        # テーブル名マッチ
        if ds["name"] in query:
            score += 0.5
        # 説明マッチ
        if any(word in ds["description"] for word in query.replace("、", " ").replace("。", " ").split() if len(word) >= 2):
            score += 0.3
        # タグマッチ
        for tag in ds["tags"]:
            if tag in query:
                score += 0.2
                matched.append(tag)
        # カラム名マッチ
        for col in ds["columns"]:
            if col["logical_name"] in query or col["physical_name"].upper() in query_upper:
                score += 0.15
                matched.append(col["logical_name"])

        if score >= 0.3:
            results.append({
                "data_source_id": ds["id"],
                "name": ds["name"],
                "relevance_score": min(score, 1.0),
                "reason": f"キーワードマッチ: {', '.join(matched) if matched else ds['name']}",
                "matched_columns": matched,
                "description": ds["description"],
                "record_count": ds["record_count"],
                "tags": ds["tags"],
                "column_count": len(ds["columns"]),
                "columns": [
                    {"logical_name": c["logical_name"], "physical_name": c["physical_name"], "data_type": c["data_type"]}
                    for c in ds["columns"]
                ],
            })

    results.sort(key=lambda x: x["relevance_score"], reverse=True)
    return {"query": query, "results": results}


def _get_data_source(ds_id: str) -> dict | None:
    """IDからデータソースを取得"""
    catalog = load_catalog()
    for ds in catalog["data_sources"]:
        if ds["id"] == ds_id:
            return ds
    return None


def get_columns_for_tables(table_names: list[str]) -> dict:
    """選択テーブルのカラムをagent.py互換形式で返す

    parse_multiple_files()と同じ出力形式を生成し、
    _session_data["parsed"]にそのまま格納できるようにする。
    """
    catalog = load_catalog()

    data_files = []
    all_columns = []

    for ds in catalog["data_sources"]:
        if ds["name"] not in table_names:
            continue

        file_columns = []
        active_count = 0
        deleted_count = 0

        for col in ds["columns"]:
            is_deleted = col.get("is_deleted", False)
            if is_deleted:
                deleted_count += 1
            else:
                active_count += 1

            custom = col.get("custom_physical_name", "")
            column_dict = {
                "data_file_id": ds["id"],
                "data_file_name": ds["name"],
                "column_id": col["column_id"],
                "customer_id_flag": col.get("customer_id_flag", 0),
                "logical_name": col["logical_name"],
                "physical_name": col["physical_name"],
                "custom_physical_name": custom,
                "created_at": "",
                "updated_at": "",
                "deleted_at": "",
                "is_deleted": is_deleted,
                "effective_physical_name": custom if custom else col["physical_name"],
                "source_file": "Redash",
            }
            all_columns.append(column_dict)
            file_columns.append(column_dict)

        data_files.append({
            "file_id": ds["id"],
            "file_name": ds["name"],
            "columns": file_columns,
            "active_count": active_count,
            "deleted_count": deleted_count,
        })

    total_active = sum(df["active_count"] for df in data_files)
    total_deleted = sum(df["deleted_count"] for df in data_files)

    return {
        "data_files": data_files,
        "all_columns": all_columns,
        "total_columns": len(all_columns),
        "total_active": total_active,
        "total_deleted": total_deleted,
        "errors": [],
    }


def list_all_data_sources() -> list[dict]:
    """全データソースの概要リストを返す"""
    catalog = load_catalog()
    return [
        {
            "id": ds["id"],
            "name": ds["name"],
            "description": ds["description"],
            "record_count": ds["record_count"],
            "tags": ds["tags"],
            "column_count": len(ds["columns"]),
        }
        for ds in catalog["data_sources"]
    ]
