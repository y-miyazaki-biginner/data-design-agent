"""Excel Parser - 複数Excelファイルからカラム定義を読み取る"""

from pathlib import Path
from typing import BinaryIO
import openpyxl


# Excelの期待カラム名
EXPECTED_HEADERS = [
    "データファイルID",
    "データファイル名",
    "カラムID",
    "顧客IDフラグ",
    "カラム論理名",
    "カラム物理名",
    "カスタムカラム物理名",
    "カラム作成時間(JST)",
    "カラム更新時間(JST)",
    "カラム削除時間(JST)",
]


def parse_excel(file: BinaryIO, filename: str = "") -> list[dict]:
    """1つのExcelファイルをパースしてカラム情報のリストを返す"""
    wb = openpyxl.load_workbook(file, read_only=True, data_only=True)
    ws = wb.active

    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return []

    # ヘッダー行を検出（最初の行 or ヘッダーっぽい行を探す）
    header_row_idx = _find_header_row(rows)
    if header_row_idx is None:
        raise ValueError(f"ヘッダー行が見つかりません: {filename}")

    headers = [str(c).strip() if c else "" for c in rows[header_row_idx]]
    col_map = _map_columns(headers)

    results = []
    for row in rows[header_row_idx + 1:]:
        if all(c is None or str(c).strip() == "" for c in row):
            continue  # 空行スキップ

        record = _parse_row(row, col_map)
        if record:
            record["source_file"] = filename
            results.append(record)

    wb.close()
    return results


def parse_multiple_files(files: list[tuple[str, BinaryIO]]) -> dict:
    """複数Excelファイルをパースして統合結果を返す"""
    all_columns = []
    errors = []

    for filename, file_obj in files:
        try:
            columns = parse_excel(file_obj, filename)
            all_columns.extend(columns)
        except Exception as e:
            errors.append({"file": filename, "error": str(e)})

    # データファイルごとにグルーピング
    data_files = {}
    for col in all_columns:
        file_id = col["data_file_id"]
        if file_id not in data_files:
            data_files[file_id] = {
                "file_id": file_id,
                "file_name": col["data_file_name"],
                "columns": [],
                "active_count": 0,
                "deleted_count": 0,
            }
        data_files[file_id]["columns"].append(col)
        if col["is_deleted"]:
            data_files[file_id]["deleted_count"] += 1
        else:
            data_files[file_id]["active_count"] += 1

    return {
        "data_files": list(data_files.values()),
        "total_columns": len(all_columns),
        "total_active": sum(1 for c in all_columns if not c["is_deleted"]),
        "total_deleted": sum(1 for c in all_columns if c["is_deleted"]),
        "all_columns": all_columns,
        "errors": errors,
    }


def get_effective_physical_name(col: dict) -> str:
    """カスタム物理名があればそちらを優先して返す"""
    return col.get("custom_physical_name") or col.get("physical_name", "")


def get_join_keys(columns: list[dict]) -> list[dict]:
    """顧客IDフラグ=1のカラムをJOINキー候補として返す"""
    return [c for c in columns if c.get("customer_id_flag") == 1 and not c.get("is_deleted")]


def _find_header_row(rows: list) -> int | None:
    """ヘッダー行のインデックスを探す"""
    for i, row in enumerate(rows[:5]):  # 最初の5行を探索
        cells = [str(c).strip() if c else "" for c in row]
        # 「データファイルID」か「カラム論理名」が含まれていればヘッダー行
        if "データファイルID" in cells or "カラム論理名" in cells:
            return i
    # 見つからなければ最初の行をヘッダーとみなす
    return 0


def _map_columns(headers: list[str]) -> dict[str, int]:
    """ヘッダー名からカラムインデックスへのマッピングを作成"""
    col_map = {}
    for i, h in enumerate(headers):
        h_clean = h.strip()
        if h_clean in EXPECTED_HEADERS:
            col_map[h_clean] = i
    return col_map


def _parse_row(row: tuple, col_map: dict) -> dict | None:
    """1行をパースしてdictに変換"""
    def get(key: str) -> str:
        idx = col_map.get(key)
        if idx is None or idx >= len(row):
            return ""
        val = row[idx]
        return str(val).strip() if val is not None else ""

    data_file_id = get("データファイルID")
    if not data_file_id:
        return None

    deleted_time = get("カラム削除時間(JST)")
    customer_id_flag_raw = get("顧客IDフラグ")

    # 顧客IDフラグの変換（1, "1", True, "TRUE" → 1）
    try:
        customer_id_flag = int(float(customer_id_flag_raw)) if customer_id_flag_raw else 0
    except (ValueError, TypeError):
        customer_id_flag = 1 if str(customer_id_flag_raw).upper() in ("TRUE", "YES", "1") else 0

    return {
        "data_file_id": data_file_id,
        "data_file_name": get("データファイル名"),
        "column_id": get("カラムID"),
        "customer_id_flag": customer_id_flag,
        "logical_name": get("カラム論理名"),
        "physical_name": get("カラム物理名"),
        "custom_physical_name": get("カスタムカラム物理名"),
        "created_at": get("カラム作成時間(JST)"),
        "updated_at": get("カラム更新時間(JST)"),
        "deleted_at": deleted_time,
        "is_deleted": bool(deleted_time),
        "effective_physical_name": get("カスタムカラム物理名") or get("カラム物理名"),
    }
