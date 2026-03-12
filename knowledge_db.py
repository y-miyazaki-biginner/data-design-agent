"""ナレッジDB - SQLiteで案件管理・生成履歴・フィードバック蓄積を管理"""

import hashlib
import json
import sqlite3
import time
from datetime import datetime
from pathlib import Path


DB_PATH = Path(__file__).parent / "knowledge.db"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _execute_with_retry(func, max_retries=3):
    """database is locked エラー時にリトライ"""
    for attempt in range(max_retries):
        try:
            return func()
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                time.sleep(0.5 * (attempt + 1))
                continue
            raise


def init_db():
    """DB初期化（テーブル作成）"""
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            name TEXT NOT NULL,
            description TEXT,
            client_name TEXT
        );

        CREATE TABLE IF NOT EXISTS generation_history (
            id INTEGER PRIMARY KEY,
            project_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'draft',
            requirement TEXT,
            source_tables TEXT,
            source_columns TEXT,
            result_design TEXT,
            result_steps TEXT,
            result_flow TEXT,
            user_feedback TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id)
        );

        CREATE TABLE IF NOT EXISTS learned_patterns (
            id INTEGER PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            pattern_name TEXT,
            description TEXT,
            trigger_keywords TEXT,
            table_structure_hash TEXT,
            bdash_steps TEXT,
            sql_template TEXT
        );

        CREATE TABLE IF NOT EXISTS feedback_history (
            id INTEGER PRIMARY KEY,
            generation_id INTEGER NOT NULL,
            project_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            feedback_type TEXT,
            feedback_text TEXT,
            affected_step INTEGER,
            original_content TEXT,
            corrected_content TEXT,
            FOREIGN KEY (generation_id) REFERENCES generation_history(id),
            FOREIGN KEY (project_id) REFERENCES projects(id)
        );
    """)
    # マイグレーション: result_flowカラムが無ければ追加
    try:
        conn.execute("SELECT result_flow FROM generation_history LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE generation_history ADD COLUMN result_flow TEXT")

    conn.commit()
    conn.close()


# === 案件管理 ===

def create_project(name: str, description: str = "", client_name: str = "") -> int:
    conn = get_connection()
    cursor = conn.execute(
        "INSERT INTO projects (name, description, client_name) VALUES (?, ?, ?)",
        (name, description, client_name),
    )
    project_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return project_id


def list_projects() -> list[dict]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT id, name, description, client_name, created_at FROM projects ORDER BY updated_at DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_project(project_id: int) -> dict | None:
    conn = get_connection()
    row = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


# === 生成履歴 ===

def _ensure_project_exists(conn, project_id: int):
    """project_idが存在しない場合は自動作成"""
    row = conn.execute("SELECT id FROM projects WHERE id = ?", (project_id,)).fetchone()
    if not row:
        conn.execute(
            "INSERT INTO projects (id, name) VALUES (?, ?)",
            (project_id, f"プロジェクト{project_id}"),
        )


def save_generation(
    project_id: int,
    requirement: str,
    source_tables: list,
    source_columns: list,
    result_design: dict,
    result_steps: list,
    result_flow: list = None,
    status: str = "draft",
) -> int:
    def _do():
        conn = get_connection()
        try:
            _ensure_project_exists(conn, project_id)
            cursor = conn.execute(
                """INSERT INTO generation_history
                (project_id, status, requirement, source_tables, source_columns, result_design, result_steps, result_flow)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    project_id,
                    status,
                    requirement,
                    json.dumps(source_tables, ensure_ascii=False),
                    json.dumps(source_columns, ensure_ascii=False),
                    json.dumps(result_design, ensure_ascii=False),
                    json.dumps(result_steps, ensure_ascii=False),
                    json.dumps(result_flow or [], ensure_ascii=False),
                ),
            )
            gen_id = cursor.lastrowid
            conn.commit()
            return gen_id
        finally:
            conn.close()
    return _execute_with_retry(_do)


def get_generation(generation_id: int) -> dict | None:
    conn = get_connection()
    row = conn.execute("SELECT * FROM generation_history WHERE id = ?", (generation_id,)).fetchone()
    conn.close()
    if not row:
        return None
    result = dict(row)
    for key in ("source_tables", "source_columns", "result_design", "result_steps", "result_flow"):
        if result.get(key):
            result[key] = json.loads(result[key])
    return result


def update_generation_status(generation_id: int, status: str):
    def _do():
        conn = get_connection()
        try:
            conn.execute(
                "UPDATE generation_history SET status = ? WHERE id = ?",
                (status, generation_id),
            )
            conn.commit()
        finally:
            conn.close()
    _execute_with_retry(_do)


def update_generation_result(generation_id: int, result_design: dict, result_steps: list, result_flow: list = None):
    def _do():
        conn = get_connection()
        try:
            conn.execute(
                "UPDATE generation_history SET result_design = ?, result_steps = ?, result_flow = ? WHERE id = ?",
                (
                    json.dumps(result_design, ensure_ascii=False),
                    json.dumps(result_steps, ensure_ascii=False),
                    json.dumps(result_flow or [], ensure_ascii=False),
                    generation_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()
    _execute_with_retry(_do)


def get_project_history(project_id: int) -> list[dict]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT id, created_at, status, requirement FROM generation_history WHERE project_id = ? ORDER BY created_at DESC",
        (project_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# === フィードバック蓄積 ===

def save_feedback(
    generation_id: int,
    project_id: int,
    feedback_type: str,
    feedback_text: str,
    affected_step: int | None = None,
    original_content: str = "",
    corrected_content: str = "",
) -> int:
    def _do():
        conn = get_connection()
        try:
            _ensure_project_exists(conn, project_id)
            cursor = conn.execute(
                """INSERT INTO feedback_history
                (generation_id, project_id, feedback_type, feedback_text, affected_step, original_content, corrected_content)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (generation_id, project_id, feedback_type, feedback_text, affected_step, original_content, corrected_content),
            )
            fb_id = cursor.lastrowid
            conn.commit()
            return fb_id
        finally:
            conn.close()
    return _execute_with_retry(_do)


def get_relevant_feedback(table_names: list[str], requirement: str, limit: int = 10) -> list[dict]:
    """類似案件のフィードバックを検索する"""
    conn = get_connection()
    # 同じテーブル名を使った過去のフィードバックを検索
    placeholders = ",".join("?" * len(table_names))
    query = f"""
        SELECT f.*, g.requirement, g.source_tables
        FROM feedback_history f
        JOIN generation_history g ON f.generation_id = g.id
        WHERE g.source_tables LIKE ?
        ORDER BY f.created_at DESC
        LIMIT ?
    """
    # テーブル名のいずれかが含まれるものを検索
    all_feedback = []
    for table_name in table_names:
        rows = conn.execute(query, (f"%{table_name}%", limit)).fetchall()
        all_feedback.extend([dict(r) for r in rows])

    conn.close()
    # 重複除去
    seen = set()
    unique = []
    for fb in all_feedback:
        if fb["id"] not in seen:
            seen.add(fb["id"])
            unique.append(fb)
    return unique[:limit]


# === パターン学習 ===

def compute_table_structure_hash(columns: list[dict]) -> str:
    """テーブル構成のハッシュを計算（類似検索用）"""
    # テーブル名とカラム物理名のソート済みリストからハッシュを生成
    structure = sorted(
        f"{c.get('data_file_name', '')}:{c.get('effective_physical_name', '')}"
        for c in columns
        if not c.get("is_deleted")
    )
    return hashlib.md5("|".join(structure).encode()).hexdigest()


def save_pattern(
    pattern_name: str,
    description: str,
    trigger_keywords: list[str],
    table_structure_hash: str,
    bdash_steps: list,
    sql_template: str,
) -> int:
    conn = get_connection()
    cursor = conn.execute(
        """INSERT INTO learned_patterns
        (pattern_name, description, trigger_keywords, table_structure_hash, bdash_steps, sql_template)
        VALUES (?, ?, ?, ?, ?, ?)""",
        (
            pattern_name,
            description,
            json.dumps(trigger_keywords, ensure_ascii=False),
            table_structure_hash,
            json.dumps(bdash_steps, ensure_ascii=False),
            sql_template,
        ),
    )
    pattern_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return pattern_id


def find_similar_patterns(table_structure_hash: str = "", keywords: list[str] = None) -> list[dict]:
    """類似パターンを検索する"""
    conn = get_connection()
    results = []

    # ハッシュ完全一致
    if table_structure_hash:
        rows = conn.execute(
            "SELECT * FROM learned_patterns WHERE table_structure_hash = ?",
            (table_structure_hash,),
        ).fetchall()
        results.extend([dict(r) for r in rows])

    # キーワード部分一致
    if keywords:
        for kw in keywords:
            rows = conn.execute(
                "SELECT * FROM learned_patterns WHERE trigger_keywords LIKE ?",
                (f"%{kw}%",),
            ).fetchall()
            for row in rows:
                row_dict = dict(row)
                if row_dict["id"] not in {r["id"] for r in results}:
                    results.append(row_dict)

    conn.close()

    # JSON文字列をパース
    for r in results:
        if r.get("trigger_keywords"):
            r["trigger_keywords"] = json.loads(r["trigger_keywords"])
        if r.get("bdash_steps"):
            r["bdash_steps"] = json.loads(r["bdash_steps"])

    return results


# 初期化
init_db()
