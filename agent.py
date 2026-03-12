"""データ設計エージェント - Claude APIを使ってデータ設計・実装手順・検証観点を生成"""

import json
import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(str(Path(__file__).parent / ".env"), override=True)

from anthropic import Anthropic

from bdash_knowledge import build_knowledge_prompt, suggest_tasks_for_requirement
from knowledge_db import (
    get_relevant_feedback,
    find_similar_patterns,
    compute_table_structure_hash,
    save_generation,
    save_feedback,
    update_generation_result,
    update_generation_status,
    get_generation,
)

client = Anthropic()

MODEL = "claude-sonnet-4-5-20250929"

# モックモード（APIクレジットがない場合にUIテスト用）
MOCK_MODE = os.environ.get("MOCK_MODE", "").lower() in ("1", "true", "yes")


def _generate_mock_result(requirement: str, selected_tables: list[str], columns: list[dict]) -> dict:
    """APIを呼ばずにモックデータを返す"""
    # JOINキー検出
    join_keys = [c for c in columns if c.get("customer_id_flag") == 1 and not c.get("is_deleted")]
    join_key_name = join_keys[0]["effective_physical_name"] if join_keys else "id"

    design = []
    for c in columns:
        if c["data_file_name"] in selected_tables and not c.get("is_deleted"):
            note = ""
            if c.get("customer_id_flag") == 1:
                note = "JOINキー, 顧客IDフラグ=1"
            design.append({
                "source_table": c["data_file_name"],
                "logical_name": c["logical_name"],
                "physical_name": c["effective_physical_name"],
                "estimated_type": "VARCHAR(255)",
                "nullable": False,
                "note": note,
            })

    table_b = selected_tables[1] if len(selected_tables) > 1 else "table_b"

    data_flow = [
        {"from": [selected_tables[0], table_b], "operation": f"横統合 - {join_key_name}で内部結合", "to": "結合済みデータ"},
        {"from": ["結合済みデータ"], "operation": f"集約 - {join_key_name}でグルーピング、購買金額を合計", "to": "顧客別月次集計"},
    ]

    steps = [
        {
            "step_number": 1,
            "title": f"横統合: {' と '.join(selected_tables)} を結合",
            "description": f"顧客IDフラグ=1の {join_key_name} をキーとして横統合（内部結合）を実行します。",
            "bdash_operation": {
                "task_type": "統合",
                "task_name": "横統合",
                "gui_steps": [
                    f"「{selected_tables[0]}」データファイルを開く",
                    "データパレットの「統合」メニューから「横統合」をクリック",
                    f"結合元に「{selected_tables[0]}」、結合先に「{table_b}」を選択",
                    f"結合キーに両テーブルの「{join_key_name}」を選択",
                    "結合方法を「内部結合」に設定",
                    "「実行」をクリック",
                ],
            },
            "validation": {
                "what_to_check": f"JOINキー({join_key_name})のNULL値とレコード件数の妥当性を確認",
                "how_to_check": f"横統合の結果データを開き、データパレットの「絞り込み」で{join_key_name}がNULLのレコードを検索。件数が0件であることを確認。また、結合前後のデータファイルの件数を比較する。",
                "expected_result": "NULL件数が0件であること。結合後の件数がソーステーブルの件数以下であること。",
                "ng_action": f"NULLが含まれる場合は、事前に「絞り込み」タスクで {join_key_name} がNULLでないレコードのみに絞り込んでください。",
                "customer_explanation": f"この手順では{' と '.join(selected_tables)}を{join_key_name}で結合しています。結合キーにNULLがないこと、結合後の件数が妥当であることをご確認ください。",
            },
        },
        {
            "step_number": 2,
            "title": "集約: 月次の購買金額を顧客ごとに集計",
            "description": "購買日時を月単位でグルーピングし、顧客ごとの購買金額合計を算出します。",
            "bdash_operation": {
                "task_type": "加工",
                "task_name": "集約",
                "gui_steps": [
                    "横統合で作成した結合済みデータファイルを開く",
                    "データパレットの「加工」メニューから「集約」をクリック",
                    f"グルーピングキーに「{join_key_name}」を選択",
                    "集約設定: 「order_amount」カラムを選択 → 集約方法「合計」 → 出力名「total_amount」",
                    "集約設定（追加）: 「order_id」カラムを選択 → 集約方法「件数」 → 出力名「order_count」",
                    "「実行」をクリック",
                ],
            },
            "validation": {
                "what_to_check": "集計結果の合計金額がソースデータと一致するか確認",
                "how_to_check": "集約前のデータファイルを開き、order_amountカラムの合計値を確認（データパレットの「集約」で全件合計を取る）。集約後のデータファイルでtotal_amountの合計値と比較する。",
                "expected_result": "集約前後で合計金額が一致すること。",
                "ng_action": "差分がある場合はグルーピングキーの設定を見直し、NULL値が除外されていないか確認してください。",
                "customer_explanation": "この手順では月次・顧客別に購買金額を集計しています。集計前後で合計金額が一致することを確認してください。一致していればデータの欠損はありません。",
            },
        },
    ]

    return {
        "data_design": design,
        "data_flow": data_flow,
        "implementation_steps": steps,
        "feedback_applied": [],
        "similar_pattern_used": None,
    }


def _parse_json_response(text: str) -> dict:
    """Claude APIのレスポンスからJSONを安全にパースする。絶対に例外を投げない。"""
    fallback = {
        "data_design": [],
        "data_flow": [],
        "implementation_steps": [],
        "error": "JSONのパースに失敗しました",
        "raw_response": (text or "")[:2000],
    }
    try:
        text = (text or "").strip()
        if not text:
            return fallback

        # マークダウンコードブロック除去
        if text.startswith("```"):
            lines = text.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            text = "\n".join(lines).strip()

        # Stage 1: そのままパース
        try:
            return json.loads(text)
        except (json.JSONDecodeError, ValueError):
            pass

        # Stage 2: JSON部分を抽出して再パース
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end])
            except (json.JSONDecodeError, ValueError):
                pass

        # Stage 3: 制御文字を除去して再パース
        if start >= 0 and end > start:
            cleaned = text[start:end]
            # 文字列値内の生の改行・タブを除去（regexを使わない安全な方法）
            in_string = False
            escape_next = False
            result_chars = []
            for ch in cleaned:
                if escape_next:
                    result_chars.append(ch)
                    escape_next = False
                    continue
                if ch == '\\' and in_string:
                    result_chars.append(ch)
                    escape_next = True
                    continue
                if ch == '"':
                    in_string = not in_string
                    result_chars.append(ch)
                    continue
                if in_string and ch == '\n':
                    result_chars.append('\\n')
                    continue
                if in_string and ch == '\t':
                    result_chars.append('\\t')
                    continue
                if in_string and ch == '\r':
                    continue
                result_chars.append(ch)
            try:
                return json.loads("".join(result_chars))
            except (json.JSONDecodeError, ValueError):
                pass

        # Stage 4: 最終手段 - 制御文字を全部削除
        if start >= 0 and end > start:
            import re
            cleaned = text[start:end]
            cleaned = re.sub(r'[\x00-\x1f\x7f]', ' ', cleaned)
            try:
                return json.loads(cleaned)
            except (json.JSONDecodeError, ValueError):
                pass

        fallback["raw_response"] = text[:2000]
        return fallback
    except Exception:
        # 何が起きても絶対にフォールバックを返す
        return fallback


def _repair_truncated_json(text: str) -> str:
    """途中で切れたJSONを修復する（閉じ括弧を補完）"""
    text = text.strip()
    # マークダウンコードブロック除去
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()

    start = text.find("{")
    if start < 0:
        return text

    text = text[start:]

    # 文字列内かどうかを追跡しつつ、開き括弧をカウント
    in_string = False
    escape_next = False
    stack = []  # '{' or '['

    for ch in text:
        if escape_next:
            escape_next = False
            continue
        if ch == '\\' and in_string:
            escape_next = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == '{':
            stack.append('}')
        elif ch == '[':
            stack.append(']')
        elif ch in ('}', ']'):
            if stack:
                stack.pop()

    # 文字列が開いたままの場合、閉じる
    if in_string:
        text += '"'

    # 閉じ括弧を補完
    while stack:
        text += stack.pop()

    return text


def _build_system_prompt(
    columns: list[dict],
    selected_tables: list[str],
    past_feedback: list[dict],
    similar_patterns: list[dict],
) -> str:
    """エージェント用システムプロンプトを構築"""

    # 選択されたテーブルのカラムのみフィルタ
    active_columns = [
        c for c in columns
        if c["data_file_name"] in selected_tables and not c["is_deleted"]
    ]

    # カラム情報をテーブルごとに整理
    table_info = {}
    for col in active_columns:
        tname = col["data_file_name"]
        if tname not in table_info:
            table_info[tname] = []
        flag = " [顧客IDフラグ=1, JOINキー候補]" if col["customer_id_flag"] == 1 else ""
        custom = f" (カスタム物理名: {col['custom_physical_name']})" if col.get("custom_physical_name") else ""
        table_info[tname].append(
            f"  - {col['logical_name']} / {col['effective_physical_name']}{custom}{flag}"
        )

    columns_text = ""
    for tname, cols in table_info.items():
        columns_text += f"\n【{tname}】\n" + "\n".join(cols) + "\n"

    # b→dashナレッジ
    bdash_knowledge = build_knowledge_prompt()

    # 過去のフィードバック
    feedback_text = ""
    if past_feedback:
        feedback_text = "\n【過去のフィードバック（同様の構築で指摘された内容）】\n"
        for fb in past_feedback:
            feedback_text += f"- [{fb['feedback_type']}] {fb['feedback_text']}\n"
        feedback_text += "※上記の指摘を踏まえ、事前に改善を反映してください。\n"

    # 類似パターン
    pattern_text = ""
    if similar_patterns:
        pattern_text = "\n【類似案件のパターン（手順統一のため参照）】\n"
        for p in similar_patterns:
            pattern_text += f"- パターン名: {p['pattern_name']}\n  説明: {p['description']}\n"
            if p.get("bdash_steps"):
                steps = p["bdash_steps"] if isinstance(p["bdash_steps"], list) else json.loads(p["bdash_steps"])
                for s in steps[:3]:
                    pattern_text += f"  - ステップ: {json.dumps(s, ensure_ascii=False)}\n"
        pattern_text += "※類似案件と手順をなるべく合わせてください。\n"

    return f"""あなたはb→dashのデータパレットに精通したデータ設計アシスタントです。
b→dashのCSが顧客に説明するための手順書を生成します。

{bdash_knowledge}

【入力されたカラム定義】
{columns_text}

{feedback_text}
{pattern_text}

【出力形式 - 必ず以下のJSON形式で出力してください】
出力は必ず1つのJSONオブジェクトのみを返してください。マークダウンのコードブロックは使わないでください。

{{
  "data_design": [
    {{
      "source_table": "テーブル名",
      "logical_name": "論理名",
      "physical_name": "物理名",
      "estimated_type": "推定型（Snowflake型）",
      "nullable": true/false,
      "note": "備考（PK, FK, JOINキーなど）"
    }}
  ],
  "data_flow": [
    {{
      "from": ["入力データ名（テーブル名や中間データ名）"],
      "operation": "操作内容（例: 集約 - customer_idでグルーピング、金額を合計）",
      "to": "出力データ名（中間データ名や最終データ名）"
    }}
  ],
  "implementation_steps": [
    {{
      "step_number": 1,
      "title": "ステップのタイトル（例: 購買履歴を顧客ごとに集約）",
      "description": "このステップで何をするかの概要説明",
      "bdash_operation": {{
        "task_type": "加工 or 統合",
        "task_name": "タスク名（集約、横統合など）",
        "gui_steps": [
          "「購買履歴」データファイルを開く",
          "データパレットの「加工」メニューから「集約」をクリック",
          "グルーピングキーに「customer_id」を選択",
          "集約設定: 「order_amount」カラムを選択し、集約方法を「合計」に設定 → 出力カラム名を「total_order_amount」にする",
          "集約設定（追加）: 「order_amount」カラムを選択し、集約方法を「件数」に設定 → 出力カラム名を「order_count」にする",
          "「実行」をクリック"
        ]
      }},
      "validation": {{
        "what_to_check": "何を確認するか",
        "how_to_check": "b→dashの画面上での確認手順",
        "expected_result": "期待値（例: 結果が0件であればOK）",
        "ng_action": "NGだった場合の対処方法",
        "customer_explanation": "顧客への説明文テンプレート"
      }}
    }}
  ],
  "feedback_applied": ["過去のフィードバックから事前に反映した内容のリスト"],
  "similar_pattern_used": "適用した類似パターン名（なければnull）"
}}

【data_flowの書き方】
data_flowは、データがどのように加工・結合されて最終的なアウトプットになるかを示すフロー図です。
- from: 入力となるデータ（複数可）。元テーブル名または前のステップで作られた中間データ名
- operation: どんな操作をするか（b→dashのタスク名 + 具体的な内容）
- to: 操作の結果できるデータの名前
- implementation_stepsの順序と対応させてください

例:
[
  {{"from": ["購買履歴"], "operation": "集約 - customer_idでグルーピング、購買金額を合計", "to": "顧客別購買集計"}},
  {{"from": ["顧客マスタ", "顧客別購買集計"], "operation": "横統合 - customer_idで内部結合", "to": "顧客分析データ"}}
]

【bdash_operation.gui_stepsの書き方 - 最重要】
gui_stepsは、CSがb→dashの画面を見ながらそのまま操作できるレベルで書いてください。
具体的には以下のルールに従ってください:

1. 最初に「どのデータファイルを開くか」を明記する
   例: 「「購買履歴」データファイルを開く」
2. 次に「どのメニューのどのタスクを選ぶか」を明記する
   例: 「データパレットの「加工」メニューから「集約」をクリック」
3. 設定項目を一つずつ具体的に書く（カラム名は実際の物理名を使う）
   例: 「グルーピングキーに「customer_id」を選択」
   例: 「集約設定: 「order_amount」カラムを選択 → 集約方法「合計」 → 出力名「total_order_amount」」
4. 横統合の場合は結合元/結合先/キー/結合方法を具体的に
   例: 「結合元に「顧客マスタ」、結合先に「購買履歴」を選択」
   例: 「結合キーに両テーブルの「customer_id」を選択」
   例: 「結合方法を「左外部結合」に設定」
5. 最後に「実行をクリック」

悪い例（抽象的すぎる）:
  "gui_path": "データパレット > 加工 > 集約",
  "settings": ["グルーピングカラム: customer_id"]

良い例（具体的な操作手順）:
  "gui_steps": [
    "「購買履歴」データファイルを開く",
    "データパレットの「加工」メニューから「集約」をクリック",
    "グルーピングキーに「customer_id」を選択",
    "集約設定: 「order_amount」カラム → 「合計」 → 出力名「total_order_amount」",
    "「実行」をクリック"
  ]

【重要ルール】
- 顧客IDフラグ=1のカラムはJOINキーとして使用してください
- カスタムカラム物理名がある場合はそちらを優先してください
- 削除済みカラムは除外済みです
- SQLは出力しないでください。b→dashのGUI操作手順のみで記載してください
- 検証観点は、CSが顧客に「ここを検証してください」と説明できるレベルで記載してください
- 検証方法（how_to_check）はb→dashの画面上での確認手順を書いてください（SQLではなくGUI操作で）
- data_flowは必ず出力してください。データがどう変換されるかのフローを示してください
- 【JSON完全性最優先】必ず有効なJSONとして完結させてください。レスポンスが長くなりそうな場合は、gui_stepsやvalidationの文言を簡潔にしてでもJSON構造を閉じてください
- data_designにはソーステーブルと最終出力データのカラムのみ記載し、中間データは省略可能です
- 過去のフィードバックがある場合は事前に反映し、feedback_appliedに明記してください
- 類似案件のパターンがある場合はなるべく手順を合わせてください"""


def generate_proposal(
    project_id: int,
    requirement: str,
    columns: list[dict],
    selected_tables: list[str],
) -> dict:
    """提案を生成する（フェーズ1）"""

    # モックモード
    if MOCK_MODE:
        result = _generate_mock_result(requirement, selected_tables, columns)
        gen_id = save_generation(
            project_id=project_id,
            requirement=requirement,
            source_tables=selected_tables,
            source_columns=columns,
            result_design=result.get("data_design", []),
            result_steps=result.get("implementation_steps", []),
            result_flow=result.get("data_flow", []),
            status="draft",
        )
        result["generation_id"] = gen_id
        result["status"] = "draft"
        return result

    # テーブル構成ハッシュ計算
    table_hash = compute_table_structure_hash(columns)

    # 過去のフィードバック検索
    past_feedback = get_relevant_feedback(selected_tables, requirement)

    # 類似パターン検索
    keywords = requirement.replace("、", " ").replace("。", " ").split()
    similar_patterns = find_similar_patterns(table_hash, keywords)

    # システムプロンプト構築
    system_prompt = _build_system_prompt(columns, selected_tables, past_feedback, similar_patterns)

    # Claude APIに問い合わせ（streaming使用: 長時間リクエスト対応）
    import sys
    with client.messages.stream(
        model=MODEL,
        max_tokens=16000,
        system=system_prompt,
        messages=[
            {
                "role": "user",
                "content": f"以下の要件に基づいて、データ設計と実装手順（b→dash GUI操作手順付き）と検証観点を生成してください。\n\n【要件】\n{requirement}\n\n【対象テーブル】\n{', '.join(selected_tables)}",
            }
        ],
    ) as stream:
        response = stream.get_final_message()

    result_text = response.content[0].text
    stop_reason = response.stop_reason

    print(f"[INFO] Claude API stop_reason={stop_reason}, response_length={len(result_text)}", file=sys.stderr)

    # truncated対策: stop_reason が "max_tokens" ならJSONを閉じる修復を試みる
    if stop_reason == "max_tokens":
        print(f"[WARN] レスポンスが途中で切れました（max_tokens到達）", file=sys.stderr)
        result_text = _repair_truncated_json(result_text)

    # JSONパース（絶対に例外を投げない）
    result = _parse_json_response(result_text)

    # パース失敗時もエラーとして返さずDB保存して返す（UIで空表示になる）
    if result.get("error"):
        print(f"[WARN] JSONパース失敗: {result.get('error')}", file=sys.stderr)
        print(f"[WARN] raw_response先頭500文字: {result.get('raw_response', '')[:500]}", file=sys.stderr)

    # DBに保存（draft状態）
    gen_id = save_generation(
        project_id=project_id,
        requirement=requirement,
        source_tables=selected_tables,
        source_columns=columns,
        result_design=result.get("data_design", []),
        result_steps=result.get("implementation_steps", []),
        result_flow=result.get("data_flow", []),
        status="draft",
    )

    result["generation_id"] = gen_id
    result["status"] = "draft"
    return result


def apply_feedback(
    generation_id: int,
    project_id: int,
    feedback_text: str,
    columns: list[dict],
    selected_tables: list[str],
) -> dict:
    """修正指示を受けて提案を更新する（フェーズ2）"""

    gen = get_generation(generation_id)
    if not gen:
        return {"error": "生成結果が見つかりません"}

    # フィードバックをDBに保存
    save_feedback(
        generation_id=generation_id,
        project_id=project_id,
        feedback_type="correction",
        feedback_text=feedback_text,
    )

    # モックモード
    if MOCK_MODE:
        result = {
            "data_design": gen["result_design"],
            "data_flow": gen.get("result_flow", []),
            "implementation_steps": gen["result_steps"],
            "feedback_applied": [f"修正反映: {feedback_text}"],
        }
        update_generation_result(generation_id, result["data_design"], result["implementation_steps"], result.get("data_flow", []))
        result["generation_id"] = generation_id
        result["status"] = "draft"
        return result

    # 現在の結果 + 修正指示でClaude APIに再問い合わせ
    system_prompt = _build_system_prompt(columns, selected_tables, [], [])

    current_result = {
        "data_design": gen["result_design"],
        "implementation_steps": gen["result_steps"],
    }

    with client.messages.stream(
        model=MODEL,
        max_tokens=16000,
        system=system_prompt,
        messages=[
            {
                "role": "user",
                "content": f"以下の提案に対して修正指示がありました。修正を反映した新しい提案を同じJSON形式で出力してください。\nSQLは出力せず、b→dashのGUI操作手順のみで記載してください。data_flowも必ず含めてください。\n\n【現在の提案】\n{json.dumps(current_result, ensure_ascii=False, indent=2)}\n\n【修正指示】\n{feedback_text}",
            }
        ],
    ) as stream:
        response = stream.get_final_message()

    result_text = response.content[0].text
    stop_reason = response.stop_reason
    if stop_reason == "max_tokens":
        result_text = _repair_truncated_json(result_text)
    result = _parse_json_response(result_text)
    if result.get("error"):
        return {"error": "修正結果のパースに失敗しました", "raw_response": result.get("raw_response", "")}

    # DBの生成結果を更新
    update_generation_result(
        generation_id,
        result.get("data_design", []),
        result.get("implementation_steps", []),
        result.get("data_flow", []),
    )

    result["generation_id"] = generation_id
    result["status"] = "draft"
    return result


def confirm_generation(generation_id: int) -> dict:
    """提案を確定する"""
    update_generation_status(generation_id, "confirmed")
    gen = get_generation(generation_id)
    if not gen:
        return None
    # フロントが期待するキー名に統一
    return {
        "generation_id": generation_id,
        "status": "confirmed",
        "data_design": gen.get("result_design", []),
        "data_flow": gen.get("result_flow", []),
        "implementation_steps": gen.get("result_steps", []),
        "feedback_applied": [],
    }
