"""CSV Exporter - データ設計・実装手順・検証観点をCSV出力"""

import csv
import io


def export_design_csv(result: dict) -> str:
    """データ設計結果をCSV文字列に変換"""
    output = io.StringIO()
    writer = csv.writer(output)

    # ヘッダー
    writer.writerow([
        "ソーステーブル", "カラム論理名", "カラム物理名",
        "推定型", "NULL許可", "備考",
    ])

    for col in result.get("data_design", []):
        writer.writerow([
            col.get("source_table", ""),
            col.get("logical_name", ""),
            col.get("physical_name", ""),
            col.get("estimated_type", ""),
            "NULLABLE" if col.get("nullable") else "NOT NULL",
            col.get("note", ""),
        ])

    return output.getvalue()


def export_steps_csv(result: dict) -> str:
    """実装手順＋検証観点をCSV文字列に変換"""
    output = io.StringIO()
    writer = csv.writer(output)

    # ヘッダー
    writer.writerow([
        "ステップ", "タイトル", "説明",
        "SQL", "b→dashタスク種別", "b→dashタスク名",
        "b→dash GUI操作パス", "b→dash設定",
        "検証: 確認内容", "検証: 検証SQL",
        "検証: 期待値", "検証: NG時対処",
        "検証: 顧客説明テンプレート",
    ])

    for step in result.get("implementation_steps", []):
        bdash = step.get("bdash_operation", {})
        validation = step.get("validation", {})

        writer.writerow([
            step.get("step_number", ""),
            step.get("title", ""),
            step.get("description", ""),
            step.get("sql", ""),
            bdash.get("task_type", ""),
            bdash.get("task_name", ""),
            bdash.get("gui_path", ""),
            " / ".join(bdash.get("settings", [])),
            validation.get("what_to_check", ""),
            validation.get("verification_sql", ""),
            validation.get("expected_result", ""),
            validation.get("ng_action", ""),
            validation.get("customer_explanation", ""),
        ])

    return output.getvalue()


def export_all_csv(result: dict) -> str:
    """全出力を1つのCSVにまとめる"""
    output = io.StringIO()
    writer = csv.writer(output)

    # セクション1: データ設計
    writer.writerow(["=== データ設計 ==="])
    writer.writerow([
        "ソーステーブル", "カラム論理名", "カラム物理名",
        "推定型", "NULL許可", "備考",
    ])
    for col in result.get("data_design", []):
        writer.writerow([
            col.get("source_table", ""),
            col.get("logical_name", ""),
            col.get("physical_name", ""),
            col.get("estimated_type", ""),
            "NULLABLE" if col.get("nullable") else "NOT NULL",
            col.get("note", ""),
        ])

    writer.writerow([])  # 空行

    # セクション2: 実装手順＋検証観点
    writer.writerow(["=== 実装手順＋検証観点 ==="])
    writer.writerow([
        "ステップ", "タイトル", "説明",
        "SQL", "b→dashタスク種別", "b→dashタスク名",
        "b→dash GUI操作パス", "b→dash設定",
        "検証: 確認内容", "検証: 検証SQL",
        "検証: 期待値", "検証: NG時対処",
        "検証: 顧客説明テンプレート",
    ])
    for step in result.get("implementation_steps", []):
        bdash = step.get("bdash_operation", {})
        validation = step.get("validation", {})
        writer.writerow([
            step.get("step_number", ""),
            step.get("title", ""),
            step.get("description", ""),
            step.get("sql", ""),
            bdash.get("task_type", ""),
            bdash.get("task_name", ""),
            bdash.get("gui_path", ""),
            " / ".join(bdash.get("settings", [])),
            validation.get("what_to_check", ""),
            validation.get("verification_sql", ""),
            validation.get("expected_result", ""),
            validation.get("ng_action", ""),
            validation.get("customer_explanation", ""),
        ])

    # フィードバック反映情報
    if result.get("feedback_applied"):
        writer.writerow([])
        writer.writerow(["=== 反映済みフィードバック ==="])
        for fb in result["feedback_applied"]:
            writer.writerow([fb])

    if result.get("similar_pattern_used"):
        writer.writerow([])
        writer.writerow(["=== 適用パターン ==="])
        writer.writerow([result["similar_pattern_used"]])

    return output.getvalue()
