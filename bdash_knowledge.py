"""b→dash ナレッジベース - データパレットの加工/統合タスク知識を管理"""

# 加工タスク（20種類）
PROCESSING_TASKS = [
    {
        "id": "concat",
        "name": "連結",
        "description": "カラム同士をつなぎ合わせてひとつのカラムにする",
        "sql_equivalent": "CONCAT(col1, col2)",
        "use_case": "姓と名を結合して氏名カラムを作成する場合など",
        "gui_path": "データパレット > 加工 > 連結",
        "params": ["対象カラム（複数選択）", "区切り文字（任意）"],
    },
    {
        "id": "text_insert",
        "name": "テキスト挿入",
        "description": "文頭/文末に任意テキストを挿入する",
        "sql_equivalent": "CONCAT('prefix', col) / CONCAT(col, 'suffix')",
        "use_case": "メールアドレスにドメインを付与する場合など",
        "gui_path": "データパレット > 加工 > テキスト挿入",
        "params": ["対象カラム", "挿入位置（文頭/文末）", "挿入テキスト"],
    },
    {
        "id": "split",
        "name": "分割",
        "description": "カラムを任意の箇所で分けて新しいカラムを作成する",
        "sql_equivalent": "SPLIT_PART(col, delimiter, part_number)",
        "use_case": "氏名を姓と名に分割する場合など",
        "gui_path": "データパレット > 加工 > 分割",
        "params": ["対象カラム", "分割位置/区切り文字"],
    },
    {
        "id": "arithmetic",
        "name": "四則演算",
        "description": "足し算/引き算/掛け算/割り算を行う",
        "sql_equivalent": "col1 + col2 / col1 - col2 / col1 * col2 / col1 / col2",
        "use_case": "税込価格の計算、利益率の算出など",
        "gui_path": "データパレット > 加工 > 四則演算",
        "params": ["対象カラム1", "演算子", "対象カラム2 or 固定値"],
    },
    {
        "id": "time_calc",
        "name": "時刻演算",
        "description": "日数や時間の差分を算出する",
        "sql_equivalent": "DATEDIFF('day', col1, col2)",
        "use_case": "登録日から現在までの経過日数、購買間隔の算出など",
        "gui_path": "データパレット > 加工 > 時刻演算",
        "params": ["対象カラム1", "対象カラム2", "差分の単位（日/時間/分）"],
    },
    {
        "id": "if_statement",
        "name": "IF文",
        "description": "条件指定で値を変換する",
        "sql_equivalent": "CASE WHEN condition THEN value1 ELSE value2 END",
        "use_case": "金額に応じてランク分け、フラグ値の変換など",
        "gui_path": "データパレット > 加工 > IF文",
        "params": ["条件カラム", "条件（=, >, <, >=, <=, !=, LIKE）", "条件値", "真の値", "偽の値"],
    },
    {
        "id": "add_column",
        "name": "追加",
        "description": "新しいカラムを追加作成する",
        "sql_equivalent": "SELECT *, 'fixed_value' AS new_col",
        "use_case": "固定値のフラグカラム追加、処理日付カラム追加など",
        "gui_path": "データパレット > 加工 > 追加",
        "params": ["新カラム名", "値（固定値 or 関数）"],
    },
    {
        "id": "duplicate",
        "name": "複製",
        "description": "カラムをコピーする",
        "sql_equivalent": "SELECT *, col AS col_copy",
        "use_case": "元カラムを保持しつつ加工用カラムを作る場合など",
        "gui_path": "データパレット > 加工 > 複製",
        "params": ["対象カラム"],
    },
    {
        "id": "delete_column",
        "name": "削除",
        "description": "カラムを消去する",
        "sql_equivalent": "SELECT以外の方法でカラムを除外",
        "use_case": "不要なカラムの除外、個人情報カラムの除去など",
        "gui_path": "データパレット > 加工 > 削除",
        "params": ["対象カラム"],
    },
    {
        "id": "ranking",
        "name": "ランキング",
        "description": "順位付けを行う",
        "sql_equivalent": "ROW_NUMBER() OVER (PARTITION BY group_col ORDER BY sort_col)",
        "use_case": "売上順位、顧客ランキングなど",
        "gui_path": "データパレット > 加工 > ランキング",
        "params": ["ランキング対象カラム", "グルーピングカラム（任意）", "ソート順（昇順/降順）"],
    },
    {
        "id": "aggregate",
        "name": "集約",
        "description": "グルーピングして合計/平均などを算出する",
        "sql_equivalent": "SELECT group_col, SUM(val) FROM table GROUP BY group_col",
        "use_case": "月次売上集計、顧客ごとの購買回数集計など",
        "gui_path": "データパレット > 加工 > 集約",
        "params": ["グルーピングカラム", "集約対象カラム", "集約関数（合計/平均/最大/最小/件数）"],
    },
    {
        "id": "replace",
        "name": "置換",
        "description": "カラムの値を変換する",
        "sql_equivalent": "REPLACE(col, 'old', 'new')",
        "use_case": "コード値をラベルに変換、特殊文字の除去など",
        "gui_path": "データパレット > 加工 > 置換",
        "params": ["対象カラム", "変換前の値", "変換後の値"],
    },
    {
        "id": "type_convert",
        "name": "型変換",
        "description": "データ型を変換する",
        "sql_equivalent": "CAST(col AS target_type)",
        "use_case": "文字列を数値に変換、日付型への変換など",
        "gui_path": "データパレット > 加工 > 型変換",
        "params": ["対象カラム", "変換先の型"],
    },
    {
        "id": "extract",
        "name": "抽出",
        "description": "特定の値を抜き出す",
        "sql_equivalent": "SUBSTR(col, start, length)",
        "use_case": "電話番号の市外局番抽出、日付から年月のみ抽出など",
        "gui_path": "データパレット > 加工 > 抽出",
        "params": ["対象カラム", "抽出開始位置", "抽出文字数"],
    },
    {
        "id": "exclude",
        "name": "除外",
        "description": "特定の値を削除する",
        "sql_equivalent": "REPLACE(col, 'target', '')",
        "use_case": "ハイフンやスペースの除去など",
        "gui_path": "データパレット > 加工 > 除外",
        "params": ["対象カラム", "除外する値"],
    },
    {
        "id": "format_convert",
        "name": "書式変換",
        "description": "全角/半角、大文字/小文字の変換を行う",
        "sql_equivalent": "UPPER(col) / LOWER(col)",
        "use_case": "メールアドレスの小文字統一、カタカナの全角統一など",
        "gui_path": "データパレット > 加工 > 書式変換",
        "params": ["対象カラム", "変換種類（全角→半角/半角→全角/大文字→小文字/小文字→大文字）"],
    },
    {
        "id": "zero_padding",
        "name": "0埋め",
        "description": "先頭に0を挿入する",
        "sql_equivalent": "LPAD(col, length, '0')",
        "use_case": "顧客コードの桁合わせ（001, 002...）など",
        "gui_path": "データパレット > 加工 > 0埋め",
        "params": ["対象カラム", "桁数"],
    },
    {
        "id": "filter",
        "name": "絞り込み",
        "description": "条件でフィルタリングする",
        "sql_equivalent": "WHERE condition",
        "use_case": "アクティブユーザーのみ抽出、特定期間のデータのみ取得など",
        "gui_path": "データパレット > 加工 > 絞り込み",
        "params": ["条件カラム", "条件（=, >, <, LIKE, IS NULL等）", "条件値"],
    },
    {
        "id": "dedup",
        "name": "名寄せ",
        "description": "重複レコードをひとつにまとめる",
        "sql_equivalent": "SELECT DISTINCT / GROUP BY + FIRST_VALUE",
        "use_case": "重複顧客データの統合、メールアドレスベースの名寄せなど",
        "gui_path": "データパレット > 加工 > 名寄せ",
        "params": ["名寄せキーカラム", "残すレコードの条件"],
    },
    {
        "id": "reference",
        "name": "参照",
        "description": "グループ内でソートし特定の値を表示する",
        "sql_equivalent": "FIRST_VALUE(col) OVER (PARTITION BY group ORDER BY sort)",
        "use_case": "顧客ごとの最新購買日取得、最大購買金額の取得など",
        "gui_path": "データパレット > 加工 > 参照",
        "params": ["参照カラム", "グルーピングカラム", "ソートカラム", "ソート順"],
    },
]

# 統合タスク
INTEGRATION_TASKS = [
    {
        "id": "horizontal_merge",
        "name": "横統合",
        "description": "共通キーで複数データファイルを結合する（SQL JOIN相当）",
        "sql_equivalent": "SELECT * FROM A JOIN B ON A.key = B.key",
        "use_case": "顧客マスタと購買履歴の結合、商品マスタとの紐づけなど",
        "gui_path": "データパレット > 統合 > 横統合",
        "params": ["結合元データファイル", "結合先データファイル", "結合キー", "結合方法（内部結合/左外部結合/右外部結合/完全外部結合）"],
        "join_types": {
            "inner": "内部結合 - 両方に存在するレコードのみ",
            "left": "左外部結合 - 左側のレコードをすべて残す",
            "right": "右外部結合 - 右側のレコードをすべて残す",
            "full": "完全外部結合 - 両方のレコードをすべて残す",
        },
    },
    {
        "id": "vertical_merge",
        "name": "縦統合",
        "description": "同じカラム構成のデータを縦に連結する（SQL UNION相当）",
        "sql_equivalent": "SELECT * FROM A UNION ALL SELECT * FROM B",
        "use_case": "月別データの統合、複数店舗データの結合など",
        "gui_path": "データパレット > 統合 > 縦統合",
        "params": ["統合元データファイル", "統合先データファイル"],
    },
]


def get_all_tasks() -> dict:
    """全タスク情報を返す"""
    return {
        "processing_tasks": PROCESSING_TASKS,
        "integration_tasks": INTEGRATION_TASKS,
    }


def get_task_by_id(task_id: str) -> dict | None:
    """タスクIDからタスク情報を取得"""
    for task in PROCESSING_TASKS + INTEGRATION_TASKS:
        if task["id"] == task_id:
            return task
    return None


def build_knowledge_prompt() -> str:
    """エージェントのプロンプトに埋め込む用のナレッジ文字列を生成"""
    lines = []
    lines.append("【b→dashデータパレット: 加工タスク（20種類）】")
    for i, task in enumerate(PROCESSING_TASKS, 1):
        lines.append(f"{i}. {task['name']} - {task['description']}")
        lines.append(f"   SQL相当: {task['sql_equivalent']}")
        lines.append(f"   GUI操作: {task['gui_path']}")
        lines.append(f"   パラメータ: {', '.join(task['params'])}")
        lines.append(f"   用途例: {task['use_case']}")
        lines.append("")

    lines.append("【b→dashデータパレット: 統合タスク】")
    for task in INTEGRATION_TASKS:
        lines.append(f"- {task['name']} - {task['description']}")
        lines.append(f"  SQL相当: {task['sql_equivalent']}")
        lines.append(f"  GUI操作: {task['gui_path']}")
        lines.append(f"  パラメータ: {', '.join(task['params'])}")
        lines.append(f"  用途例: {task['use_case']}")
        if "join_types" in task:
            for jtype, jdesc in task["join_types"].items():
                lines.append(f"    - {jtype}: {jdesc}")
        lines.append("")

    return "\n".join(lines)


def suggest_tasks_for_requirement(requirement: str) -> list[dict]:
    """要件テキストからおすすめのタスクを提案する（キーワードマッチング）"""
    keyword_map = {
        "結合": ["horizontal_merge"],
        "JOIN": ["horizontal_merge"],
        "マージ": ["horizontal_merge"],
        "統合": ["horizontal_merge", "vertical_merge"],
        "UNION": ["vertical_merge"],
        "集計": ["aggregate"],
        "合計": ["aggregate"],
        "平均": ["aggregate"],
        "グルーピング": ["aggregate"],
        "GROUP BY": ["aggregate"],
        "変換": ["replace", "type_convert", "format_convert"],
        "置換": ["replace"],
        "フィルタ": ["filter"],
        "絞り込み": ["filter"],
        "WHERE": ["filter"],
        "ランキング": ["ranking"],
        "順位": ["ranking"],
        "IF": ["if_statement"],
        "条件": ["if_statement", "filter"],
        "名寄せ": ["dedup"],
        "重複": ["dedup"],
        "連結": ["concat"],
        "分割": ["split"],
        "日数": ["time_calc"],
        "日付": ["time_calc", "extract"],
        "計算": ["arithmetic"],
        "0埋め": ["zero_padding"],
    }

    suggested_ids = set()
    req_upper = requirement.upper()
    for keyword, task_ids in keyword_map.items():
        if keyword.upper() in req_upper:
            suggested_ids.update(task_ids)

    return [get_task_by_id(tid) for tid in suggested_ids if get_task_by_id(tid)]
