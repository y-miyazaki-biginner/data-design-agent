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


# b→dash ワークフローパターン（標準的な処理の流れ）
WORKFLOW_PATTERNS = {
    "standard_flow": "データパレット（加工・統合） → セグメント → コンテンツ → 配信設定",
    "data_palette_flow": "データ取り込み → 加工（型変換・IF文・集約等） → 統合（横統合・縦統合） → 出力データ",
}

# サポートページベースのユースケースパターン
USE_CASE_PATTERNS = [
    {
        "id": "cart_reminder",
        "name": "カゴ落ちリマインド",
        "description": "ECサイトでカートに入れたが購入していない顧客にリマインドメールを送る",
        "data_sources": ["顧客データ", "注文データ", "商品データ", "Webアクセスログ"],
        "steps": [
            "Webアクセスログからカート追加イベントを抽出（絞り込み: event_type = 'add_to_cart'）",
            "注文データと横統合（左外部結合: customer_id）して未購入を特定",
            "IF文で「カート追加あり & 注文なし」のフラグを作成",
            "商品データと横統合（product_id）して商品名・画像URLを取得",
            "顧客データと横統合（customer_id）してメールアドレスを取得",
            "セグメントでフラグ=1の顧客を抽出 → コンテンツ作成 → メール配信",
        ],
        "key_operations": ["絞り込み", "横統合", "IF文", "時刻演算"],
    },
    {
        "id": "rank_monthly_purchasers",
        "name": "ランク・月ごとの購買者数集計",
        "description": "会員ランク別・月別に購買者数を集計してレポートを作成する",
        "data_sources": ["顧客データ", "注文データ", "会員ランクマスタ"],
        "steps": [
            "注文データから注文月を抽出（抽出: order_dateから年月部分を取得）",
            "顧客データと横統合（customer_id）して会員ランクを取得",
            "会員ランクマスタと横統合（rank_id）してランク名を取得",
            "集約: ランク名 + 注文月でグルーピングし、customer_idの件数（ユニーク）を集計",
        ],
        "key_operations": ["抽出", "横統合", "集約"],
    },
    {
        "id": "form_response_analysis",
        "name": "アンケート回答分析",
        "description": "フォーム・アンケートの回答データを顧客情報と紐づけて分析する",
        "data_sources": ["アンケート回答データ", "顧客データ"],
        "steps": [
            "アンケート回答データと顧客データを横統合（email = email_address）",
            "回答内容に応じてIF文でセグメント分類フラグを作成",
            "集約: 回答選択肢ごとに件数を集計",
        ],
        "key_operations": ["横統合", "IF文", "集約"],
    },
    {
        "id": "line_exclusion",
        "name": "LINE除外リスト作成",
        "description": "LINE連携済みの顧客をメール配信対象から除外する",
        "data_sources": ["顧客データ", "LINE連携データ"],
        "steps": [
            "顧客データとLINE連携データを横統合（customer_id、左外部結合）",
            "IF文で「LINE連携あり（id_linked_flag=1）かつブロックなし（block_flag=0）」のフラグ作成",
            "絞り込みでLINE連携フラグ=0の顧客のみ抽出（メール配信対象）",
        ],
        "key_operations": ["横統合", "IF文", "絞り込み"],
    },
    {
        "id": "incomplete_registration",
        "name": "未本登録リマインドメール",
        "description": "仮登録のまま本登録が完了していない顧客にリマインドを送る",
        "data_sources": ["顧客データ"],
        "steps": [
            "IF文で「provisional_registration_date IS NOT NULL かつ full_registration_date IS NULL」のフラグ作成",
            "時刻演算で仮登録からの経過日数を算出",
            "絞り込みで経過日数が指定範囲（例: 3〜7日）の顧客を抽出",
            "セグメント → コンテンツ（本登録URL付き） → メール配信",
        ],
        "key_operations": ["IF文", "時刻演算", "絞り込み"],
    },
    {
        "id": "two_month_notification",
        "name": "2か月無アクション通知",
        "description": "直近2か月間購買やアクセスがない顧客を特定して通知する",
        "data_sources": ["顧客データ", "注文データ", "Webアクセスログ"],
        "steps": [
            "注文データを集約: customer_idでグルーピングし最終注文日（MAX）を取得",
            "Webアクセスログを集約: customer_idでグルーピングし最終アクセス日（MAX）を取得",
            "顧客データと横統合（customer_id）",
            "時刻演算で最終注文日・最終アクセス日からの経過日数を算出",
            "IF文で「両方60日以上」のフラグを作成",
            "絞り込みでフラグ=1の顧客を抽出",
        ],
        "key_operations": ["集約", "横統合", "時刻演算", "IF文", "絞り込み"],
    },
    {
        "id": "favorite_reminder",
        "name": "お気に入りリマインド",
        "description": "お気に入り登録した商品の価格変動や在庫復活を通知する",
        "data_sources": ["顧客データ", "商品データ", "Webアクセスログ", "Webコンバージョンデータ"],
        "steps": [
            "Webコンバージョンデータから「お気に入り登録」イベントを絞り込み",
            "商品データと横統合（product_id）して最新価格・在庫状況を取得",
            "IF文で「値下げあり」や「在庫復活」のフラグ作成（current_price < regular_price 等）",
            "顧客データと横統合（customer_id）してメールアドレス取得",
            "セグメント → パーソナライズコンテンツ → 配信",
        ],
        "key_operations": ["絞り込み", "横統合", "IF文", "四則演算"],
    },
    {
        "id": "sales_forecast",
        "name": "営業担当×月×商品の受注見込み集計",
        "description": "案件データから営業担当者ごと・月ごと・商品ごとの受注見込みを集計する",
        "data_sources": ["案件データ"],
        "steps": [
            "案件データの受注確度をIF文でランク分け（A: 80%以上、B: 50-79%、C: 50%未満）",
            "四則演算で見込み金額を算出（amount × order_confirmation_rate）",
            "集約: sales_rep + expected_order_month + product_name でグルーピングし、見込み金額の合計・件数を集計",
        ],
        "key_operations": ["IF文", "四則演算", "集約"],
    },
    {
        "id": "cross_division_integration",
        "name": "部門横断データ統合",
        "description": "EC・店舗・コールセンター等の複数部門データを統合して顧客360度ビューを作成する",
        "data_sources": ["顧客データ", "注文データ", "店舗マスタ", "問い合わせ履歴"],
        "steps": [
            "注文データを集約: customer_idでグルーピングし、購買回数・累計金額・最終購買日を集計",
            "店舗マスタと横統合（store_id）して店舗名・エリアを取得",
            "顧客データと横統合（customer_id）して基本情報を取得",
            "必要に応じて問い合わせデータも集約・横統合",
            "全データを横統合でcustomer_idベースに統合",
        ],
        "key_operations": ["集約", "横統合"],
    },
    {
        "id": "horizontal_to_vertical",
        "name": "横持ち→縦持ち変換",
        "description": "横持ちデータ（カラムに月別値等）を縦持ちに変換する",
        "data_sources": [],
        "steps": [
            "元データを複製して月数分のコピーを作成",
            "各コピーに「月」カラムを追加（固定値）",
            "各コピーで該当月のカラムを「値」カラムにリネーム（複製→削除で調整）",
            "全コピーを縦統合で結合",
        ],
        "key_operations": ["複製", "追加", "削除", "縦統合"],
    },
    {
        "id": "email_click_analysis",
        "name": "メール配信クリック率分析",
        "description": "メール配信のクリック率をキャンペーン別・セグメント別に分析する",
        "data_sources": ["メール配信ログ", "顧客データ", "会員ランクマスタ"],
        "steps": [
            "メール配信ログと顧客データを横統合（customer_id）",
            "会員ランクマスタと横統合（rank_id）してランク名取得",
            "IF文でクリック有無フラグ作成（click_flag = 1 → クリックあり）",
            "集約: campaign_name + rank_name でグルーピングし、配信数（件数）・クリック数（SUM of click_flag）を集計",
            "四則演算でクリック率を算出（クリック数 / 配信数 × 100）",
        ],
        "key_operations": ["横統合", "IF文", "集約", "四則演算"],
    },
]

# データパレット テンプレート（よく使う加工パターン）
PROCESSING_TEMPLATES = [
    {
        "name": "日付から年月抽出",
        "operation": "抽出",
        "description": "日付カラムから年月（YYYYMM）を取り出す",
        "example": "order_date '2024-03-15' → '202403'",
    },
    {
        "name": "経過日数算出",
        "operation": "時刻演算",
        "description": "基準日から対象日までの日数差を算出",
        "example": "現在日 - registration_date → 経過日数",
    },
    {
        "name": "金額ランク分け",
        "operation": "IF文",
        "description": "金額に応じてA/B/Cランクを付与",
        "example": "purchase_amount >= 100000 → 'A', >= 30000 → 'B', else → 'C'",
    },
    {
        "name": "NULL埋め",
        "operation": "IF文",
        "description": "NULLの場合にデフォルト値を設定",
        "example": "column IS NULL → '未設定', else → column の値",
    },
    {
        "name": "フラグ変換",
        "operation": "置換",
        "description": "0/1フラグを日本語ラベルに変換",
        "example": "1 → '有効', 0 → '無効'",
    },
    {
        "name": "税込計算",
        "operation": "四則演算",
        "description": "税抜金額から税込金額を算出",
        "example": "price × 1.1 → tax_included_price",
    },
    {
        "name": "割合算出",
        "operation": "四則演算",
        "description": "2つの数値から割合（%）を算出",
        "example": "click_count / send_count × 100 → click_rate",
    },
]


def get_all_tasks() -> dict:
    """全タスク情報を返す"""
    return {
        "processing_tasks": PROCESSING_TASKS,
        "integration_tasks": INTEGRATION_TASKS,
        "use_case_patterns": USE_CASE_PATTERNS,
        "processing_templates": PROCESSING_TEMPLATES,
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

    # ワークフロー
    lines.append("【b→dash 標準ワークフロー】")
    lines.append(f"全体フロー: {WORKFLOW_PATTERNS['standard_flow']}")
    lines.append(f"データパレット内フロー: {WORKFLOW_PATTERNS['data_palette_flow']}")
    lines.append("")

    # 加工タスク
    lines.append("【b→dashデータパレット: 加工タスク（20種類）】")
    for i, task in enumerate(PROCESSING_TASKS, 1):
        lines.append(f"{i}. {task['name']} - {task['description']}")
        lines.append(f"   GUI操作: {task['gui_path']}")
        lines.append(f"   パラメータ: {', '.join(task['params'])}")
        lines.append(f"   用途例: {task['use_case']}")
        lines.append("")

    # 統合タスク
    lines.append("【b→dashデータパレット: 統合タスク】")
    for task in INTEGRATION_TASKS:
        lines.append(f"- {task['name']} - {task['description']}")
        lines.append(f"  GUI操作: {task['gui_path']}")
        lines.append(f"  パラメータ: {', '.join(task['params'])}")
        lines.append(f"  用途例: {task['use_case']}")
        if "join_types" in task:
            for jtype, jdesc in task["join_types"].items():
                lines.append(f"    - {jtype}: {jdesc}")
        lines.append("")

    # ユースケースパターン
    lines.append("【実践ユースケースパターン（b→dashサポートページ準拠）】")
    for uc in USE_CASE_PATTERNS:
        lines.append(f"■ {uc['name']}: {uc['description']}")
        if uc['data_sources']:
            lines.append(f"  使用データ: {', '.join(uc['data_sources'])}")
        lines.append(f"  主要操作: {', '.join(uc['key_operations'])}")
        for j, step in enumerate(uc['steps'], 1):
            lines.append(f"  {j}. {step}")
        lines.append("")

    # テンプレート
    lines.append("【よく使う加工テンプレート】")
    for tmpl in PROCESSING_TEMPLATES:
        lines.append(f"- {tmpl['name']}（{tmpl['operation']}）: {tmpl['description']}")
        lines.append(f"  例: {tmpl['example']}")
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
