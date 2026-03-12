"""Excel Parserのテスト"""
import io
import openpyxl
from excel_parser import parse_excel, parse_multiple_files, get_join_keys, get_effective_physical_name


def create_test_excel() -> io.BytesIO:
    """テスト用Excelファイルを作成"""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "カラム定義"

    headers = [
        "データファイルID", "データファイル名", "カラムID", "顧客IDフラグ",
        "カラム論理名", "カラム物理名", "カスタムカラム物理名",
        "カラム作成時間(JST)", "カラム更新時間(JST)", "カラム削除時間(JST)",
    ]
    ws.append(headers)

    rows = [
        ["DF001", "顧客マスタ", "C001", 1, "顧客ID", "customer_id", "", "2024-01-15 10:00:00", "2024-06-01 12:00:00", ""],
        ["DF001", "顧客マスタ", "C002", 0, "顧客名", "customer_name", "", "2024-01-15 10:00:00", "2024-06-01 12:00:00", ""],
        ["DF001", "顧客マスタ", "C003", 0, "メールアドレス", "email", "customer_email", "2024-01-15 10:00:00", "2024-08-20 09:00:00", ""],
        ["DF001", "顧客マスタ", "C004", 0, "電話番号", "phone_number", "", "2024-01-15 10:00:00", "2024-03-01 15:00:00", "2024-09-01 10:00:00"],
        ["DF002", "購買履歴", "C010", 0, "注文ID", "order_id", "", "2024-02-01 09:00:00", "2024-07-15 11:00:00", ""],
        ["DF002", "購買履歴", "C011", 1, "顧客ID", "customer_id", "", "2024-02-01 09:00:00", "2024-07-15 11:00:00", ""],
        ["DF002", "購買履歴", "C012", 0, "購買金額", "purchase_amount", "order_amount", "2024-02-01 09:00:00", "2024-10-01 14:00:00", ""],
    ]
    for row in rows:
        ws.append(row)

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


def create_test_excel_2() -> io.BytesIO:
    """2つ目のテスト用Excelファイル（商品マスタ）"""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "カラム定義"

    headers = [
        "データファイルID", "データファイル名", "カラムID", "顧客IDフラグ",
        "カラム論理名", "カラム物理名", "カスタムカラム物理名",
        "カラム作成時間(JST)", "カラム更新時間(JST)", "カラム削除時間(JST)",
    ]
    ws.append(headers)

    rows = [
        ["DF003", "商品マスタ", "C020", 0, "商品ID", "product_id", "", "2024-03-01 10:00:00", "2024-09-01 16:00:00", ""],
        ["DF003", "商品マスタ", "C021", 0, "商品名", "product_name", "", "2024-03-01 10:00:00", "2024-09-01 16:00:00", ""],
        ["DF003", "商品マスタ", "C022", 0, "カテゴリ", "category", "product_category", "2024-03-01 10:00:00", "2024-09-01 16:00:00", ""],
    ]
    for row in rows:
        ws.append(row)

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


def test_single_file():
    print("=== テスト1: 単一ファイルパース ===")
    buf = create_test_excel()
    results = parse_excel(buf, "test1.xlsx")

    assert len(results) == 7, f"Expected 7 rows, got {len(results)}"
    print(f"  行数: {len(results)} OK")

    # 顧客IDフラグ確認
    customer_id_flags = [r for r in results if r["customer_id_flag"] == 1]
    assert len(customer_id_flags) == 2, f"Expected 2 customer ID flags, got {len(customer_id_flags)}"
    print(f"  顧客IDフラグ=1: {len(customer_id_flags)}件 OK")

    # 削除済みカラム確認
    deleted = [r for r in results if r["is_deleted"]]
    assert len(deleted) == 1, f"Expected 1 deleted, got {len(deleted)}"
    assert deleted[0]["logical_name"] == "電話番号"
    print(f"  削除済み: {deleted[0]['logical_name']} OK")

    # カスタム物理名優先確認
    email_col = [r for r in results if r["logical_name"] == "メールアドレス"][0]
    assert email_col["effective_physical_name"] == "customer_email"
    print(f"  カスタム物理名優先: {email_col['effective_physical_name']} OK")

    amount_col = [r for r in results if r["logical_name"] == "購買金額"][0]
    assert amount_col["effective_physical_name"] == "order_amount"
    print(f"  カスタム物理名優先2: {amount_col['effective_physical_name']} OK")

    print("  PASS\n")


def test_multiple_files():
    print("=== テスト2: 複数ファイルパース ===")
    buf1 = create_test_excel()
    buf2 = create_test_excel_2()

    result = parse_multiple_files([("file1.xlsx", buf1), ("file2.xlsx", buf2)])

    assert len(result["data_files"]) == 3, f"Expected 3 data files, got {len(result['data_files'])}"
    print(f"  データファイル数: {len(result['data_files'])} OK")
    print(f"  総カラム数: {result['total_columns']} OK")
    print(f"  有効カラム数: {result['total_active']} OK")
    print(f"  削除済み数: {result['total_deleted']} OK")

    # データファイル名一覧
    file_names = [f["file_name"] for f in result["data_files"]]
    assert "顧客マスタ" in file_names
    assert "購買履歴" in file_names
    assert "商品マスタ" in file_names
    print(f"  データファイル名: {file_names} OK")

    print("  PASS\n")


def test_join_keys():
    print("=== テスト3: JOINキー検出 ===")
    buf = create_test_excel()
    results = parse_excel(buf, "test.xlsx")
    keys = get_join_keys(results)

    assert len(keys) == 2
    key_names = [k["logical_name"] for k in keys]
    assert all(n == "顧客ID" for n in key_names)
    print(f"  JOINキー: {key_names} OK")
    print("  PASS\n")


def test_effective_name():
    print("=== テスト4: 有効物理名 ===")
    col_with_custom = {"physical_name": "email", "custom_physical_name": "customer_email"}
    col_without_custom = {"physical_name": "email", "custom_physical_name": ""}
    col_none_custom = {"physical_name": "email"}

    assert get_effective_physical_name(col_with_custom) == "customer_email"
    assert get_effective_physical_name(col_without_custom) == "email"
    assert get_effective_physical_name(col_none_custom) == "email"
    print("  全パターン OK")
    print("  PASS\n")


if __name__ == "__main__":
    test_single_file()
    test_multiple_files()
    test_join_keys()
    test_effective_name()
    print("ALL TESTS PASSED!")
