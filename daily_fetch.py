from backend.egx_stocks import DEFAULT_CSV_PATH, fetch_all_data, safe_save_csv


def main() -> int:
    result_df = fetch_all_data()
    if result_df.empty:
        print("Fetch completed but returned no rows.")
        return 1

    ok, message = safe_save_csv(result_df, DEFAULT_CSV_PATH)
    print(message)

    if not ok:
        return 1

    print(f"Rows fetched: {len(result_df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
