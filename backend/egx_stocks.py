import pandas as pd
import yfinance as yf
import logging
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from tvDatafeed import Interval, TvDatafeed

# Reduce noisy logs from third-party APIs in the Streamlit console.
logging.getLogger("tvDatafeed").setLevel(logging.CRITICAL)
logging.getLogger("tvDatafeed.main").setLevel(logging.CRITICAL)
logging.getLogger("yfinance").setLevel(logging.CRITICAL)


BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent


def resolve_data_path(filename: str) -> Path:
    backend_path = BASE_DIR / filename
    root_path = ROOT_DIR / filename

    if backend_path.exists():
        return backend_path
    if root_path.exists():
        return root_path
    return backend_path


SLOW_OR_MISSING_YAHOO_SYMBOLS = {
    "ACTF",
    "AIDC",
    "AIHC",
    "EIUD",
    "EKHO",
    "GOCO",
    "GPIM",
    "KRDI",
    "PHGC",
    "TANM",
    "TAQA",
    "VALU",
    "VERT",
    "VLMR",
    "VLMRA",
}
SLOW_OR_MISSING_TV_SYMBOLS = {"AIHC"}
DEFAULT_CSV_PATH = resolve_data_path("EGX_latest_close_52week.csv")
SYMBOLS_EXCEL_PATH = resolve_data_path("EGX100.xlsx")
SYMBOLS_EXCEL_SHEET = "EGX100"
YAHOO_SUFFIX = ".CA"
TV_RETRIES = 1
PER_CALL_TIMEOUT_SECONDS = 8
SKIP_SLOW_YAHOO = True
RESULT_COLUMNS = [
    "Symbol",
    "Company",
    "Latest Close",
    "52W High",
    "% From 52W High",
]

# Priority order for major EGX names so loading starts with stronger/larger companies.
BLUE_CHIP_PRIORITY = [
    "COMI",  # Commercial International Bank
    "HRHO",  # EFG Holding
    "ETEL",  # Telecom Egypt
    "SWDY",  # Elsewedy Electric
    "EAST",  # Eastern Company
    "FWRY",  # Fawry
    "ABUK",  # Abu Qir Fertilizers
    "ADIB",  # Abu Dhabi Islamic Bank Egypt
    "CNFN",  # Contact Financial
    "MFPC",  # Misr Fertilizers Production
    "ORWE",  # Oriental Weavers
    "TMGH",  # Talaat Moustafa Group
    "ORAS",  # Orascom Construction
    "EGAL",  # Egypt Aluminum
    "HDBK",  # Housing and Development Bank
]


def is_valid_egx_ticker(symbol: object) -> bool:
    return isinstance(symbol, str) and symbol.isalpha() and len(symbol) <= 5


def normalize_excel_symbol(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip().upper()
    if not text:
        return None

    # Symbols in Excel may include suffixes like .CA; keep only the base ticker.
    base = text.split(".", 1)[0]
    base = "".join(ch for ch in base if ch.isalpha())
    return base or None


def normalize_company_name(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def run_with_timeout(func, timeout_seconds: int):
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(func)
        return future.result(timeout=timeout_seconds)


def safe_save_csv(df: pd.DataFrame, path: Path) -> tuple[bool, str]:
    try:
        df.to_csv(path, index=False)
        return True, f"Saved {path.name} to workspace."
    except PermissionError:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        fallback = path.with_name(f"{path.stem}_{timestamp}{path.suffix}")
        df.to_csv(fallback, index=False)
        return (
            True,
            f"{path.name} is currently locked. Saved instead as {fallback.name}.",
        )
    except Exception as exc:
        return False, f"Could not save CSV: {exc}"


def prioritize_symbols(symbols: list[str]) -> list[str]:
    symbol_set = set(symbols)
    prioritized = [s for s in BLUE_CHIP_PRIORITY if s in symbol_set]
    remaining = [s for s in symbols if s not in set(prioritized)]
    return prioritized + remaining


def fmt_number(value: object) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):.2f}"


def fmt_percent(value: object) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):.2f}%"


def load_egx_symbols() -> pd.DataFrame:
    if not SYMBOLS_EXCEL_PATH.exists():
        raise FileNotFoundError(f"Symbols file not found: {SYMBOLS_EXCEL_PATH}")

    xls = pd.ExcelFile(SYMBOLS_EXCEL_PATH)
    sheet_name = SYMBOLS_EXCEL_SHEET
    if sheet_name not in xls.sheet_names:
        normalized_target = sheet_name.strip().casefold()
        normalized_map = {name.strip().casefold(): name for name in xls.sheet_names}
        sheet_name = normalized_map.get(normalized_target, xls.sheet_names[0])

    symbols_df = pd.read_excel(SYMBOLS_EXCEL_PATH, sheet_name=sheet_name)
    if symbols_df.shape[1] < 2:
        raise ValueError("EGX100 sheet must contain at least two columns.")

    # Prefer the company-name column (commonly third column), fallback to first.
    name_col_index = 2 if symbols_df.shape[1] >= 3 else 0
    raw_names = symbols_df.iloc[:, name_col_index].apply(normalize_company_name)
    raw_symbols = symbols_df.iloc[:, 1].apply(normalize_excel_symbol)
    filtered = pd.DataFrame({"company": raw_names, "ticker": raw_symbols})
    filtered = filtered[filtered["ticker"].notna()].copy()
    filtered = filtered[filtered["ticker"].apply(is_valid_egx_ticker)].copy()
    filtered["company"] = filtered["company"].fillna(filtered["ticker"])
    filtered = filtered.drop_duplicates(subset=["ticker"]).sort_values("ticker")
    return filtered


def fetch_latest_close(
    tv: TvDatafeed,
    symbol: str,
    retries: int,
    timeout_seconds: int,
) -> tuple[float | None, str | None]:
    if symbol in SLOW_OR_MISSING_TV_SYMBOLS:
        return None, "TV skipped"

    for attempt in range(retries + 1):
        try:
            data = run_with_timeout(
                lambda: tv.get_hist(
                    symbol=symbol,
                    exchange="EGX",
                    interval=Interval.in_daily,
                    n_bars=1,
                ),
                timeout_seconds=timeout_seconds,
            )
            if data is None or data.empty:
                return None, "No TV data"
            return float(data["close"].iloc[-1]), None
        except FuturesTimeoutError:
            if attempt == retries:
                return None, "TV timeout"
        except Exception as exc:
            message = str(exc).lower()
            if "no data" in message or "check the exchange and symbol" in message:
                return None, "No TV data"
            if attempt == retries:
                return None, "TV error"
    return None, "TV unknown error"


def fetch_tv_52w_high(
    tv: TvDatafeed,
    symbol: str,
    retries: int,
    timeout_seconds: int,
) -> tuple[float | None, str | None]:
    if symbol in SLOW_OR_MISSING_TV_SYMBOLS:
        return None, "TV skipped"

    for attempt in range(retries + 1):
        try:
            data = run_with_timeout(
                lambda: tv.get_hist(
                    symbol=symbol,
                    exchange="EGX",
                    interval=Interval.in_daily,
                    n_bars=260,
                ),
                timeout_seconds=timeout_seconds,
            )
            if data is None or data.empty or "high" not in data.columns:
                return None, "No TV data"

            highs = pd.to_numeric(data["high"], errors="coerce").dropna()
            if highs.empty:
                return None, "No TV data"
            return float(highs.max()), None
        except FuturesTimeoutError:
            if attempt == retries:
                return None, "TV timeout"
        except Exception as exc:
            message = str(exc).lower()
            if "no data" in message or "check the exchange and symbol" in message:
                return None, "No TV data"
            if attempt == retries:
                return None, "TV error"
    return None, "TV unknown error"


def calculate_split_adjusted_high(history: pd.DataFrame) -> float | None:
    if history is None or history.empty or "High" not in history.columns:
        return None

    high = pd.to_numeric(history["High"], errors="coerce")
    split_events = history.get("Stock Splits")

    if split_events is None:
        adjusted_high = high
    else:
        split_series = pd.to_numeric(split_events, errors="coerce").fillna(0)
        split_factor = split_series.replace(0, 1).shift(-1, fill_value=1).iloc[::-1].cumprod().iloc[::-1]
        adjusted_high = high / split_factor

    adjusted_high = adjusted_high.dropna()
    if adjusted_high.empty:
        return None
    return float(adjusted_high.max())


def fetch_yahoo_metrics(
    symbol: str,
    suffix: str,
    timeout_seconds: int,
    skip_slow_yahoo: bool,
) -> tuple[float | None, str | None]:
    if skip_slow_yahoo and symbol in SLOW_OR_MISSING_YAHOO_SYMBOLS:
        return None, "Yahoo skipped"

    try:
        ticker = yf.Ticker(f"{symbol}{suffix}")
        high_52week = None

        # Adjust highs only for share distributions/splits, not cash dividends.
        try:
            hist_1y = run_with_timeout(
                lambda: ticker.history(
                    period="1y",
                    interval="1d",
                    auto_adjust=False,
                    actions=True,
                ),
                timeout_seconds=timeout_seconds,
            )
            if hist_1y is not None and not hist_1y.empty:
                high_52week = calculate_split_adjusted_high(hist_1y)
        except Exception:
            pass

        if high_52week is None:
            return None, "No Yahoo high"

        return high_52week, None
    except FuturesTimeoutError:
        return None, "Yahoo timeout"
    except Exception:
        return None, "Yahoo error"


def build_result_row(
    tv: TvDatafeed,
    symbol: str,
    company: str,
    suffix: str,
    tv_retries: int,
    per_call_timeout_seconds: int,
    skip_slow_yahoo: bool,
) -> dict[str, float | str | None]:
    latest_close, _tv_error = fetch_latest_close(
        tv=tv,
        symbol=symbol,
        retries=tv_retries,
        timeout_seconds=per_call_timeout_seconds,
    )
    high_52week, _yf_error = fetch_yahoo_metrics(
        symbol=symbol,
        suffix=suffix,
        timeout_seconds=per_call_timeout_seconds,
        skip_slow_yahoo=skip_slow_yahoo,
    )

    if high_52week is None:
        high_52week, _tv_52w_error = fetch_tv_52w_high(
            tv=tv,
            symbol=symbol,
            retries=tv_retries,
            timeout_seconds=per_call_timeout_seconds,
        )

    if latest_close is not None and high_52week not in (None, 0):
        pct_from_high = ((latest_close - high_52week) / high_52week) * 100
    else:
        pct_from_high = None

    return {
        "Symbol": symbol,
        "Company": company,
        "Latest Close": latest_close,
        "52W High": high_52week,
        "% From 52W High": pct_from_high,
    }


def collect_stock_data(
    symbol_company_pairs: list[tuple[str, str]],
    suffix: str,
    progress_bar=None,
    status_text=None,
    tv_retries: int = TV_RETRIES,
    per_call_timeout_seconds: int = PER_CALL_TIMEOUT_SECONDS,
    skip_slow_yahoo: bool = SKIP_SLOW_YAHOO,
) -> pd.DataFrame:
    results: list[dict[str, float | str | None]] = []
    tv = TvDatafeed()

    total = len(symbol_company_pairs)
    for idx, (symbol, company) in enumerate(symbol_company_pairs, start=1):
        if status_text is not None:
            status_text.text(f"Fetching {idx}/{total}: {symbol}")
        row = build_result_row(
            tv=tv,
            symbol=symbol,
            company=company,
            suffix=suffix,
            tv_retries=tv_retries,
            per_call_timeout_seconds=per_call_timeout_seconds,
            skip_slow_yahoo=skip_slow_yahoo,
        )
        results.append(row)

        if progress_bar is not None:
            progress_bar.progress(idx / total)
        if status_text is not None:
            status_text.text(f"Completed {idx}/{total}: {symbol}")

    if status_text is not None:
        status_text.text("Data fetch completed.")
    return pd.DataFrame(results, columns=RESULT_COLUMNS)


def build_symbol_company_pairs() -> tuple[list[tuple[str, str]], dict[str, str], int]:
    symbols_df = load_egx_symbols()
    all_symbols = symbols_df["ticker"].tolist()
    symbol_to_company = symbols_df.set_index("ticker")["company"].to_dict()
    ordered_symbols = prioritize_symbols(all_symbols)
    selected_pairs = [(symbol, symbol_to_company.get(symbol, symbol)) for symbol in ordered_symbols]
    return selected_pairs, symbol_to_company, len(all_symbols)


def fetch_all_data(progress_bar=None, status_text=None) -> pd.DataFrame:
    selected_pairs, _, _ = build_symbol_company_pairs()
    if not selected_pairs:
        return pd.DataFrame(columns=RESULT_COLUMNS)

    result_df = collect_stock_data(
        symbol_company_pairs=selected_pairs,
        suffix=YAHOO_SUFFIX,
        progress_bar=progress_bar,
        status_text=status_text,
        tv_retries=TV_RETRIES,
        per_call_timeout_seconds=PER_CALL_TIMEOUT_SECONDS,
        skip_slow_yahoo=SKIP_SLOW_YAHOO,
    )
    return result_df.sort_values("Symbol").reset_index(drop=True)


def main() -> None:
    try:
        import streamlit as st
    except ImportError:
        raise SystemExit("Streamlit is not installed. Run: pip install streamlit")

    st.set_page_config(page_title="EGX Stocks Dashboard", page_icon="📈", layout="wide")
    st.title("EGX Stocks Dashboard")
    st.caption(
        "Live EGX dashboard with daily refreshed data for all symbols from the Excel sheet."
    )

    controls_col, button_col = st.columns([4, 1])
    with controls_col:
        search_query = st.text_input("Search company or symbol", value="")
    with button_col:
        st.write("")
        fetch_clicked = st.button("Refresh Data Now", type="primary", use_container_width=True)

    try:
        selected_pairs, symbol_to_company, total_symbols = build_symbol_company_pairs()
    except Exception as exc:
        st.error(f"Failed to load symbols dataset: {exc}")
        return

    st.info(f"Loaded {total_symbols} valid EGX symbols from Excel.")

    if fetch_clicked:
        if not selected_pairs:
            st.warning("No valid symbols found in the Excel symbol column.")
            return

        progress_bar = st.progress(0)
        status_text = st.empty()

        with st.spinner("Collecting data from APIs..."):
            status_text.text("Fetching all symbols...")
            result_df = fetch_all_data(progress_bar=progress_bar, status_text=status_text)

        st.session_state["result_df"] = result_df
        ok, message = safe_save_csv(result_df, DEFAULT_CSV_PATH)
        if ok:
            st.success(message)
        else:
            st.error(message)

    if "result_df" not in st.session_state:
        if DEFAULT_CSV_PATH.exists():
            try:
                st.session_state["result_df"] = pd.read_csv(DEFAULT_CSV_PATH)
            except Exception as exc:
                st.error(f"Could not load {DEFAULT_CSV_PATH.name}: {exc}")
                return
        else:
            with st.spinner("No saved data found. Running first full fetch..."):
                progress_bar = st.progress(0)
                status_text = st.empty()
                result_df = fetch_all_data(progress_bar=progress_bar, status_text=status_text)
                st.session_state["result_df"] = result_df
                ok, message = safe_save_csv(result_df, DEFAULT_CSV_PATH)
                if ok:
                    st.success(message)
                else:
                    st.warning(message)

    result_df = st.session_state.get("result_df")

    if result_df is not None and not result_df.empty:
        if "Company" not in result_df.columns:
            result_df["Company"] = result_df["Symbol"].map(symbol_to_company)
        result_df["Company"] = result_df["Company"].fillna(result_df["Symbol"])

        display_df = result_df.copy()
        search_text = search_query.strip()
        if search_text:
            query = search_text.casefold()
            symbol_match = display_df["Symbol"].astype(str).str.casefold().str.contains(query, na=False)
            company_match = display_df["Company"].astype(str).str.casefold().str.contains(query, na=False)
            display_df = display_df[symbol_match | company_match].copy()

        valid_close_count = int(display_df["Latest Close"].notna().sum())
        valid_high_count = int(display_df["52W High"].notna().sum())
        both_count = int(display_df[["Latest Close", "52W High"]].notna().all(axis=1).sum())

        c1, c2, c3 = st.columns(3)
        c1.metric("Rows", len(display_df))
        c2.metric("Latest Close Available", valid_close_count)
        c3.metric("52W High Available", valid_high_count)

        styled_df = display_df.style.format(
            {
                "Latest Close": fmt_number,
                "52W High": fmt_number,
                "% From 52W High": fmt_percent,
            }
        )

        st.dataframe(styled_df, use_container_width=True, hide_index=True)

        st.caption(f"Rows with both latest close and 52W high: {both_count}")

        csv_data = display_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download CSV",
            data=csv_data,
            file_name="EGX_latest_close_52week.csv",
            mime="text/csv",
            use_container_width=True,
        )
    else:
        st.warning("No data available yet.")


if __name__ == "__main__":
    main()