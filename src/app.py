import json
import os
import re
import time

import google.generativeai as genai

# --- Google Sheets Libraries ---
import gspread
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from duckduckgo_search import DDGS
from google.api_core import exceptions
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

# --- Selenium Setup ---
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

# ============================================================
# ★設定エリア
# ============================================================

if "GOOGLE_API_KEYS" in st.secrets:
    env_keys = st.secrets["GOOGLE_API_KEYS"]
else:
    load_dotenv()
    env_keys = os.getenv("GOOGLE_API_KEYS")

if env_keys:
    API_KEYS = env_keys.split(",")
else:
    API_KEYS = []

import os

import streamlit as st
from dotenv import load_dotenv

# --- Google Sheets Libraries ---

# --- Selenium Setup ---

# ============================================================
# ★設定エリア
# ============================================================

if "GOOGLE_API_KEYS" in st.secrets:
    env_keys = st.secrets["GOOGLE_API_KEYS"]
else:
    load_dotenv()
    env_keys = os.getenv("GOOGLE_API_KEYS")

if env_keys:
    API_KEYS = env_keys.split(",")
else:
    API_KEYS = []

MODEL_CANDIDATES = ["models/gemini-2.0-flash-exp", "models/gemini-1.5-flash"]
current_key_index = 0


def configure_genai():
    global current_key_index
    if API_KEYS and current_key_index < len(API_KEYS):
        genai.configure(api_key=API_KEYS[current_key_index])


configure_genai()


def generate_ultimate_rotation(prompt):
    global current_key_index
    if not API_KEYS:
        return "エラー: APIキーなし"
    while current_key_index < len(API_KEYS):
        for model_name in MODEL_CANDIDATES:
            try:
                model = genai.GenerativeModel(model_name)
                response = model.generate_content(prompt)
                return response.text
            except exceptions.ResourceExhausted:
                continue
            except Exception as e:
                print(f"Error: {e}")
                continue
        current_key_index += 1
        configure_genai()
    return "エラー: 全キー枯渇"


# ============================================================
# ★ Google Sheets 接続設定
# ============================================================

SHEET_URL = "https://docs.google.com/spreadsheets/d/xxxxxxxx/edit"
if "SHEET_URL" in st.secrets:
    SHEET_URL = st.secrets["SHEET_URL"]


def get_google_sheet_data():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    if "gcp_service_account" in st.secrets:
        creds_dict = st.secrets["gcp_service_account"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    else:
        json_file = "service_account.json"
        if os.path.exists(json_file):
            creds = ServiceAccountCredentials.from_json_keyfile_name(json_file, scope)
        else:
            st.error("認証ファイルが見つかりません")
            return None, None

    client = gspread.authorize(creds)
    try:
        sheet = client.open_by_url(SHEET_URL)
        worksheet = sheet.get_worksheet(0)
        data = worksheet.get_all_values()

        if not data:
            return pd.DataFrame(), worksheet

        headers = data.pop(0)
        df = pd.DataFrame(data, columns=headers)
        return df, worksheet
    except Exception as e:
        st.error(f"スプレッドシート接続エラー: {e}")
        return None, None


def save_to_google_sheet(worksheet, df):
    worksheet.clear()
    set_with_dataframe(worksheet, df)


# ============================================================
# アプリケーション本体
# ============================================================

st.set_page_config(page_title="銀行マスタ管理 Cloud", layout="wide")
st.title("🏦 銀行手続き完全自動化システム (Cloud版)")

# --- ★ここを強化：URLマスタデータの定義 ---
# 自動検索がブロックされても大丈夫なように、主要URLを最初から持っておきます
BANK_MASTER_DB = {
    "三菱UFJ銀行": "https://www.bk.mufg.jp/tsukau/tetsuduki/souzoku/index.html",
    "三井住友銀行": "https://www.smbc.co.jp/kojin/souzoku/",
    "みずほ銀行": "https://www.mizuhobank.co.jp/retail/products/souzoku/index.html",
    "ゆうちょ銀行": "https://www.jp-bank.japanpost.jp/kojin/tetuzuki/souzoku/kj_tzk_szk_index.html",
    "りそな銀行": "https://www.resonabank.co.jp/kojin/souzoku/",
    "埼玉りそな銀行": "https://www.saitamaresona.co.jp/kojin/souzoku/",
    "横浜銀行": "https://www.boy.co.jp/kojin/tetuzuki/souzoku/",
    "千葉銀行": "https://www.chibabank.co.jp/kojin/procedure/inheritance/",
    "福岡銀行": "https://www.fukuokabank.co.jp/personal/service/souzoku/",
    "静岡銀行": "https://www.shizuokabank.co.jp/personal/procedure/inheritance",
    "常陽銀行": "https://www.joyobank.co.jp/personal/service/souzoku/",
    "楽天銀行": "https://www.rakuten-bank.co.jp/support/inheritance/",
    "住信SBIネット銀行": "https://www.netbk.co.jp/contents/support/form/inheritance/",
    "ソニー銀行": "https://moneykit.net/visitor/support/inheritance.html",
    "auじぶん銀行": "https://www.jibunbank.co.jp/procedure/inheritance/",
    "三井住友信託銀行": "https://www.smtb.jp/personal/procedure/inheritance",
    "三菱UFJ信託銀行": "https://www.tr.mufg.jp/shisan/souzoku_tetsuzuki.html",
    "みずほ信託銀行": "https://www.mizuho-tb.co.jp/souzoku/tetsuzuki/",
}


def find_bank_url(bank_name):
    """
    1. まずマスタDBを見る
    2. なければ検索する（クラウドでは失敗しやすいのであくまで予備）
    """
    # マスタにあればそれを返す（高速・確実）
    if bank_name in BANK_MASTER_DB:
        return BANK_MASTER_DB[bank_name]

    # なければ検索（予備）
    try:
        query = f"{bank_name} 相続手続き"
        results = DDGS().text(query, max_results=1)
        if results:
            return results[0]["href"]
    except:
        return None
    return None


def ask_gemini_to_extract(html_text):
    prompt = f"""
    以下のHTMLから銀行情報を抽出し、必ず以下のJSON形式のみを出力してください。
    余計な装飾は一切不要です。
    
    {{
        "phone": "電話番号", "hours": "受付時間",
        "method": "手続き方法", "summary": "要約"
    }}
    HTML: {html_text[:30000]} 
    """
    return generate_ultimate_rotation(prompt)


def extract_json_from_text(text):
    try:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            return json.loads(match.group(0))
    except:
        pass
    return None


def process_single_bank(bank_name, target_url):
    # URLの補完ロジック
    if not target_url or pd.isna(target_url) or target_url == "":
        st.write(f"🔍URL確認中: {bank_name}...")
        found = find_bank_url(bank_name)  # ここでマスタDBが効く
        if found:
            target_url = found
            st.write(f"   → URLセット: {target_url}")
        else:
            return None, "URLなし", ""

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    # ユーザーエージェント偽装（アクセス拒否対策）
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    try:
        driver = webdriver.Chrome(
            service=ChromeService(ChromeDriverManager().install()), options=options
        )
        driver.set_page_load_timeout(60)  # タイムアウト延長
        driver.get(target_url)
        time.sleep(5)  # 読み込み待ち延長

        body = driver.find_element("tag name", "body").text
        driver.quit()

        json_text = ask_gemini_to_extract(body)
        return json_text, "Success", target_url
    except Exception as e:
        return None, f"Error: {str(e)}", target_url


# --- メイン処理 ---

df, worksheet = get_google_sheet_data()

# 空の場合の初期化
if df is not None and df.empty:
    bank_names = list(BANK_MASTER_DB.keys())
    # 初期URLもセットする
    init_urls = [BANK_MASTER_DB[name] for name in bank_names]

    df = pd.DataFrame(
        {
            "金融機関名": bank_names,
            "WebサイトURL": init_urls,
            "電話番号": [""] * len(bank_names),
            "受付時間": [""] * len(bank_names),
            "手続き方法": [""] * len(bank_names),
            "AI要約": ["未取得"] * len(bank_names),
            "最終更新": ["-"] * len(bank_names),
        }
    )
    save_to_google_sheet(worksheet, df)
    st.rerun()

st.markdown("### 🚀 一括自動収集")
col1, col2 = st.columns([2, 1])

with col1:
    if st.button("全銀行更新 (Cloud)", type="primary"):
        if df is not None:
            total = len(df)
            bar = st.progress(0)
            status_text = st.empty()

            for i, row in df.iterrows():
                bank = row["金融機関名"]
                # 既存URLがあれば使う、なければマスタから引く
                current_url = (
                    row["WebサイトURL"] if "WebサイトURL" in df.columns else ""
                )

                status_text.text(f"アクセス中: {bank} ...")

                res_json_text, status, final_url = process_single_bank(
                    bank, current_url
                )

                if final_url:
                    df.at[i, "WebサイトURL"] = final_url

                if status == "Success" and res_json_text:
                    data = extract_json_from_text(res_json_text)
                    if data:
                        df.at[i, "電話番号"] = data.get("phone", "不明")
                        df.at[i, "受付時間"] = data.get("hours", "不明")
                        df.at[i, "手続き方法"] = data.get("method", "不明")
                        df.at[i, "AI要約"] = data.get("summary", "抽出成功")
                    else:
                        df.at[i, "AI要約"] = "JSON解析失敗"
                elif status != "Success":
                    df.at[i, "AI要約"] = f"アクセス失敗: {status}"

                import datetime

                df.at[i, "最終更新"] = datetime.datetime.now().strftime(
                    "%Y-%m-%d %H:%M"
                )

                save_to_google_sheet(worksheet, df)
                bar.progress((i + 1) / total)

            status_text.success("完了！リロードします")
            time.sleep(1)
            st.rerun()

with col2:
    if st.button("⚠️ 銀行リストを初期化・再読込"):
        bank_names = list(BANK_MASTER_DB.keys())
        init_urls = [BANK_MASTER_DB[name] for name in bank_names]

        new_df = pd.DataFrame(
            {
                "金融機関名": bank_names,
                "WebサイトURL": init_urls,
                "電話番号": [""] * len(bank_names),
                "受付時間": [""] * len(bank_names),
                "手続き方法": [""] * len(bank_names),
                "AI要約": ["未取得"] * len(bank_names),
                "最終更新": ["-"] * len(bank_names),
            }
        )
        save_to_google_sheet(worksheet, new_df)
        st.warning("リストを初期化しました（URL入り）。")
        time.sleep(1)
        st.rerun()

st.markdown("---")
if df is not None:
    column_config = {
        "WebサイトURL": st.column_config.LinkColumn("URL", display_text="開く")
    }
    edited_df = st.data_editor(
        df, column_config=column_config, num_rows="dynamic", use_container_width=True
    )

    if st.button("手動変更を保存"):
        save_to_google_sheet(worksheet, edited_df)
        st.success("スプレッドシートに保存しました")
current_key_index = 0


def configure_genai():
    global current_key_index
    if API_KEYS and current_key_index < len(API_KEYS):
        genai.configure(api_key=API_KEYS[current_key_index])


configure_genai()


def generate_ultimate_rotation(prompt):
    global current_key_index
    if not API_KEYS:
        return "エラー: APIキーなし"
    while current_key_index < len(API_KEYS):
        for model_name in MODEL_CANDIDATES:
            try:
                model = genai.GenerativeModel(model_name)
                response = model.generate_content(prompt)
                return response.text
            except exceptions.ResourceExhausted:
                continue
            except Exception as e:
                print(f"Error: {e}")
                continue
        current_key_index += 1
        configure_genai()
    return "エラー: 全キー枯渇"


# ============================================================
# ★ Google Sheets 接続設定
# ============================================================

SHEET_URL = "https://docs.google.com/spreadsheets/d/xxxxxxxx/edit"
if "SHEET_URL" in st.secrets:
    SHEET_URL = st.secrets["SHEET_URL"]


def get_google_sheet_data():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    if "gcp_service_account" in st.secrets:
        creds_dict = st.secrets["gcp_service_account"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    else:
        json_file = "service_account.json"
        if os.path.exists(json_file):
            creds = ServiceAccountCredentials.from_json_keyfile_name(json_file, scope)
        else:
            st.error("認証ファイルが見つかりません")
            return None, None

    client = gspread.authorize(creds)
    try:
        sheet = client.open_by_url(SHEET_URL)
        worksheet = sheet.get_worksheet(0)
        data = worksheet.get_all_values()

        if not data:
            return pd.DataFrame(), worksheet

        headers = data.pop(0)
        df = pd.DataFrame(data, columns=headers)
        return df, worksheet
    except Exception as e:
        st.error(f"スプレッドシート接続エラー: {e}")
        return None, None


def save_to_google_sheet(worksheet, df):
    worksheet.clear()
    set_with_dataframe(worksheet, df)


# ============================================================
# アプリケーション本体
# ============================================================

st.set_page_config(page_title="銀行マスタ管理 Cloud", layout="wide")
st.title("🏦 銀行手続き完全自動化システム (Cloud版)")

# --- ★ここを強化：URLマスタデータの定義 ---
# 自動検索がブロックされても大丈夫なように、主要URLを最初から持っておきます
BANK_MASTER_DB = {
    "三菱UFJ銀行": "https://www.bk.mufg.jp/tsukau/tetsuduki/souzoku/index.html",
    "三井住友銀行": "https://www.smbc.co.jp/kojin/souzoku/",
    "みずほ銀行": "https://www.mizuhobank.co.jp/retail/products/souzoku/index.html",
    "ゆうちょ銀行": "https://www.jp-bank.japanpost.jp/kojin/tetuzuki/souzoku/kj_tzk_szk_index.html",
    "りそな銀行": "https://www.resonabank.co.jp/kojin/souzoku/",
    "埼玉りそな銀行": "https://www.saitamaresona.co.jp/kojin/souzoku/",
    "横浜銀行": "https://www.boy.co.jp/kojin/tetuzuki/souzoku/",
    "千葉銀行": "https://www.chibabank.co.jp/kojin/procedure/inheritance/",
    "福岡銀行": "https://www.fukuokabank.co.jp/personal/service/souzoku/",
    "静岡銀行": "https://www.shizuokabank.co.jp/personal/procedure/inheritance",
    "常陽銀行": "https://www.joyobank.co.jp/personal/service/souzoku/",
    "楽天銀行": "https://www.rakuten-bank.co.jp/support/inheritance/",
    "住信SBIネット銀行": "https://www.netbk.co.jp/contents/support/form/inheritance/",
    "ソニー銀行": "https://moneykit.net/visitor/support/inheritance.html",
    "auじぶん銀行": "https://www.jibunbank.co.jp/procedure/inheritance/",
    "三井住友信託銀行": "https://www.smtb.jp/personal/procedure/inheritance",
    "三菱UFJ信託銀行": "https://www.tr.mufg.jp/shisan/souzoku_tetsuzuki.html",
    "みずほ信託銀行": "https://www.mizuho-tb.co.jp/souzoku/tetsuzuki/",
}


def find_bank_url(bank_name):
    """
    1. まずマスタDBを見る
    2. なければ検索する（クラウドでは失敗しやすいのであくまで予備）
    """
    # マスタにあればそれを返す（高速・確実）
    if bank_name in BANK_MASTER_DB:
        return BANK_MASTER_DB[bank_name]

    # なければ検索（予備）
    try:
        query = f"{bank_name} 相続手続き"
        results = DDGS().text(query, max_results=1)
        if results:
            return results[0]["href"]
    except:
        return None
    return None


def ask_gemini_to_extract(html_text):
    prompt = f"""
    以下のHTMLから銀行情報を抽出し、必ず以下のJSON形式のみを出力してください。
    余計な装飾は一切不要です。
    
    {{
        "phone": "電話番号", "hours": "受付時間",
        "method": "手続き方法", "summary": "要約"
    }}
    HTML: {html_text[:30000]} 
    """
    return generate_ultimate_rotation(prompt)


def extract_json_from_text(text):
    try:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            return json.loads(match.group(0))
    except:
        pass
    return None


def process_single_bank(bank_name, target_url):
    # URLの補完ロジック
    if not target_url or pd.isna(target_url) or target_url == "":
        st.write(f"🔍URL確認中: {bank_name}...")
        found = find_bank_url(bank_name)  # ここでマスタDBが効く
        if found:
            target_url = found
            st.write(f"   → URLセット: {target_url}")
        else:
            return None, "URLなし", ""

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    # ユーザーエージェント偽装（アクセス拒否対策）
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    try:
        driver = webdriver.Chrome(
            service=ChromeService(ChromeDriverManager().install()), options=options
        )
        driver.set_page_load_timeout(60)  # タイムアウト延長
        driver.get(target_url)
        time.sleep(5)  # 読み込み待ち延長

        body = driver.find_element("tag name", "body").text
        driver.quit()

        json_text = ask_gemini_to_extract(body)
        return json_text, "Success", target_url
    except Exception as e:
        return None, f"Error: {str(e)}", target_url


# --- メイン処理 ---

df, worksheet = get_google_sheet_data()

# 空の場合の初期化
if df is not None and df.empty:
    bank_names = list(BANK_MASTER_DB.keys())
    # 初期URLもセットする
    init_urls = [BANK_MASTER_DB[name] for name in bank_names]

    df = pd.DataFrame(
        {
            "金融機関名": bank_names,
            "WebサイトURL": init_urls,
            "電話番号": [""] * len(bank_names),
            "受付時間": [""] * len(bank_names),
            "手続き方法": [""] * len(bank_names),
            "AI要約": ["未取得"] * len(bank_names),
            "最終更新": ["-"] * len(bank_names),
        }
    )
    save_to_google_sheet(worksheet, df)
    st.rerun()

st.markdown("### 🚀 一括自動収集")
col1, col2 = st.columns([2, 1])

with col1:
    if st.button("全銀行更新 (Cloud)", type="primary"):
        if df is not None:
            total = len(df)
            bar = st.progress(0)
            status_text = st.empty()

            for i, row in df.iterrows():
                bank = row["金融機関名"]
                # 既存URLがあれば使う、なければマスタから引く
                current_url = (
                    row["WebサイトURL"] if "WebサイトURL" in df.columns else ""
                )

                status_text.text(f"アクセス中: {bank} ...")

                res_json_text, status, final_url = process_single_bank(
                    bank, current_url
                )

                if final_url:
                    df.at[i, "WebサイトURL"] = final_url

                if status == "Success" and res_json_text:
                    data = extract_json_from_text(res_json_text)
                    if data:
                        df.at[i, "電話番号"] = data.get("phone", "不明")
                        df.at[i, "受付時間"] = data.get("hours", "不明")
                        df.at[i, "手続き方法"] = data.get("method", "不明")
                        df.at[i, "AI要約"] = data.get("summary", "抽出成功")
                    else:
                        df.at[i, "AI要約"] = "JSON解析失敗"
                elif status != "Success":
                    df.at[i, "AI要約"] = f"アクセス失敗: {status}"

                import datetime

                df.at[i, "最終更新"] = datetime.datetime.now().strftime(
                    "%Y-%m-%d %H:%M"
                )

                save_to_google_sheet(worksheet, df)
                bar.progress((i + 1) / total)

            status_text.success("完了！リロードします")
            time.sleep(1)
            st.rerun()

with col2:
    if st.button("⚠️ 銀行リストを初期化・再読込"):
        bank_names = list(BANK_MASTER_DB.keys())
        init_urls = [BANK_MASTER_DB[name] for name in bank_names]

        new_df = pd.DataFrame(
            {
                "金融機関名": bank_names,
                "WebサイトURL": init_urls,
                "電話番号": [""] * len(bank_names),
                "受付時間": [""] * len(bank_names),
                "手続き方法": [""] * len(bank_names),
                "AI要約": ["未取得"] * len(bank_names),
                "最終更新": ["-"] * len(bank_names),
            }
        )
        save_to_google_sheet(worksheet, new_df)
        st.warning("リストを初期化しました（URL入り）。")
        time.sleep(1)
        st.rerun()

st.markdown("---")
if df is not None:
    column_config = {
        "WebサイトURL": st.column_config.LinkColumn("URL", display_text="開く")
    }
    edited_df = st.data_editor(
        df, column_config=column_config, num_rows="dynamic", use_container_width=True
    )

    if st.button("手動変更を保存"):
        save_to_google_sheet(worksheet, edited_df)
        st.success("スプレッドシートに保存しました")
