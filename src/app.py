import json
import os
import re  # 正規表現用（追加）
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

MODEL_CANDIDATES = [
    "models/gemini-2.5-flash-lite",
    "models/gemini-2.5-flash",
]
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

FULL_BANK_LIST = [
    "三菱UFJ銀行",
    "三井住友銀行",
    "みずほ銀行",
    "ゆうちょ銀行",
    "りそな銀行",
    "埼玉りそな銀行",
    "横浜銀行",
    "千葉銀行",
    "福岡銀行",
    "静岡銀行",
    "常陽銀行",
    "楽天銀行",
    "住信SBIネット銀行",
    "ソニー銀行",
    "auじぶん銀行",
    "三井住友信託銀行",
    "三菱UFJ信託銀行",
    "みずほ信託銀行",
]


def find_bank_url(bank_name):
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
    余計なMarkdown装飾や挨拶は一切不要です。
    
    {{
        "phone": "電話番号", "hours": "受付時間",
        "method": "手続き方法", "summary": "要約"
    }}
    HTML: {html_text[:30000]} 
    """
    return generate_ultimate_rotation(prompt)


def process_single_bank(bank_name, target_url):
    if not target_url or pd.isna(target_url) or target_url == "":
        st.write(f"🔍検索中: {bank_name}...")
        found = find_bank_url(bank_name)
        if found:
            target_url = found
        else:
            return None, "URLなし", ""

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")

    try:
        driver = webdriver.Chrome(
            service=ChromeService(ChromeDriverManager().install()), options=options
        )
        driver.set_page_load_timeout(30)
        driver.get(target_url)
        time.sleep(3)
        body = driver.find_element("tag name", "body").text
        driver.quit()

        json_text = ask_gemini_to_extract(body)
        return json_text, "Success", target_url
    except Exception as e:
        return None, f"Error: {str(e)}", target_url


# --- ヘルパー関数: JSON抽出の強化版 ---
def extract_json_from_text(text):
    """
    AIの返答からJSON部分（{...}）だけを無理やり抜き出す
    """
    try:
        # 最初の "{" から 最後の "}" までを探す
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            json_str = match.group(0)
            return json.loads(json_str)
        else:
            return None
    except:
        return None


# --- メイン処理 ---

df, worksheet = get_google_sheet_data()

if df is not None and df.empty:
    df = pd.DataFrame(
        {
            "金融機関名": FULL_BANK_LIST,
            "WebサイトURL": [""] * len(FULL_BANK_LIST),
            "電話番号": [""] * len(FULL_BANK_LIST),
            "受付時間": [""] * len(FULL_BANK_LIST),
            "手続き方法": [""] * len(FULL_BANK_LIST),
            "AI要約": ["未取得"] * len(FULL_BANK_LIST),
            "最終更新": ["-"] * len(FULL_BANK_LIST),
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
                url = row["WebサイトURL"] if "WebサイトURL" in df.columns else ""

                status_text.text(f"処理中: {bank} ...")

                res_json_text, status, final_url = process_single_bank(bank, url)

                # URL更新
                if final_url:
                    df.at[i, "WebサイトURL"] = final_url

                # データ更新処理
                if status == "Success" and res_json_text:
                    # ★強力なJSON抽出を使用
                    data = extract_json_from_text(res_json_text)

                    if data:
                        df.at[i, "電話番号"] = data.get("phone", "不明")
                        df.at[i, "受付時間"] = data.get("hours", "不明")
                        df.at[i, "手続き方法"] = data.get("method", "不明")
                        df.at[i, "AI要約"] = data.get("summary", "抽出成功")
                    else:
                        # 失敗したら生テキストを入れる（デバッグ用）
                        df.at[i, "AI要約"] = f"解析失敗: {res_json_text[:50]}..."

                # 日時更新
                import datetime

                df.at[i, "最終更新"] = datetime.datetime.now().strftime(
                    "%Y-%m-%d %H:%M"
                )

                # 1行ごとに保存
                save_to_google_sheet(worksheet, df)
                bar.progress((i + 1) / total)

            status_text.success("完了！画面を更新します...")
            time.sleep(1)
            st.rerun()  # ★ここで強制的に再読み込み

with col2:
    if st.button("⚠️ 銀行リストを初期化・再読込"):
        new_df = pd.DataFrame(
            {
                "金融機関名": FULL_BANK_LIST,
                "WebサイトURL": [""] * len(FULL_BANK_LIST),
                "電話番号": [""] * len(FULL_BANK_LIST),
                "受付時間": [""] * len(FULL_BANK_LIST),
                "手続き方法": [""] * len(FULL_BANK_LIST),
                "AI要約": ["未取得"] * len(FULL_BANK_LIST),
                "最終更新": ["-"] * len(FULL_BANK_LIST),
            }
        )
        save_to_google_sheet(worksheet, new_df)
        st.warning("リストを初期化しました。")
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
