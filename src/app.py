import json
import os
import time

import google.generativeai as genai

# --- Google Sheets Libraries ---
import gspread
import pandas as pd
import streamlit as st
from dotenv import load_dotenv  # ローカル開発用
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
# ★設定エリア (Cloud & Local Hybrid)
# ============================================================

# 1. APIキーの読み込み (Streamlit CloudのSecrets または .env)
# クラウド上のSecretsを優先し、なければローカルの.envを見る
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

# スプレッドシートのURL (Secretsから取得推奨だが、今はコードに書いてもOK)
# ★ここにSTEP1で作ったスプレッドシートのURLを入れてください
SHEET_URL = "https://docs.google.com/spreadsheets/d/1kQJ7j6jgs0RqS1IRvrdyuNseZ9GKgov5YXiDq-vawCc/edit?gid=0#gid=0"
if "SHEET_URL" in st.secrets:
    SHEET_URL = st.secrets["SHEET_URL"]


def get_google_sheet_data():
    """Googleスプレッドシートに接続してDFを返す"""
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    # Secretsから認証情報を取得 (Streamlit Cloud用)
    if "gcp_service_account" in st.secrets:
        creds_dict = st.secrets["gcp_service_account"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    # ローカル開発用 (JSONファイルを直接指定)
    else:
        # ★ダウンロードしたJSONファイルの名前を書いてください
        json_file = "service_account.json"
        if os.path.exists(json_file):
            creds = ServiceAccountCredentials.from_json_keyfile_name(json_file, scope)
        else:
            st.error("認証ファイルが見つかりません")
            return None, None

    client = gspread.authorize(creds)
    try:
        sheet = client.open_by_url(SHEET_URL)
        worksheet = sheet.get_worksheet(0)  # 1枚目のシート
        data = worksheet.get_all_values()

        if not data:  # 空っぽの場合
            return pd.DataFrame(), worksheet

        headers = data.pop(0)
        df = pd.DataFrame(data, columns=headers)
        return df, worksheet
    except Exception as e:
        st.error(f"スプレッドシート接続エラー: {e}")
        return None, None


def save_to_google_sheet(worksheet, df):
    """データフレームをスプレッドシートに保存"""
    worksheet.clear()  # 一度クリア
    set_with_dataframe(worksheet, df)  # 書き込み


# ============================================================
# アプリケーション本体
# ============================================================

st.set_page_config(page_title="銀行マスタ管理 Cloud", layout="wide")
st.title("🏦 銀行手続き完全自動化システム (Cloud版)")


# --- 1. URL検索 & AI解析 ---
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

    # ★クラウド用Selenium設定（必須）
    options = Options()
    options.add_argument("--headless")  # 画面なし
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


# --- メイン処理 ---

# データ読み込み
df, worksheet = get_google_sheet_data()

# 初回起動などでシートが空の場合の初期化
if df is not None and df.empty:
    sample_banks = ["三菱UFJ銀行", "ゆうちょ銀行", "三井住友銀行"]
    df = pd.DataFrame(
        {
            "金融機関名": sample_banks,
            "WebサイトURL": [""] * len(sample_banks),
            "電話番号": [""] * len(sample_banks),
            "受付時間": [""] * len(sample_banks),
            "手続き方法": [""] * len(sample_banks),
            "AI要約": ["未取得"] * len(sample_banks),
            "最終更新": ["-"] * len(sample_banks),
        }
    )
    save_to_google_sheet(worksheet, df)

# UI: 自動収集
st.markdown("### 🚀 一括自動収集")
if st.button("全銀行更新 (Cloud)", type="primary"):
    if df is not None:
        total = len(df)
        bar = st.progress(0)
        for i, row in df.iterrows():
            bank = row["金融機関名"]
            url = row["WebサイトURL"] if "WebサイトURL" in df.columns else ""

            res_json, status, final_url = process_single_bank(bank, url)

            if final_url:
                df.at[i, "WebサイトURL"] = final_url
            if status == "Success" and res_json:
                try:
                    cleaned = res_json.replace("```json", "").replace("```", "").strip()
                    data = json.loads(cleaned)
                    df.at[i, "電話番号"] = data.get("phone", "")
                    df.at[i, "受付時間"] = data.get("hours", "")
                    df.at[i, "手続き方法"] = data.get("method", "")
                    df.at[i, "AI要約"] = data.get("summary", "")
                    import datetime

                    df.at[i, "最終更新"] = datetime.datetime.now().strftime(
                        "%Y-%m-%d %H:%M"
                    )
                except:
                    pass

            # 1行ごとに保存（クラウド環境でのデータ消失防止）
            save_to_google_sheet(worksheet, df)
            bar.progress((i + 1) / total)
        st.success("完了！")

# UI: データ確認
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
