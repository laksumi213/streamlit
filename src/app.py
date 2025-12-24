import json
import os
import random
import re
import shutil
import time

import google.generativeai as genai
import gspread
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv
from duckduckgo_search import DDGS
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
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

# ★指定のモデル構成に統一
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

    for _ in range(len(API_KEYS)):
        for model_name in MODEL_CANDIDATES:
            try:
                model = genai.GenerativeModel(model_name)
                response = model.generate_content(prompt)
                return response.text
            except Exception:
                continue
        current_key_index = (current_key_index + 1) % len(API_KEYS)
        configure_genai()
    return "エラー: 生成失敗"


# ============================================================
# ★ Google Sheets
# ============================================================

SHEET_URL = "https://docs.google.com/spreadsheets/d/xxxxxxxx/edit"
if "SHEET_URL" in st.secrets:
    SHEET_URL = st.secrets["SHEET_URL"]


@st.cache_data(ttl=60)
def get_google_sheet_data_cached():
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
    except:
        return None, None


def get_worksheet_object():
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
            return None
    client = gspread.authorize(creds)
    try:
        sheet = client.open_by_url(SHEET_URL)
        return sheet.get_worksheet(0)
    except:
        return None


def save_to_google_sheet(worksheet, df):
    try:
        worksheet.clear()
        set_with_dataframe(worksheet, df)
    except Exception as e:
        st.warning(f"保存スキップ: {e}")


# ============================================================
# ★ 調査・解析ロジック
# ============================================================

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

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


def search_new_url_with_snippet(bank_name):
    try:
        query = f"{bank_name} 相続手続き"
        results = DDGS().text(query, max_results=3)
        if results:
            top_url = results[0]["href"]
            combined_snippet = "\n".join(
                [f"- {r.get('title', '')}: {r.get('body', '')}" for r in results]
            )
            return top_url, combined_snippet
    except:
        return None, None
    return None, None


def ask_gemini_to_extract_7points(text_data, is_html=True):
    data_type = "HTML" if is_html else "テキスト"
    prompt = f"""
    行政書士の実務アシスタントとして、{data_type}から相続手続きの「7つの重要項目」を抽出してください。
    JSON形式で出力し、情報がない場合は「記載なし」としてください。
    {{
        "contact_phone": "電話番号", "freeze_method": "凍結連絡方法",
        "balance_cert": "残高証明申請", "transaction_history": "取引明細申請",
        "cancellation": "解約手続", "investment": "投信・国債手続",
        "safe_deposit": "貸金庫手続", "summary": "その他要約"
    }}
    --- データ ---
    {text_data[:30000]} 
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


def run_selenium_and_extract(target_url):
    sleep_time = random.uniform(3, 6)
    time.sleep(sleep_time)
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")
    try:
        chromium_path = shutil.which("chromium")
        chromedriver_path = shutil.which("chromedriver")
        if chromium_path and chromedriver_path:
            options.binary_location = chromium_path
            service = Service(executable_path=chromedriver_path)
        else:
            service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        driver.set_page_load_timeout(45)
        try:
            driver.get(target_url)
            time.sleep(4)
            body_text = driver.find_element("tag name", "body").text
        except:
            driver.quit()
            return None, "Access Error"
        driver.quit()
        json_text = ask_gemini_to_extract_7points(body_text, is_html=True)
        return json_text, "Success"
    except Exception as e:
        return None, f"Error: {str(e)}"


def fetch_bank_data_dynamic(bank_name):
    found_url, snippet = search_new_url_with_snippet(bank_name)
    if not found_url:
        return None, "検索失敗"
    res_json, status = run_selenium_and_extract(found_url)
    data = extract_json_from_text(res_json)
    if status == "Success" and data:
        return {
            "金融機関名": bank_name,
            "WebサイトURL": found_url,
            "電話番号": data.get("contact_phone", ""),
            "凍結方法": data.get("freeze_method", ""),
            "残高証明": data.get("balance_cert", ""),
            "取引明細": data.get("transaction_history", ""),
            "解約手続": data.get("cancellation", ""),
            "投信国債": data.get("investment", ""),
            "貸金庫": data.get("safe_deposit", ""),
            "AI要約": data.get("summary", ""),
            "最終更新": "自動取得(Live)",
        }, "Success"
    elif snippet:
        res_fb = ask_gemini_to_extract_7points(snippet, is_html=False)
        data_fb = extract_json_from_text(res_fb)
        if data_fb:
            return {
                "金融機関名": bank_name,
                "WebサイトURL": found_url,
                "電話番号": data_fb.get("contact_phone", ""),
                "凍結方法": data_fb.get("freeze_method", ""),
                "残高証明": data_fb.get("balance_cert", ""),
                "取引明細": data_fb.get("transaction_history", ""),
                "解約手続": data_fb.get("cancellation", ""),
                "投信国債": data_fb.get("investment", ""),
                "貸金庫": data_fb.get("safe_deposit", ""),
                "AI要約": data_fb.get("summary", "") + "(検索推測)",
                "最終更新": "自動取得(Fallback)",
            }, "Fallback"
    return None, "失敗"


def focus_chat_input():
    js = """<script>
    function setFocus() {
        const doc = window.parent.document;
        const textareas = doc.querySelectorAll('textarea[data-testid="stChatInputTextArea"]');
        if (textareas.length > 0) { textareas[0].focus(); }
    }
    setTimeout(setFocus, 300);
    </script>"""
    components.html(js, height=0, width=0)


# ============================================================
# ★ App Main
# ============================================================

st.set_page_config(page_title="銀行手続システム", layout="wide")
page = st.sidebar.radio(
    "メニュー選択", ["🤖 AIアシスタント (実務用)", "📝 マスタ管理・更新 (管理者用)"]
)

df, _ = get_google_sheet_data_cached()
worksheet = get_worksheet_object()

# ------------------------------------------------------------
# PAGE 1: AIアシスタント
# ------------------------------------------------------------
if page == "🤖 AIアシスタント (実務用)":
    st.title("🤖 銀行手続 AIコンシェルジュ")
    # ★ご指定のメッセージに変更
    st.info(
        "「三菱UFJ」「みずほ銀行」など入力してください。なお、ufjなど部分的な言葉でもOKがです。"
    )
    focus_chat_input()

    # --- Session State 初期化 ---
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "current_bank_data" not in st.session_state:
        st.session_state.current_bank_data = None
    if "candidate_list" not in st.session_state:
        st.session_state.candidate_list = None  # 複数候補のリスト

    # --- 入力処理ロジック ---
    def handle_input(user_text):
        st.session_state.messages.append({"role": "user", "content": user_text})

        search_key = (
            user_text.replace("手続き", "")
            .replace("教えて", "")
            .replace("銀行", "")
            .strip()
        )
        found_candidates = []
        if df is not None:
            for bank in df["金融機関名"].tolist():
                if (bank in user_text) or (
                    len(search_key) > 1 and search_key.lower() in bank.lower()
                ):
                    found_candidates.append(bank)
        found_candidates = list(set(found_candidates))

        if len(found_candidates) == 1:
            bank_name = found_candidates[0]
            data = df[df["金融機関名"] == bank_name].iloc[0].to_dict()
            st.session_state.current_bank_data = data
            st.session_state.candidate_list = None
            msg = f"✅ **{bank_name}** が見つかりました。\n下のボタンから知りたい項目を選んでください。"
            st.session_state.messages.append({"role": "assistant", "content": msg})

        elif len(found_candidates) > 1:
            st.session_state.candidate_list = found_candidates
            st.session_state.current_bank_data = None
            msg = f"🤔 **「{search_key}」** に一致する銀行が複数あります。下から選択してください。"
            st.session_state.messages.append({"role": "assistant", "content": msg})

        else:
            st.session_state.candidate_list = None
            msg_searching = f"🕵️ **{search_key or user_text}** をWeb調査中..."
            st.session_state.messages.append(
                {"role": "assistant", "content": msg_searching}
            )

            with st.spinner("AIが調査中... (しばらくお待ちください)"):
                data, status = fetch_bank_data_dynamic(search_key or user_text)

            if status in ["Success", "Fallback"] and data:
                st.session_state.current_bank_data = data
                msg_done = (
                    f"🎉 **{data['金融機関名']}** の情報を取得しました（{status}）。"
                )
                st.session_state.messages.append(
                    {"role": "assistant", "content": msg_done}
                )
            else:
                st.session_state.messages.append(
                    {"role": "assistant", "content": "🙏 情報が見つかりませんでした。"}
                )
                st.session_state.current_bank_data = None

    # --- 画面描画 ---
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # 候補ボタン (消えないように配置)
    if st.session_state.candidate_list:
        st.markdown("---")
        st.markdown("##### 🔍 候補を選択してください")
        cands = st.session_state.candidate_list
        cols = st.columns(min(len(cands), 4))
        for idx, cand in enumerate(cands):
            if cols[idx % 4].button(
                cand, key=f"btn_cand_{cand}", use_container_width=True
            ):
                handle_input(cand)
                st.rerun()

    # 7項目ボタン
    if st.session_state.current_bank_data and not st.session_state.candidate_list:
        data = st.session_state.current_bank_data
        st.markdown("---")
        st.markdown(f"##### 👇 **{data['金融機関名']}** の詳細メニュー")

        b1, b2, b3, b4 = st.columns(4)
        b5, b6, b7, b8 = st.columns(4)

        if b1.button("📞 連絡先", use_container_width=True):
            st.session_state.messages.append(
                {"role": "assistant", "content": f"**📞 連絡先**\n{data['電話番号']}"}
            )
            st.rerun()
        if b2.button("🧊 凍結手続", use_container_width=True):
            st.session_state.messages.append(
                {"role": "assistant", "content": f"**🧊 凍結手続**\n{data['凍結方法']}"}
            )
            st.rerun()
        if b3.button("📄 残高証明", use_container_width=True):
            st.session_state.messages.append(
                {"role": "assistant", "content": f"**📄 残高証明**\n{data['残高証明']}"}
            )
            st.rerun()
        if b4.button("📊 取引明細", use_container_width=True):
            st.session_state.messages.append(
                {"role": "assistant", "content": f"**📊 取引明細**\n{data['取引明細']}"}
            )
            st.rerun()
        if b5.button("🚪 解約手続", use_container_width=True):
            st.session_state.messages.append(
                {"role": "assistant", "content": f"**🚪 解約手続**\n{data['解約手続']}"}
            )
            st.rerun()
        if b6.button("📈 投信国債", use_container_width=True):
            st.session_state.messages.append(
                {"role": "assistant", "content": f"**📈 投信国債**\n{data['投信国債']}"}
            )
            st.rerun()
        if b7.button("🔐 貸金庫", use_container_width=True):
            st.session_state.messages.append(
                {"role": "assistant", "content": f"**🔐 貸金庫**\n{data['貸金庫']}"}
            )
            st.rerun()
        if b8.button("💡 全て表示", use_container_width=True):
            full_msg = f"### {data['金融機関名']} 全情報\n**📞**: {data['電話番号']}\n**🧊**: {data['凍結方法']}\n**📄**: {data['残高証明']}\n**🚪**: {data['解約手続']}\n**💡**: {data['AI要約']}"
            st.session_state.messages.append({"role": "assistant", "content": full_msg})
            st.rerun()

    # 入力欄
    if prompt := st.chat_input("銀行名を入力..."):
        handle_input(prompt)
        st.rerun()

# ------------------------------------------------------------
# PAGE 2: マスタ管理
# ------------------------------------------------------------
elif page == "📝 マスタ管理・更新 (管理者用)":
    st.title("📝 銀行マスタ管理画面")
    COLS = [
        "金融機関名",
        "WebサイトURL",
        "電話番号",
        "凍結方法",
        "残高証明",
        "取引明細",
        "解約手続",
        "投信国債",
        "貸金庫",
        "AI要約",
        "最終更新",
    ]
    if df is not None and (df.empty or "凍結方法" not in df.columns):
        bank_names = list(BANK_MASTER_DB.keys())
        init_urls = [BANK_MASTER_DB[name] for name in bank_names]
        df = pd.DataFrame(columns=COLS)
        df["金融機関名"] = bank_names
        df["WebサイトURL"] = init_urls
        df = df.fillna("")
        if worksheet:
            save_to_google_sheet(worksheet, df)
            st.cache_data.clear()
            st.rerun()

    with st.expander("🚀 データ一括更新パネル"):
        if st.button("全銀行更新 (Cloud)", type="primary"):
            if df is not None and worksheet is not None:
                total = len(df)
                bar = st.progress(0)
                status = st.empty()
                for i, row in df.iterrows():
                    bank = row["金融機関名"]
                    status.text(f"調査中: {bank}")
                    # 管理画面用の処理
                    # process_single_bank 相当のロジックを実行 (簡易化のためfetchを使用)
                    res_data, stat = fetch_bank_data_dynamic(bank)
                    if stat in ["Success", "Fallback"] and res_data:
                        for k in COLS:
                            if k in res_data:
                                df.at[i, k] = res_data[k]
                    import datetime

                    df.at[i, "最終更新"] = datetime.datetime.now().strftime(
                        "%Y-%m-%d %H:%M"
                    )
                    if (i + 1) % 3 == 0:
                        save_to_google_sheet(worksheet, df)
                        time.sleep(1)
                    bar.progress((i + 1) / total)
                status.success("完了")
                st.cache_data.clear()
                st.rerun()

    if df is not None:
        cfg = {
            "WebサイトURL": st.column_config.LinkColumn("URL"),
            "電話番号": st.column_config.TextColumn("電話", width="medium"),
            "AI要約": st.column_config.TextColumn("要約", width="medium"),
        }
        st.dataframe(df, column_config=cfg, use_container_width=True, height=300)
