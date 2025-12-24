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


# ★チャット用（検索必須）
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


# ★管理画面用（URL優先更新）
def update_bank_data_smart(bank_name, existing_url):
    # 1. 既存URLがある場合はそれを優先（検索ブロック回避）
    target_url = existing_url
    if not target_url or pd.isna(target_url) or target_url == "":
        # URLがなければマスタから補完
        if bank_name in BANK_MASTER_DB:
            target_url = BANK_MASTER_DB[bank_name]

    # 2. URLがあればSelenium実行
    if target_url:
        st.write(f"   → サイト解析: {target_url}")
        res_json, status = run_selenium_and_extract(target_url)
        data = extract_json_from_text(res_json)
        if status == "Success" and data:
            return {
                "金融機関名": bank_name,
                "WebサイトURL": target_url,
                "電話番号": data.get("contact_phone", ""),
                "凍結方法": data.get("freeze_method", ""),
                "残高証明": data.get("balance_cert", ""),
                "取引明細": data.get("transaction_history", ""),
                "解約手続": data.get("cancellation", ""),
                "投信国債": data.get("investment", ""),
                "貸金庫": data.get("safe_deposit", ""),
                "AI要約": data.get("summary", ""),
                "最終更新": "一括更新",
            }, "Success"

    # 3. URLがない、または失敗した場合は検索へ
    st.write("   → URL不明/失敗のため検索中...")
    return fetch_bank_data_dynamic(bank_name)


def fetch_specific_detail(bank_name, topic):
    try:
        query = f"{bank_name} 相続 {topic}"
        results = DDGS().text(query, max_results=3)
        if not results:
            return "情報が見つかりませんでした。"
        snippet_text = "\n".join([f"- {r.get('body', '')}" for r in results])
        prompt = f"""
        行政書士のアシスタントとして、以下の検索結果から
        「{bank_name}」の「{topic}」に関する手続き方法を簡潔にまとめてください。
        箇条書きで、実務に必要な情報だけを抽出してください。
        --- 検索結果 ---
        {snippet_text}
        """
        return generate_ultimate_rotation(prompt)
    except Exception as e:
        return f"調査中にエラーが発生しました: {str(e)}"


# JavaScript Hooks
def focus_search_input():
    js = """<script>
    function setFocus() {
        const doc = window.parent.document;
        const inputs = doc.querySelectorAll('input[type="text"]');
        if (inputs.length > 0) { inputs[0].focus(); }
    }
    setTimeout(setFocus, 300);
    </script>"""
    components.html(js, height=0, width=0)


def scroll_to_results():
    js = """<script>
    function scrollToResult() {
        const doc = window.parent.document;
        const element = doc.getElementById("result_anchor");
        if (element) { element.scrollIntoView({behavior: "smooth", block: "start"}); }
    }
    setTimeout(scrollToResult, 500);
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
# PAGE 1: AIアシスタント (ダッシュボード型)
# ------------------------------------------------------------
if page == "🤖 AIアシスタント (実務用)":
    st.title("🤖 銀行手続 AIコンシェルジュ")
    st.info(
        "「三菱UFJ」「みずほ銀行」など入力してください。なお、ufjなど部分的な言葉でもOKです。"
    )

    if "current_bank_data" not in st.session_state:
        st.session_state.current_bank_data = None
    if "candidate_list" not in st.session_state:
        st.session_state.candidate_list = None
    if "display_result" not in st.session_state:
        st.session_state.display_result = ""
    if "display_title" not in st.session_state:
        st.session_state.display_title = ""

    def select_bank(bank_name_arg):
        if df is not None:
            found_row = df[df["金融機関名"] == bank_name_arg]
            if not found_row.empty:
                data = found_row.iloc[0].to_dict()
                st.session_state.current_bank_data = data
                st.session_state.candidate_list = None
                st.session_state.display_title = f"✅ {bank_name_arg} を選択中"
                st.session_state.display_result = (
                    "下のボタンから詳細を選択してください。"
                )
                return

        with st.spinner(f"{bank_name_arg} をWeb調査中..."):
            data, status = fetch_bank_data_dynamic(bank_name_arg)
            if status in ["Success", "Fallback"] and data:
                st.session_state.current_bank_data = data
                st.session_state.candidate_list = None
                st.session_state.display_title = f"🎉 {bank_name_arg} (Web調査)"
                st.session_state.display_result = (
                    "下のボタンから詳細を選択してください。"
                )
            else:
                st.session_state.display_title = "❌ エラー"
                st.session_state.display_result = "情報が見つかりませんでした。"

    def handle_input(user_text):
        found_candidates = []
        full_match_found = False
        if df is not None:
            all_banks = df["金融機関名"].tolist()
            if user_text in all_banks:
                found_candidates = [user_text]
                full_match_found = True

            if not full_match_found:
                search_key = (
                    user_text.replace("手続き", "")
                    .replace("教えて", "")
                    .replace("銀行", "")
                    .strip()
                )
                for bank in all_banks:
                    if (bank in user_text) or (
                        len(search_key) > 1 and search_key.lower() in bank.lower()
                    ):
                        found_candidates.append(bank)
                found_candidates = list(set(found_candidates))

        if len(found_candidates) == 1:
            select_bank(found_candidates[0])
        elif len(found_candidates) > 1:
            st.session_state.candidate_list = found_candidates
            st.session_state.current_bank_data = None
            st.session_state.display_title = "🤔 複数の候補があります"
            st.session_state.display_result = "上のリストから選択してください。"
        else:
            select_bank(user_text)

    # UI
    st.write("▼ **銀行を検索・選択**")
    search_query = st.text_input("🔍 銀行名を入力 (Enterで検索)", key="main_search_bar")
    focus_search_input()

    visible_banks = []
    if df is not None:
        all_banks = df["金融機関名"].tolist()
        if search_query:
            s_key = search_query.strip().lower()
            visible_banks = [b for b in all_banks if s_key in b.lower()]
        else:
            visible_banks = all_banks

    if visible_banks:
        with st.container(height=200):
            cols = st.columns(4)
            for idx, b_name in enumerate(visible_banks):
                if cols[idx % 4].button(
                    b_name, key=f"nav_{idx}", use_container_width=True
                ):
                    select_bank(b_name)
                    st.rerun()

    if search_query and not st.session_state.candidate_list:
        if (
            not st.session_state.current_bank_data
            or st.session_state.current_bank_data["金融機関名"] != search_query
        ):
            if not visible_banks:
                handle_input(search_query)
                st.rerun()

    if st.session_state.candidate_list:
        st.info("👇 以下の候補から選択してください")
        cands = st.session_state.candidate_list
        c_cols = st.columns(4)
        for idx, cand in enumerate(cands):
            if c_cols[idx % 4].button(
                cand, key=f"cand_{cand}", use_container_width=True
            ):
                select_bank(cand)
                st.rerun()

    st.markdown("---")
    st.markdown('<div id="result_anchor"></div>', unsafe_allow_html=True)

    if st.session_state.current_bank_data:
        scroll_to_results()
        data = st.session_state.current_bank_data
        st.subheader(f"🏦 {data['金融機関名']}")

        b1, b2, b3, b4 = st.columns(4)
        b5, b6, b7, b8 = st.columns(4)

        target_topic = None
        topic_label = ""
        if b1.button("📞 連絡先", use_container_width=True):
            target_topic = "電話番号"
            topic_label = "相続センター電話番号"
        if b2.button("🧊 凍結手続", use_container_width=True):
            target_topic = "凍結方法"
            topic_label = "口座凍結の手続き"
        if b3.button("📄 残高証明", use_container_width=True):
            target_topic = "残高証明"
            topic_label = "残高証明書の発行"
        if b4.button("📊 取引明細", use_container_width=True):
            target_topic = "取引明細"
            topic_label = "取引推移証明書の発行"
        if b5.button("🚪 解約手続", use_container_width=True):
            target_topic = "解約手続"
            topic_label = "相続預金の解約手続"
        if b6.button("📈 投信国債", use_container_width=True):
            target_topic = "投信国債"
            topic_label = "投資信託・国債の相続"
        if b7.button("🔐 貸金庫", use_container_width=True):
            target_topic = "貸金庫"
            topic_label = "貸金庫の相続手続"
        if b8.button("💡 全て表示", use_container_width=True):
            target_topic = "ALL"

        if target_topic:
            if target_topic == "ALL":
                st.session_state.display_title = "💡 全情報"
                st.session_state.display_result = f"**📞 連絡先**: {data.get('電話番号', '')}\n**🧊 凍結**: {data.get('凍結方法', '')}\n**📄 残高証明**: {data.get('残高証明', '')}\n**📊 取引明細**: {data.get('取引明細', '')}\n**🚪 解約**: {data.get('解約手続', '')}\n**📈 投信**: {data.get('投信国債', '')}\n**🔐 貸金庫**: {data.get('貸金庫', '')}\n**💡 要約**: {data.get('AI要約', '')}"
            else:
                content = data.get(target_topic, "")
                if not content or content in ["", "記載なし", "不明"]:
                    st.session_state.display_title = f"🔍 {topic_label} (Web調査中...)"
                    with st.spinner(f"Webで「{topic_label}」を再調査しています..."):
                        fetched_info = fetch_specific_detail(
                            data["金融機関名"], topic_label
                        )
                        st.session_state.display_result = fetched_info
                        st.session_state.display_title = f"✅ {topic_label} (Web取得)"
                else:
                    st.session_state.display_title = f"✅ {topic_label}"
                    st.session_state.display_result = content
            st.rerun()

        if st.session_state.display_result:
            with st.container(border=True):
                st.markdown(f"#### {st.session_state.display_title}")
                st.markdown(st.session_state.display_result)
        if data.get("WebサイトURL"):
            st.link_button("🔗 公式サイトを開く", data["WebサイトURL"])
    else:
        st.info("👆 上のリストから銀行を選択するか、検索してください。")

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
                    status.text(f"調査中: {bank} ...")

                    # ★ここが重要：URLがある場合はそれを優先する「smart」関数を使用
                    res_data, stat = update_bank_data_smart(bank, row["WebサイトURL"])

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

                save_to_google_sheet(worksheet, df)
                status.success("完了！")
                st.cache_data.clear()
                time.sleep(1)
                st.rerun()

    if df is not None:
        cfg = {
            "WebサイトURL": st.column_config.LinkColumn("URL"),
            "電話番号": st.column_config.TextColumn("電話", width="medium"),
            "AI要約": st.column_config.TextColumn("要約", width="medium"),
        }
        st.dataframe(df, column_config=cfg, use_container_width=True, height=300)
