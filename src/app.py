import streamlit as st
import pandas as pd
import os
import time
import json
import re
import shutil
from dotenv import load_dotenv
import google.generativeai as genai
from google.api_core import exceptions
from duckduckgo_search import DDGS

# --- JavaScript実行用ライブラリ ---
import streamlit.components.v1 as components

# --- Google Sheets Libraries ---
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

# --- Selenium Setup ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
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

# ★指定のモデルに変更
MODEL_CANDIDATES = [
    "models/gemini-2.5-flash-lite",
    "models/gemini-2.5-flash",
    # 万が一2.5がまだAPIで未解禁の場合の予備として既存も残すか、
    # 完全に統一する場合は上記2つのみにしてください。一旦指定通りにします。
]
current_key_index = 0

def configure_genai():
    global current_key_index
    if API_KEYS and current_key_index < len(API_KEYS):
        genai.configure(api_key=API_KEYS[current_key_index])

configure_genai()

def generate_ultimate_rotation(prompt):
    global current_key_index
    if not API_KEYS: return "エラー: APIキーなし"
    
    # 全キー × 全モデルで試行
    for _ in range(len(API_KEYS)):
        for model_name in MODEL_CANDIDATES:
            try:
                model = genai.GenerativeModel(model_name)
                response = model.generate_content(prompt)
                return response.text
            except Exception:
                continue # 次のモデルへ
        
        # キーローテーション
        current_key_index = (current_key_index + 1) % len(API_KEYS)
        configure_genai()
        
    return "エラー: 全モデル・全キーで生成失敗"

# ============================================================
# ★ Google Sheets & Data Logic
# ============================================================

SHEET_URL = "https://docs.google.com/spreadsheets/d/xxxxxxxx/edit" 
if "SHEET_URL" in st.secrets:
    SHEET_URL = st.secrets["SHEET_URL"]

@st.cache_data(ttl=60)
def get_google_sheet_data_cached():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
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
        if not data: return pd.DataFrame(), worksheet
        headers = data.pop(0)
        df = pd.DataFrame(data, columns=headers)
        return df, worksheet
    except: return None, None

def get_worksheet_object():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    if "gcp_service_account" in st.secrets:
        creds_dict = st.secrets["gcp_service_account"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    else:
        json_file = "service_account.json" 
        if os.path.exists(json_file):
            creds = ServiceAccountCredentials.from_json_keyfile_name(json_file, scope)
        else: return None
    client = gspread.authorize(creds)
    try:
        sheet = client.open_by_url(SHEET_URL)
        return sheet.get_worksheet(0)
    except: return None

def save_to_google_sheet(worksheet, df):
    try:
        worksheet.clear()
        set_with_dataframe(worksheet, df)
    except Exception as e:
        st.warning(f"保存エラー(スキップ): {e}")

# ============================================================
# スクレイピング & AI解析ロジック (自動修復機能付き)
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
    "みずほ信託銀行": "https://www.mizuho-tb.co.jp/souzoku/tetsuzuki/"
}

def search_new_url(bank_name):
    """DuckDuckGoで新しいURLを探す"""
    try:
        query = f"{bank_name} 相続手続き"
        results = DDGS().text(query, max_results=1)
        if results: return results[0]['href']
    except: return None
    return None

def ask_gemini_to_extract(html_text):
    prompt = f"""
    以下のHTMLから銀行情報を抽出し、必ず以下のJSON形式のみを出力してください。
    Markdown装飾は不要です。
    {{
        "phone": "電話番号", "hours": "受付時間",
        "method": "手続き方法", "summary": "要約(注意点など)"
    }}
    HTML: {html_text[:30000]} 
    """
    return generate_ultimate_rotation(prompt)

def extract_json_from_text(text):
    try:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match: return json.loads(match.group(0))
    except: pass
    return None

def run_selenium_and_extract(target_url):
    """指定URLにアクセスして情報を抽出する処理（共通化）"""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

    try:
        chromium_path = shutil.which("chromium")
        chromedriver_path = shutil.which("chromedriver")
        if chromium_path and chromedriver_path:
            options.binary_location = chromium_path
            service = Service(executable_path=chromedriver_path)
        else:
            service = Service(ChromeDriverManager().install())

        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(60)
        driver.get(target_url)
        time.sleep(5)
        body = driver.find_element("tag name", "body").text
        driver.quit()
        
        json_text = ask_gemini_to_extract(body)
        return json_text, "Success"
    except Exception as e:
        return None, f"Error: {str(e)}"

def process_single_bank(bank_name, current_url):
    """
    銀行処理のメインロジック：
    1. 既存URLがあればトライ
    2. 失敗 or 空なら検索してトライ（自動修復）
    """
    
    # URL決定ロジック
    target_url = current_url
    if not target_url or pd.isna(target_url):
        if bank_name in BANK_MASTER_DB:
            target_url = BANK_MASTER_DB[bank_name]
    
    # 1回目のトライ（URLがある場合）
    if target_url:
        st.write(f"   Using: {target_url}")
        res_json, status = run_selenium_and_extract(target_url)
        
        # 成功してJSONも取れたら終了
        data = extract_json_from_text(res_json)
        if status == "Success" and data:
            return res_json, "Success", target_url
            
    # ここに来る＝URLがない、または1回目が失敗した
    st.write("   ⚠️ 情報取得失敗。URLを検索してリトライします...")
    
    # 新しいURLを探す
    found_url = search_new_url(bank_name)
    if not found_url:
        return None, "検索失敗", target_url
        
    st.write(f"   🔍 発見: {found_url}")
    
    # 2回目のトライ（検索したURLで）
    res_json, status = run_selenium_and_extract(found_url)
    return res_json, status, found_url # 成功しても失敗してもこの結果を返す

# --- 便利なJS機能: チャット入力欄にフォーカスを当てる ---
def focus_chat_input():
    js = f"""
    <script>
        function setFocus() {{
            const doc = window.parent.document;
            const textareas = doc.querySelectorAll('textarea[data-testid="stChatInputTextArea"]');
            if (textareas.length > 0) {{
                textareas[0].focus();
            }}
        }}
        setTimeout(setFocus, 300);
    </script>
    """
    components.html(js, height=0, width=0)

# ============================================================
# ★ アプリケーション本体 (Page構成)
# ============================================================

st.set_page_config(page_title="銀行手続システム", layout="wide")

page = st.sidebar.radio("メニュー選択", ["🤖 AIアシスタント (実務用)", "📝 マスタ管理・更新 (管理者用)"])

df, _ = get_google_sheet_data_cached()
worksheet = get_worksheet_object()

# ------------------------------------------------------------
# PAGE 1: AIアシスタント (Chat Interface)
# ------------------------------------------------------------
if page == "🤖 AIアシスタント (実務用)":
    st.title("🤖 銀行手続 AIコンシェルジュ")
    st.info("「三菱UFJの手続きはどうすればいい？」「〇〇銀行に電話する時の台本を作って」などと話しかけてください。")
    focus_chat_input()

    if "messages" not in st.session_state:
        st.session_state.messages = []

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if prompt := st.chat_input("何でも聞いてください..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("データベースを確認して回答を作成中..."):
                relevant_info = ""
                found_bank = None
                
                if df is not None:
                    for bank in df["金融機関名"].tolist():
                        if bank in prompt:
                            row = df[df["金融機関名"] == bank].iloc[0]
                            relevant_info = f"""
                            【{bank} の登録データ】
                            - 電話番号: {row['電話番号']}
                            - 受付時間: {row['受付時間']}
                            - 手続方法: {row['手続き方法']}
                            - AI要約: {row['AI要約']}
                            - Webサイト: {row['WebサイトURL']}
                            """
                            found_bank = bank
                            break
                
                system_prompt = f"""
                あなたは行政書士事務所の優秀なアシスタントAIです。
                以下の「データベース情報」をもとに、ユーザーの質問に具体的に答えてください。
                
                --- データベース情報 ---
                {relevant_info if relevant_info else "（該当データなし。一般知識で回答してください。）"}
                
                --- ユーザーの質問 ---
                {prompt}
                """
                
                response_text = generate_ultimate_rotation(system_prompt)
                st.markdown(response_text)
                
                if found_bank and relevant_info:
                    row = df[df["金融機関名"] == found_bank].iloc[0]
                    if row['WebサイトURL']:
                        st.link_button(f"🔗 {found_bank}のWebサイトを開く", row['WebサイトURL'])

        st.session_state.messages.append({"role": "assistant", "content": response_text})


# ------------------------------------------------------------
# PAGE 2: マスタ管理 (Grid & Scraping)
# ------------------------------------------------------------
elif page == "📝 マスタ管理・更新 (管理者用)":
    st.title("📝 銀行マスタ管理画面")
    st.markdown("ここで情報の閲覧・修正・一括更新を行います。")

    if df is not None and df.empty:
        bank_names = list(BANK_MASTER_DB.keys())
        init_urls = [BANK_MASTER_DB[name] for name in bank_names]
        df = pd.DataFrame({
            "金融機関名": bank_names, "WebサイトURL": init_urls,
            "電話番号": [""]*len(bank_names), "受付時間": [""]*len(bank_names),
            "手続き方法": [""]*len(bank_names), "AI要約": ["未取得"]*len(bank_names),
            "最終更新": ["-"]*len(bank_names)
        })
        if worksheet:
            save_to_google_sheet(worksheet, df)
            st.cache_data.clear()
            st.rerun()

    with st.expander("🚀 データ一括更新パネル（管理者のみ操作）"):
        st.warning("⚠️ 全銀行の情報を更新するには時間がかかります（情報が見つからない場合、自動で検索してリトライします）。")
        col1, col2 = st.columns([2, 1])
        with col1:
            if st.button("全銀行更新 (Cloud)", type="primary"):
                if df is not None and worksheet is not None:
                    total = len(df)
                    bar = st.progress(0)
                    status = st.empty()
                    for i, row in df.iterrows():
                        bank = row['金融機関名']
                        url = row['WebサイトURL'] if 'WebサイトURL' in df.columns else ""
                        status.text(f"処理中: {bank}")
                        
                        # ★ここが改善点：結果だけでなく、最終的に採用したURLも返ってくる
                        res_json, stat, final_url = process_single_bank(bank, url)
                        
                        # URLが変わっていれば更新
                        if final_url: df.at[i, 'WebサイトURL'] = final_url
                        
                        if stat == "Success" and res_json:
                            d = extract_json_from_text(res_json)
                            if d:
                                df.at[i, '電話番号'] = d.get("phone", "")
                                df.at[i, '受付時間'] = d.get("hours", "")
                                df.at[i, '手続き方法'] = d.get("method", "")
                                df.at[i, 'AI要約'] = d.get("summary", "")
                            else: df.at[i, 'AI要約'] = "Parse Error"
                        elif stat != "Success":
                            df.at[i, 'AI要約'] = f"Error: {stat}"
                        
                        import datetime
                        df.at[i, '最終更新'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                        
                        if (i+1)%3==0 or (i+1)==total:
                            save_to_google_sheet(worksheet, df)
                            status.text("Saving...")
                            time.sleep(2)
                        bar.progress((i+1)/total)
                    status.success("完了")
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()

        with col2:
            if st.button("⚠️ リスト初期化"):
                names = list(BANK_MASTER_DB.keys())
                new_df = pd.DataFrame({
                    "金融機関名": names, "WebサイトURL": [BANK_MASTER_DB[n] for n in names],
                    "電話番号": [""]*len(names), "受付時間": [""]*len(names),
                    "手続き方法": [""]*len(names), "AI要約": ["未取得"]*len(names),
                    "最終更新": ["-"]*len(names)
                })
                if worksheet:
                    save_to_google_sheet(worksheet, new_df)
                    st.cache_data.clear()
                    st.warning("初期化しました")
                    time.sleep(1)
                    st.rerun()

    st.markdown("---")
    st.subheader("🔍 データベース閲覧")
    
    if df is not None:
        st.info("👇 行をクリックすると、下に詳細が表示されます。")
        cfg_view = {
            "WebサイトURL": st.column_config.LinkColumn("URL", display_text="Link"),
            "AI要約": st.column_config.TextColumn("AI要約", width="medium"),
        }
        event = st.dataframe(
            df, column_config=cfg_view, use_container_width=True, height=300,
            on_select="rerun", selection_mode="single-row", hide_index=True
        )
        
        if len(event.selection.rows) > 0:
            selected_index = event.selection.rows[0]
            selected_row = df.iloc[selected_index]
            
            st.markdown(f"### 🏦 {selected_row['金融機関名']} の詳細情報")
            with st.container(border=True):
                c1, c2 = st.columns(2)
                with c1:
                    st.text_input("📞 電話番号", value=selected_row['電話番号'], disabled=True)
                    st.text_input("⏰ 受付時間", value=selected_row['受付時間'], disabled=True)
                with c2:
                    st.text_area("📝 手続き方法", value=selected_row['手続き方法'], height=108, disabled=True)
                
                st.text_area("🤖 AIによる要約・注意点", value=selected_row['AI要約'], height=200, disabled=True)
                if selected_row['WebサイトURL']:
                    st.link_button("👉 Webサイトを開く", selected_row['WebサイトURL'])
        else:
            st.caption("（上の表から銀行を選択してください）")

        st.markdown("<br>", unsafe_allow_html=True)
        with st.expander("🛠️ データを手動で修正・保存する"):
            st.markdown("データを修正したい場合は、以下の表を直接編集して「保存」を押してください。")
            edited_df = st.data_editor(
                df, column_config={"WebサイトURL": st.column_config.LinkColumn("URL")}, 
                num_rows="dynamic", key="editor"
            )
            if st.button("💾 手動変更を保存"):
                if worksheet:
                    save_to_google_sheet(worksheet, edited_df)
                    st.cache_data.clear()
                    st.success("スプレッドシートに保存しました！")
                    time.sleep(1)
                    st.rerun()import streamlit as st
import pandas as pd
import os
import time
import json
import re
import shutil
from dotenv import load_dotenv
import google.generativeai as genai
from google.api_core import exceptions
from duckduckgo_search import DDGS

# --- JavaScript実行用ライブラリ ---
import streamlit.components.v1 as components

# --- Google Sheets Libraries ---
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

# --- Selenium Setup ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
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

# ★指定のモデルに変更
MODEL_CANDIDATES = [
    "models/gemini-2.5-flash-lite",
    "models/gemini-2.5-flash",
    # 万が一2.5がまだAPIで未解禁の場合の予備として既存も残すか、
    # 完全に統一する場合は上記2つのみにしてください。一旦指定通りにします。
]
current_key_index = 0

def configure_genai():
    global current_key_index
    if API_KEYS and current_key_index < len(API_KEYS):
        genai.configure(api_key=API_KEYS[current_key_index])

configure_genai()

def generate_ultimate_rotation(prompt):
    global current_key_index
    if not API_KEYS: return "エラー: APIキーなし"
    
    # 全キー × 全モデルで試行
    for _ in range(len(API_KEYS)):
        for model_name in MODEL_CANDIDATES:
            try:
                model = genai.GenerativeModel(model_name)
                response = model.generate_content(prompt)
                return response.text
            except Exception:
                continue # 次のモデルへ
        
        # キーローテーション
        current_key_index = (current_key_index + 1) % len(API_KEYS)
        configure_genai()
        
    return "エラー: 全モデル・全キーで生成失敗"

# ============================================================
# ★ Google Sheets & Data Logic
# ============================================================

SHEET_URL = "https://docs.google.com/spreadsheets/d/xxxxxxxx/edit" 
if "SHEET_URL" in st.secrets:
    SHEET_URL = st.secrets["SHEET_URL"]

@st.cache_data(ttl=60)
def get_google_sheet_data_cached():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
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
        if not data: return pd.DataFrame(), worksheet
        headers = data.pop(0)
        df = pd.DataFrame(data, columns=headers)
        return df, worksheet
    except: return None, None

def get_worksheet_object():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    if "gcp_service_account" in st.secrets:
        creds_dict = st.secrets["gcp_service_account"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    else:
        json_file = "service_account.json" 
        if os.path.exists(json_file):
            creds = ServiceAccountCredentials.from_json_keyfile_name(json_file, scope)
        else: return None
    client = gspread.authorize(creds)
    try:
        sheet = client.open_by_url(SHEET_URL)
        return sheet.get_worksheet(0)
    except: return None

def save_to_google_sheet(worksheet, df):
    try:
        worksheet.clear()
        set_with_dataframe(worksheet, df)
    except Exception as e:
        st.warning(f"保存エラー(スキップ): {e}")

# ============================================================
# スクレイピング & AI解析ロジック (自動修復機能付き)
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
    "みずほ信託銀行": "https://www.mizuho-tb.co.jp/souzoku/tetsuzuki/"
}

def search_new_url(bank_name):
    """DuckDuckGoで新しいURLを探す"""
    try:
        query = f"{bank_name} 相続手続き"
        results = DDGS().text(query, max_results=1)
        if results: return results[0]['href']
    except: return None
    return None

def ask_gemini_to_extract(html_text):
    prompt = f"""
    以下のHTMLから銀行情報を抽出し、必ず以下のJSON形式のみを出力してください。
    Markdown装飾は不要です。
    {{
        "phone": "電話番号", "hours": "受付時間",
        "method": "手続き方法", "summary": "要約(注意点など)"
    }}
    HTML: {html_text[:30000]} 
    """
    return generate_ultimate_rotation(prompt)

def extract_json_from_text(text):
    try:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match: return json.loads(match.group(0))
    except: pass
    return None

def run_selenium_and_extract(target_url):
    """指定URLにアクセスして情報を抽出する処理（共通化）"""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

    try:
        chromium_path = shutil.which("chromium")
        chromedriver_path = shutil.which("chromedriver")
        if chromium_path and chromedriver_path:
            options.binary_location = chromium_path
            service = Service(executable_path=chromedriver_path)
        else:
            service = Service(ChromeDriverManager().install())

        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(60)
        driver.get(target_url)
        time.sleep(5)
        body = driver.find_element("tag name", "body").text
        driver.quit()
        
        json_text = ask_gemini_to_extract(body)
        return json_text, "Success"
    except Exception as e:
        return None, f"Error: {str(e)}"

def process_single_bank(bank_name, current_url):
    """
    銀行処理のメインロジック：
    1. 既存URLがあればトライ
    2. 失敗 or 空なら検索してトライ（自動修復）
    """
    
    # URL決定ロジック
    target_url = current_url
    if not target_url or pd.isna(target_url):
        if bank_name in BANK_MASTER_DB:
            target_url = BANK_MASTER_DB[bank_name]
    
    # 1回目のトライ（URLがある場合）
    if target_url:
        st.write(f"   Using: {target_url}")
        res_json, status = run_selenium_and_extract(target_url)
        
        # 成功してJSONも取れたら終了
        data = extract_json_from_text(res_json)
        if status == "Success" and data:
            return res_json, "Success", target_url
            
    # ここに来る＝URLがない、または1回目が失敗した
    st.write("   ⚠️ 情報取得失敗。URLを検索してリトライします...")
    
    # 新しいURLを探す
    found_url = search_new_url(bank_name)
    if not found_url:
        return None, "検索失敗", target_url
        
    st.write(f"   🔍 発見: {found_url}")
    
    # 2回目のトライ（検索したURLで）
    res_json, status = run_selenium_and_extract(found_url)
    return res_json, status, found_url # 成功しても失敗してもこの結果を返す

# --- 便利なJS機能: チャット入力欄にフォーカスを当てる ---
def focus_chat_input():
    js = f"""
    <script>
        function setFocus() {{
            const doc = window.parent.document;
            const textareas = doc.querySelectorAll('textarea[data-testid="stChatInputTextArea"]');
            if (textareas.length > 0) {{
                textareas[0].focus();
            }}
        }}
        setTimeout(setFocus, 300);
    </script>
    """
    components.html(js, height=0, width=0)

# ============================================================
# ★ アプリケーション本体 (Page構成)
# ============================================================

st.set_page_config(page_title="銀行手続システム", layout="wide")

page = st.sidebar.radio("メニュー選択", ["🤖 AIアシスタント (実務用)", "📝 マスタ管理・更新 (管理者用)"])

df, _ = get_google_sheet_data_cached()
worksheet = get_worksheet_object()

# ------------------------------------------------------------
# PAGE 1: AIアシスタント (Chat Interface)
# ------------------------------------------------------------
if page == "🤖 AIアシスタント (実務用)":
    st.title("🤖 銀行手続 AIコンシェルジュ")
    st.info("「三菱UFJの手続きはどうすればいい？」「〇〇銀行に電話する時の台本を作って」などと話しかけてください。")
    focus_chat_input()

    if "messages" not in st.session_state:
        st.session_state.messages = []

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if prompt := st.chat_input("何でも聞いてください..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("データベースを確認して回答を作成中..."):
                relevant_info = ""
                found_bank = None
                
                if df is not None:
                    for bank in df["金融機関名"].tolist():
                        if bank in prompt:
                            row = df[df["金融機関名"] == bank].iloc[0]
                            relevant_info = f"""
                            【{bank} の登録データ】
                            - 電話番号: {row['電話番号']}
                            - 受付時間: {row['受付時間']}
                            - 手続方法: {row['手続き方法']}
                            - AI要約: {row['AI要約']}
                            - Webサイト: {row['WebサイトURL']}
                            """
                            found_bank = bank
                            break
                
                system_prompt = f"""
                あなたは行政書士事務所の優秀なアシスタントAIです。
                以下の「データベース情報」をもとに、ユーザーの質問に具体的に答えてください。
                
                --- データベース情報 ---
                {relevant_info if relevant_info else "（該当データなし。一般知識で回答してください。）"}
                
                --- ユーザーの質問 ---
                {prompt}
                """
                
                response_text = generate_ultimate_rotation(system_prompt)
                st.markdown(response_text)
                
                if found_bank and relevant_info:
                    row = df[df["金融機関名"] == found_bank].iloc[0]
                    if row['WebサイトURL']:
                        st.link_button(f"🔗 {found_bank}のWebサイトを開く", row['WebサイトURL'])

        st.session_state.messages.append({"role": "assistant", "content": response_text})


# ------------------------------------------------------------
# PAGE 2: マスタ管理 (Grid & Scraping)
# ------------------------------------------------------------
elif page == "📝 マスタ管理・更新 (管理者用)":
    st.title("📝 銀行マスタ管理画面")
    st.markdown("ここで情報の閲覧・修正・一括更新を行います。")

    if df is not None and df.empty:
        bank_names = list(BANK_MASTER_DB.keys())
        init_urls = [BANK_MASTER_DB[name] for name in bank_names]
        df = pd.DataFrame({
            "金融機関名": bank_names, "WebサイトURL": init_urls,
            "電話番号": [""]*len(bank_names), "受付時間": [""]*len(bank_names),
            "手続き方法": [""]*len(bank_names), "AI要約": ["未取得"]*len(bank_names),
            "最終更新": ["-"]*len(bank_names)
        })
        if worksheet:
            save_to_google_sheet(worksheet, df)
            st.cache_data.clear()
            st.rerun()

    with st.expander("🚀 データ一括更新パネル（管理者のみ操作）"):
        st.warning("⚠️ 全銀行の情報を更新するには時間がかかります（情報が見つからない場合、自動で検索してリトライします）。")
        col1, col2 = st.columns([2, 1])
        with col1:
            if st.button("全銀行更新 (Cloud)", type="primary"):
                if df is not None and worksheet is not None:
                    total = len(df)
                    bar = st.progress(0)
                    status = st.empty()
                    for i, row in df.iterrows():
                        bank = row['金融機関名']
                        url = row['WebサイトURL'] if 'WebサイトURL' in df.columns else ""
                        status.text(f"処理中: {bank}")
                        
                        # ★ここが改善点：結果だけでなく、最終的に採用したURLも返ってくる
                        res_json, stat, final_url = process_single_bank(bank, url)
                        
                        # URLが変わっていれば更新
                        if final_url: df.at[i, 'WebサイトURL'] = final_url
                        
                        if stat == "Success" and res_json:
                            d = extract_json_from_text(res_json)
                            if d:
                                df.at[i, '電話番号'] = d.get("phone", "")
                                df.at[i, '受付時間'] = d.get("hours", "")
                                df.at[i, '手続き方法'] = d.get("method", "")
                                df.at[i, 'AI要約'] = d.get("summary", "")
                            else: df.at[i, 'AI要約'] = "Parse Error"
                        elif stat != "Success":
                            df.at[i, 'AI要約'] = f"Error: {stat}"
                        
                        import datetime
                        df.at[i, '最終更新'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                        
                        if (i+1)%3==0 or (i+1)==total:
                            save_to_google_sheet(worksheet, df)
                            status.text("Saving...")
                            time.sleep(2)
                        bar.progress((i+1)/total)
                    status.success("完了")
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()

        with col2:
            if st.button("⚠️ リスト初期化"):
                names = list(BANK_MASTER_DB.keys())
                new_df = pd.DataFrame({
                    "金融機関名": names, "WebサイトURL": [BANK_MASTER_DB[n] for n in names],
                    "電話番号": [""]*len(names), "受付時間": [""]*len(names),
                    "手続き方法": [""]*len(names), "AI要約": ["未取得"]*len(names),
                    "最終更新": ["-"]*len(names)
                })
                if worksheet:
                    save_to_google_sheet(worksheet, new_df)
                    st.cache_data.clear()
                    st.warning("初期化しました")
                    time.sleep(1)
                    st.rerun()

    st.markdown("---")
    st.subheader("🔍 データベース閲覧")
    
    if df is not None:
        st.info("👇 行をクリックすると、下に詳細が表示されます。")
        cfg_view = {
            "WebサイトURL": st.column_config.LinkColumn("URL", display_text="Link"),
            "AI要約": st.column_config.TextColumn("AI要約", width="medium"),
        }
        event = st.dataframe(
            df, column_config=cfg_view, use_container_width=True, height=300,
            on_select="rerun", selection_mode="single-row", hide_index=True
        )
        
        if len(event.selection.rows) > 0:
            selected_index = event.selection.rows[0]
            selected_row = df.iloc[selected_index]
            
            st.markdown(f"### 🏦 {selected_row['金融機関名']} の詳細情報")
            with st.container(border=True):
                c1, c2 = st.columns(2)
                with c1:
                    st.text_input("📞 電話番号", value=selected_row['電話番号'], disabled=True)
                    st.text_input("⏰ 受付時間", value=selected_row['受付時間'], disabled=True)
                with c2:
                    st.text_area("📝 手続き方法", value=selected_row['手続き方法'], height=108, disabled=True)
                
                st.text_area("🤖 AIによる要約・注意点", value=selected_row['AI要約'], height=200, disabled=True)
                if selected_row['WebサイトURL']:
                    st.link_button("👉 Webサイトを開く", selected_row['WebサイトURL'])
        else:
            st.caption("（上の表から銀行を選択してください）")

        st.markdown("<br>", unsafe_allow_html=True)
        with st.expander("🛠️ データを手動で修正・保存する"):
            st.markdown("データを修正したい場合は、以下の表を直接編集して「保存」を押してください。")
            edited_df = st.data_editor(
                df, column_config={"WebサイトURL": st.column_config.LinkColumn("URL")}, 
                num_rows="dynamic", key="editor"
            )
            if st.button("💾 手動変更を保存"):
                if worksheet:
                    save_to_google_sheet(worksheet, edited_df)
                    st.cache_data.clear()
                    st.success("スプレッドシートに保存しました！")
                    time.sleep(1)
                    st.rerun()