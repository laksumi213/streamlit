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
    "models/gemini-2.0-flash-exp",
    "models/gemini-1.5-flash",
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
# ★ 7項目特化型スクレイピング & AI解析
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
    あなたは行政書士の実務アシスタントです。
    以下の{data_type}から、相続手続きに関する**「実務で必要な具体的情報」**のみを抽出してください。
    必ず以下のJSON形式で出力してください。情報がない場合は「記載なし」としてください。
    
    {{
        "contact_phone": "相続専用ダイヤル・連絡先の電話番号",
        "freeze_method": "凍結連絡の方法（電話/Web/来店など）",
        "balance_cert": "残高証明書の申請方法・必要書類",
        "transaction_history": "取引推移証明書（明細）の申請方法",
        "cancellation": "解約（払戻）の手続き方法",
        "investment": "投資信託・国債・公共債の手続き",
        "safe_deposit": "貸金庫の手続き（開扉・解約など）",
        "summary": "上記以外の重要な注意点（Web予約必須など）"
    }}
    --- 対象データ ---
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
    sleep_time = random.uniform(5, 8)
    time.sleep(sleep_time)

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
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
        driver.set_page_load_timeout(60)

        try:
            driver.get(target_url)
            time.sleep(5)
            body_text = driver.find_element("tag name", "body").text
        except:
            driver.quit()
            return None, "Access Error"

        driver.quit()
        json_text = ask_gemini_to_extract_7points(body_text, is_html=True)
        return json_text, "Success"

    except Exception as e:
        return None, f"Error: {str(e)}"


def process_single_bank(bank_name, current_url):
    target_url = current_url
    if not target_url or pd.isna(target_url):
        if bank_name in BANK_MASTER_DB:
            target_url = BANK_MASTER_DB[bank_name]

    if target_url:
        st.write(f"   Trying: {target_url}")
        res_json, status = run_selenium_and_extract(target_url)
        data = extract_json_from_text(res_json)
        if status == "Success" and data:
            return res_json, "Success", target_url

    st.write("   ⚠️ サイト不可。検索スニペットから抽出...")
    found_url, snippet_text = search_new_url_with_snippet(bank_name)
    if not snippet_text:
        return None, "完全失敗", target_url

    final_url = found_url if found_url else target_url
    res_json = ask_gemini_to_extract_7points(snippet_text, is_html=False)
    return res_json, "SnippetFallback", final_url


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
# PAGE 1: AIアシスタント (あいまい検索・候補選択版)
# ------------------------------------------------------------
if page == "🤖 AIアシスタント (実務用)":
    st.title("🤖 銀行手続 AIコンシェルジュ")
    st.info("「ufj」「みずほ」のように短く入力してもOKです。")
    focus_chat_input()

    if "messages" not in st.session_state:
        st.session_state.messages = []

    # チャット履歴表示
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # ユーザー入力
    if prompt := st.chat_input("例: ufj、ゆうちょ"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            found_candidates = []

            # 検索ロジック (1): 登録名がプロンプトに含まれる場合 (例: "三菱UFJ銀行の手続き" -> "三菱UFJ銀行")
            # 検索ロジック (2): プロンプトが登録名に含まれる場合 (例: "ufj" -> "三菱UFJ銀行")
            # 検索ロジック (3): 余計な言葉("手続き"等)を除外して検索

            search_key = (
                prompt.replace("手続き", "")
                .replace("教えて", "")
                .replace("銀行", "")
                .strip()
            )

            if df is not None:
                bank_list = df["金融機関名"].tolist()
                for bank in bank_list:
                    # 完全一致 or 包含関係のチェック (大文字小文字区別なし)
                    if (bank in prompt) or (
                        len(search_key) > 1 and search_key.lower() in bank.lower()
                    ):
                        found_candidates.append(bank)

            # 重複除去
            found_candidates = list(set(found_candidates))

            # --- 分岐処理 ---

            # パターンA: 候補が1つだけ見つかった場合 -> 即回答
            if len(found_candidates) == 1:
                bank_name_hit = found_candidates[0]
                found_bank_data = df[df["金融機関名"] == bank_name_hit].iloc[0]

                response_text = f"""
### 【{bank_name_hit}】 相続手続き情報
*(最終確認: {found_bank_data.get("最終更新", "-")})*

**1. 📞 相続連絡先**
{found_bank_data.get("電話番号", "記載なし")}

**2. 🧊 凍結連絡**
{found_bank_data.get("凍結方法", "記載なし")}

**3. 📄 残高証明書**
{found_bank_data.get("残高証明", "記載なし")}

**4. 📊 取引明細**
{found_bank_data.get("取引明細", "記載なし")}

**5. 🚪 解約手続き**
{found_bank_data.get("解約手続", "記載なし")}

**6. 📈 投資信託・国債**
{found_bank_data.get("投信国債", "記載なし")}

**7. 🔐 貸金庫**
{found_bank_data.get("貸金庫", "記載なし")}

---
**💡 その他・要約**
{found_bank_data.get("AI要約", "なし")}
                """
                st.markdown(response_text)
                if found_bank_data["WebサイトURL"]:
                    st.link_button(
                        f"🔗 {bank_name_hit} 公式サイトへ",
                        found_bank_data["WebサイトURL"],
                    )

                st.session_state.messages.append(
                    {"role": "assistant", "content": response_text}
                )

            # パターンB: 候補が複数見つかった場合 -> ボタンで選ばせる
            elif len(found_candidates) > 1:
                st.markdown(
                    f"**「{search_key}」** に一致する銀行が複数見つかりました。どちらですか？"
                )

                # ボタンを並べる
                cols = st.columns(min(len(found_candidates), 3))  # 最大3列で表示
                for idx, candidate in enumerate(found_candidates):
                    # ボタンが押されたら、その銀行名で再検索させるような挙動にする
                    if cols[idx % 3].button(candidate, key=f"btn_{candidate}"):
                        # ユーザーがその銀行名を発言したことにする（履歴に追加）
                        st.session_state.messages.append(
                            {"role": "user", "content": candidate}
                        )
                        st.rerun()  # 画面をリロードして、パターンAのロジックを通す

                # 履歴には「候補を提示しました」と残す
                st.session_state.messages.append(
                    {
                        "role": "assistant",
                        "content": "複数の候補が見つかりました。ボタンから選択してください。",
                    }
                )

            # パターンC: 見つからなかった場合 -> 一般AI回答
            else:
                with st.spinner("データ未登録のため、一般的な知識で回答します..."):
                    fallback_prompt = f"行政書士として、{prompt} に関する一般的な相続手続きの流れを簡潔に教えてください。"
                    response_text = generate_ultimate_rotation(fallback_prompt)
                    st.markdown(response_text)
                    st.session_state.messages.append(
                        {"role": "assistant", "content": response_text}
                    )

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
            st.warning("DB構造更新完了")
            time.sleep(1)
            st.rerun()

    with st.expander("🚀 データ一括更新パネル（管理者のみ操作）"):
        st.info(
            "💡 7項目（凍結・残高・明細・解約・投信・貸金庫・電話）を重点的に抽出します。"
        )
        col1, col2 = st.columns([2, 1])
        with col1:
            if st.button("全銀行更新 (Cloud)", type="primary"):
                if df is not None and worksheet is not None:
                    total = len(df)
                    bar = st.progress(0)
                    status = st.empty()
                    for i, row in df.iterrows():
                        bank = row["金融機関名"]
                        url = row["WebサイトURL"]
                        status.text(f"調査中: {bank}")
                        res_json, stat, final_url = process_single_bank(bank, url)
                        if final_url:
                            df.at[i, "WebサイトURL"] = final_url
                        if res_json:
                            d = extract_json_from_text(res_json)
                            if d:
                                df.at[i, "電話番号"] = d.get("contact_phone", "")
                                df.at[i, "凍結方法"] = d.get("freeze_method", "")
                                df.at[i, "残高証明"] = d.get("balance_cert", "")
                                df.at[i, "取引明細"] = d.get("transaction_history", "")
                                df.at[i, "解約手続"] = d.get("cancellation", "")
                                df.at[i, "投信国債"] = d.get("investment", "")
                                df.at[i, "貸金庫"] = d.get("safe_deposit", "")
                                df.at[i, "AI要約"] = d.get("summary", "")
                            else:
                                df.at[i, "AI要約"] = "解析エラー"
                        import datetime

                        df.at[i, "最終更新"] = datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M"
                        )
                        if (i + 1) % 3 == 0 or (i + 1) == total:
                            save_to_google_sheet(worksheet, df)
                            status.text("Saving...")
                            time.sleep(2)
                        bar.progress((i + 1) / total)
                    status.success("完了")
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()

        with col2:
            if st.button("⚠️ リスト初期化"):
                bank_names = list(BANK_MASTER_DB.keys())
                init_urls = [BANK_MASTER_DB[name] for name in bank_names]
                df = pd.DataFrame(columns=COLS)
                df["金融機関名"] = bank_names
                df["WebサイトURL"] = init_urls
                df = df.fillna("")
                if worksheet:
                    save_to_google_sheet(worksheet, df)
                    st.cache_data.clear()
                    st.warning("初期化しました")
                    time.sleep(1)
                    st.rerun()

    st.markdown("---")
    st.subheader("🔍 データベース閲覧")

    if df is not None:
        cfg_view = {
            "WebサイトURL": st.column_config.LinkColumn("URL", display_text="Link"),
            "電話番号": st.column_config.TextColumn("📞 電話", width="medium"),
            "凍結方法": st.column_config.TextColumn("🧊 凍結", width="medium"),
            "AI要約": st.column_config.TextColumn("要約", width="medium"),
        }
        event = st.dataframe(
            df,
            column_config=cfg_view,
            use_container_width=True,
            height=300,
            on_select="rerun",
            selection_mode="single-row",
            hide_index=True,
        )

        if len(event.selection.rows) > 0:
            idx = event.selection.rows[0]
            row = df.iloc[idx]
            st.markdown(f"### 🏦 {row['金融機関名']} 詳細")
            with st.container(border=True):
                c1, c2 = st.columns(2)
                with c1:
                    st.info(f"**📞 連絡先**: {row['電話番号']}")
                    st.write(f"**🧊 凍結連絡**: {row['凍結方法']}")
                    st.write(f"**📄 残高証明**: {row['残高証明']}")
                    st.write(f"**📊 取引明細**: {row['取引明細']}")
                with c2:
                    st.write(f"**🚪 解約手続**: {row['解約手続']}")
                    st.write(f"**📈 投信国債**: {row['投信国債']}")
                    st.write(f"**🔐 貸金庫**: {row['貸金庫']}")
                    st.warning(f"**💡 その他**: {row['AI要約']}")
                if row["WebサイトURL"]:
                    st.link_button("公式サイト", row["WebサイトURL"])
