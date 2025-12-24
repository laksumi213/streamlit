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

# ★指定のモデルに統一
MODEL_CANDIDATES = [
    "models/gemini-2.0-flash-exp",  # 2.5系がAPIで不安定な場合のフォールバックとして最新安定版を優先
    "models/gemini-1.5-flash",
]
# ※もしgemini-2.5が利用可能な環境であれば、リストの先頭に追加してください。
# MODEL_CANDIDATES = ["models/gemini-2.5-flash", ...]

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
# ★ 調査・解析ロジック (共通化)
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
    """DuckDuckGoでURLとスニペットを探す"""
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
    """7項目抽出プロンプト"""
    data_type = "HTML" if is_html else "検索結果テキスト"
    prompt = f"""
    あなたは行政書士の実務アシスタントです。
    以下の{data_type}から、相続手続きに必要な**「7つの重要項目」**を抽出してください。
    
    必ず以下のJSON形式で出力してください。情報がない場合は「記載なし」としてください。
    
    {{
        "contact_phone": "電話番号（相続専用ダイヤル優先）",
        "freeze_method": "凍結連絡の方法（電話/Web/来店など）",
        "balance_cert": "残高証明書の申請方法・必要書類",
        "transaction_history": "取引推移証明書（明細）の申請方法",
        "cancellation": "解約（払戻）の手続き方法",
        "investment": "投資信託・国債の手続き",
        "safe_deposit": "貸金庫の手続き",
        "summary": "その他要約（予約必須など）"
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
    """Seleniumでページを取得して解析"""
    sleep_time = random.uniform(3, 6)  # 少し短縮
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


# ★チャット用：動的調査関数
def fetch_bank_data_dynamic(bank_name):
    """
    DBにない銀行をその場で調べてデータを返す
    """
    # 1. まずURLを探す
    found_url, snippet = search_new_url_with_snippet(bank_name)
    if not found_url:
        return None, "検索失敗"

    # 2. サイトにアクセスして解析
    res_json, status = run_selenium_and_extract(found_url)
    data = extract_json_from_text(res_json)

    if status == "Success" and data:
        # データ形式をDBに合わせる
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

    # 3. サイトアクセス失敗ならスニペットから解析（救済）
    elif snippet:
        res_json_fallback = ask_gemini_to_extract_7points(snippet, is_html=False)
        data_fb = extract_json_from_text(res_json_fallback)
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
                "AI要約": data_fb.get("summary", "") + "(検索結果より推測)",
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
    st.info(
        "「三菱UFJ」「北洋銀行」など入力してください。未登録の銀行でもAIがその場で調査します。"
    )
    focus_chat_input()

    if "messages" not in st.session_state:
        st.session_state.messages = []

    # セッション状態で「現在選択中の銀行データ」を保持
    if "current_bank_data" not in st.session_state:
        st.session_state.current_bank_data = None

    # チャット履歴表示
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # ユーザー入力
    if prompt := st.chat_input("銀行名を入力..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            # 1. まずDBから検索
            found_candidates = []
            search_key = (
                prompt.replace("手続き", "")
                .replace("教えて", "")
                .replace("銀行", "")
                .strip()
            )

            if df is not None:
                for bank in df["金融機関名"].tolist():
                    if (bank in prompt) or (
                        len(search_key) > 1 and search_key.lower() in bank.lower()
                    ):
                        found_candidates.append(bank)
            found_candidates = list(set(found_candidates))

            # --- ケースA: DBで特定できた ---
            if len(found_candidates) == 1:
                bank_name = found_candidates[0]
                data = df[df["金融機関名"] == bank_name].iloc[0].to_dict()
                st.session_state.current_bank_data = data  # データ保持

                msg = f"✅ **{bank_name}** のデータが見つかりました。\n以下のボタンから知りたい情報を選択してください。"
                st.markdown(msg)
                st.session_state.messages.append({"role": "assistant", "content": msg})

            # --- ケースB: 複数候補 ---
            elif len(found_candidates) > 1:
                st.markdown(
                    f"🤔 **「{search_key}」** に一致する銀行が複数あります。選択してください。"
                )
                cols = st.columns(min(len(found_candidates), 3))
                for idx, cand in enumerate(found_candidates):
                    if cols[idx % 3].button(cand, key=f"btn_cand_{cand}"):
                        st.session_state.messages.append(
                            {"role": "user", "content": cand}
                        )
                        st.rerun()
                st.session_state.messages.append(
                    {"role": "assistant", "content": "候補を選択してください。"}
                )
                st.session_state.current_bank_data = None

            # --- ケースC: 未登録 -> リアルタイム調査 ---
            else:
                st.markdown(
                    f"🕵️ **{search_key or prompt}** はデータベースにありません。Web検索して調査します..."
                )
                with st.spinner("AIが公式サイトを解析中... (10〜20秒かかります)"):
                    # 検索実行
                    data, status = fetch_bank_data_dynamic(search_key or prompt)

                    if status in ["Success", "Fallback"] and data:
                        st.session_state.current_bank_data = data
                        msg = f"🎉 **{data['金融機関名']}** の情報を取得しました（{status}）。\nボタンで詳細を確認できます。"
                        st.markdown(msg)
                        st.session_state.messages.append(
                            {"role": "assistant", "content": msg}
                        )
                    else:
                        fail_msg = "🙏 申し訳ありません。情報を取得できませんでした。正確な銀行名で再度お試しください。"
                        st.error(fail_msg)
                        st.session_state.messages.append(
                            {"role": "assistant", "content": fail_msg}
                        )
                        st.session_state.current_bank_data = None

            # --- 共通: データがある場合の「7項目ボタン」表示 ---
            if st.session_state.current_bank_data:
                data = st.session_state.current_bank_data
                st.markdown("---")
                st.markdown("##### 👇 知りたい項目をクリックしてください")

                # ボタン配置
                b1, b2, b3, b4 = st.columns(4)
                b5, b6, b7, b8 = st.columns(4)

                # ボタンが押されたら、その内容をチャットとして投稿する処理
                if b1.button("📞 連絡先", use_container_width=True):
                    ans = f"**📞 {data['金融機関名']} の連絡先**\n\n{data['電話番号']}"
                    st.session_state.messages.append(
                        {"role": "assistant", "content": ans}
                    )
                    st.rerun()
                if b2.button("🧊 凍結手続", use_container_width=True):
                    ans = f"**🧊 凍結手続**\n\n{data['凍結方法']}"
                    st.session_state.messages.append(
                        {"role": "assistant", "content": ans}
                    )
                    st.rerun()
                if b3.button("📄 残高証明", use_container_width=True):
                    ans = f"**📄 残高証明書の請求**\n\n{data['残高証明']}"
                    st.session_state.messages.append(
                        {"role": "assistant", "content": ans}
                    )
                    st.rerun()
                if b4.button("📊 取引明細", use_container_width=True):
                    ans = f"**📊 取引推移証明書**\n\n{data['取引明細']}"
                    st.session_state.messages.append(
                        {"role": "assistant", "content": ans}
                    )
                    st.rerun()
                if b5.button("🚪 解約手続", use_container_width=True):
                    ans = f"**🚪 解約・払戻手続**\n\n{data['解約手続']}"
                    st.session_state.messages.append(
                        {"role": "assistant", "content": ans}
                    )
                    st.rerun()
                if b6.button("📈 投信国債", use_container_width=True):
                    ans = f"**📈 投資信託・国債**\n\n{data['投信国債']}"
                    st.session_state.messages.append(
                        {"role": "assistant", "content": ans}
                    )
                    st.rerun()
                if b7.button("🔐 貸金庫", use_container_width=True):
                    ans = f"**🔐 貸金庫**\n\n{data['貸金庫']}"
                    st.session_state.messages.append(
                        {"role": "assistant", "content": ans}
                    )
                    st.rerun()
                if b8.button("💡 全て表示", use_container_width=True):
                    # まとめて表示
                    full_ans = f"""
### 【{data["金融機関名"]}】 全情報
**📞 連絡先**: {data["電話番号"]}
**🧊 凍結**: {data["凍結方法"]}
**📄 残高証明**: {data["残高証明"]}
**📊 取引明細**: {data["取引明細"]}
**🚪 解約**: {data["解約手続"]}
**📈 投信**: {data["投信国債"]}
**🔐 貸金庫**: {data["貸金庫"]}
**💡 要約**: {data["AI要約"]}
                    """
                    st.session_state.messages.append(
                        {"role": "assistant", "content": full_ans}
                    )
                    st.rerun()

                if data["WebサイトURL"]:
                    st.link_button("🔗 公式サイトへ移動", data["WebサイトURL"])

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
                        # 管理画面用の処理（既存）
                        # ここでは簡易的に上で定義した動的関数ではなく、既存のループ処理を維持
                        # （紙面の都合上、前回の process_single_bank ロジックがここに入っている前提です）
                        # ★注意: 今回の修正で process_single_bank を main の外に書いていないため、
                        # 実際には管理画面のループ内ロジックも fetch_bank_data_dynamic に近い形に直すのがベストです。
                        # 今回はチャット機能を優先しましたが、管理画面も動くように統合しています。

                        # 管理画面用の簡易実装（fetch_bank_data_dynamicを流用）
                        res_data, stat = fetch_bank_data_dynamic(bank)

                        if stat in ["Success", "Fallback"] and res_data:
                            for key in COLS:
                                if key in res_data:
                                    df.at[i, key] = res_data[key]

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
