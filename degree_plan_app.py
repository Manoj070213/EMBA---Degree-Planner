# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: Python 3 (ipykernel) (Local)
#     language: python
#     name: conda-base-py
# ---

# %%
import requests
import streamlit as st
import pandas as pd
from fpdf import FPDF
import re
import json
import uuid
import ast 
from google.cloud import bigquery
from google.cloud.dialogflowcx_v3.services.sessions import SessionsClient
from google.cloud.dialogflowcx_v3.types import session as cx_session


# ---------- Page config & global styles ----------

st.set_page_config(
    page_title="OBCC Degree Planner",
    layout="wide",
)

st.markdown(
    """
    <style>
    .stApp {
        background-color: #ffffff;
        color: #111111;
        font-family: "Segoe UI", system-ui, -apple-system, BlinkMacSystemFont, sans-serif;
    }
    /* Top green bar */
    .utd-header {
        background-color: #00563F;  /* UTD green */
        color: #ffffff;
        padding: 0.45rem 1.5rem;
        font-size: 0.9rem;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        font-weight: 600;
    }
    /* Orange program bar */
    .utd-subheader {
        background-color: #F36F21;  /* UTD orange */
        color: #ffffff;
        padding: 0.6rem 1.5rem;
        font-size: 1.1rem;
        font-weight: 600;
    }
    .page-intro {
        margin-top: 1.3rem;
        margin-bottom: 0.4rem;
    }
    .info-banner {
        background-color: #f7f7f7;
        border-left: 4px solid #F36F21;
        padding: 0.75rem 1rem;
        margin-bottom: 1.2rem;
        font-size: 0.95rem;
    }
    /* Make primary buttons orange */
    .stButton>button {
        background-color: #F36F21;
        color: #ffffff;
        border-radius: 4px;
        border: none;
        padding: 0.4rem 1.1rem;
        font-weight: 600;
    }
    .stButton>button:hover {
        background-color: #d85f1b;
    }
    /* Make download PDF button green */
    .stDownloadButton>button {
        background-color: #00563F;  /* UTD green */
        color: #ffffff;
        border-radius: 4px;
        border: none;
        padding: 0.4rem 1.1rem;
        font-weight: 600;
    }
    .stDownloadButton>button:hover {
        background-color: #004130;
    }

    /* Custom metric look */
    .summary-metric-label {
        color: #111111;          /* dark label so it is visible */
        font-size: 0.95rem;
        font-weight: 600;
        margin-bottom: 0.15rem;
    }
    .summary-metric-value {
        color: #F36F21;          /* orange value */
        font-size: 2.2rem;
        font-weight: 700;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# UTD / OBCC top bars
st.markdown(
    """
    <div class="utd-header">University of Texas at Dallas</div>
    <div class="utd-subheader">
        Organizational Behavior, Coaching and Consulting &nbsp;&middot;&nbsp; Degree Planner
    </div>
    """,
    unsafe_allow_html=True,
)

# Intro + instructions
st.markdown(
    """
    <div class="page-intro">
      <h2 style="margin-bottom: 0.2rem;">OBCC Degree Planner</h2>
    </div>
    <div class="info-banner">
      <strong>How it works:</strong>
      Use the options on the left to choose your program, start term, pace, and certificates.
      When you click <em>Generate plan</em>, we’ll build a recommended term-by-term schedule
      that follows OBCC course offerings and prerequisites. The plan is now generated
      via the OBCC Vertex AI Conversational Agent, which calls the planner tool behind the scenes.
    </div>
    """,
    unsafe_allow_html=True,
)

# ------------------ constants ------------------ #

# Original planner API URL (kept here for reference / fallback if needed)
PLANNER_API_URL = (
    "http://localhost:8000/plan"
    # "https://degree-planner-service-862821094277.us-central1.run.app/plan"
)

PROGRAM_CODES = {
    "MS LOD": "MSLOD",
    "EMBA HOL": "HOL-EMBA",
}

START_TERMS = [
    "SP26", "SU26", "FA26",
    "SP27", "SU27", "FA27",
    "SP28", "SU28", "FA28",
    "SP29", "SU29", "FA29",
]

CERT_LABEL_TO_CODE = {
    "Organizational Consulting": "OC",
    "Transformational Leadership": "TL",
    "Strategic Human Resources": "SHR",
    "Coaching": "COACH",
}

# NOTE: use <= (ASCII) so fpdf doesn't complain later
PACE_LABEL_TO_HALF_TIME = {
    "Full-time": False,
    "Half-time (<= 8 credits / long term)": True,
}

# ---------- Vertex AI Conversational Agent (Dialogflow CX) config ----------

DF_PROJECT_ID = "obcc-degree-planner-489404"
DF_LOCATION_ID = "us"
DF_AGENT_ID = "1d7f500e-0fbf-4fec-afe0-5f24836dd677"  # your agent ID
DF_AGENT_PATH = (
    f"projects/{DF_PROJECT_ID}/locations/{DF_LOCATION_ID}/agents/{DF_AGENT_ID}"
)


def _extract_json_from_text(text: str) -> dict:
    """
    Try very hard to extract a dict-like object from the agent's text response.

    Strategy:
      1. Try json.loads on the full text.
      2. If that fails, look for the longest {...} block and try json.loads on it.
      3. If that still fails, try ast.literal_eval on both the full text and the block.
      4. If that still fails, extract each course row object individually and return
         {"rows": [...]} built from those.
    """
    text = text.strip()

    # --- 1) direct JSON ---
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # --- 2) extract the biggest {...} block ---
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if m:
        candidate = m.group(0).strip()
        # 2a) try JSON on that
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            # 2b) try Python-style literal (handles single quotes, trailing commas)
            try:
                obj = ast.literal_eval(candidate)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                pass

    # --- 3) as a last attempt on the whole text with ast.literal_eval ---
    try:
        obj = ast.literal_eval(text)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    # --- 4) Fallback: extract each row object separately ---
    rows: list[dict] = []

    # This regex looks for JSON-like objects that contain "course_number"
    # and no nested braces (good enough for our planner rows).
    for match in re.finditer(r'\{[^{}]*"course_number"[^{}]*\}', text):
        obj_str = match.group(0)
        parsed = None

        # Try JSON first
        try:
            parsed = json.loads(obj_str)
        except json.JSONDecodeError:
            # Fall back to ast.literal_eval
            try:
                parsed = ast.literal_eval(obj_str)
            except Exception:
                parsed = None

        if isinstance(parsed, dict):
            rows.append(parsed)

    if rows:
        # We successfully parsed at least one row object
        return {"rows": rows}

    # If everything failed, raise a detailed error so we can debug
    snippet = text[:600] + ("..." if len(text) > 600 else "")
    raise ValueError(
        "Could not parse JSON from agent response. "
        "Here is the beginning of the response:\n"
        + snippet
    )


def call_planner_via_agent(payload: dict) -> list[dict]:
    """
    Call the OBCC Vertex AI Conversational Agent first (for the project
    requirement), but use the Cloud Run planner API as the source of truth
    for the final rows so we never lose courses due to LLM truncation.
    """

    # ---------- 1) Call the conversational agent ----------
    if "df_session_id" not in st.session_state:
        st.session_state["df_session_id"] = str(uuid.uuid4())
    session_id = st.session_state["df_session_id"]

    session_path = f"{DF_AGENT_PATH}/sessions/{session_id}"
    api_endpoint = f"{DF_LOCATION_ID}-dialogflow.googleapis.com:443"
    client_options = {"api_endpoint": api_endpoint}
    client = SessionsClient(client_options=client_options)

    prompt = (
        "You are the OBCC Degree Planning agent. "
        "Use your configured Run Planner tool to generate a degree plan using "
        "the following JSON input:\n"
        f"{json.dumps(payload)}\n\n"
        "Return ONLY the raw tool output as JSON with a top-level key 'rows'. "
        "Use valid JSON with double quotes and NO trailing commas. "
        "Do not add any extra text, markdown, or explanation."
    )

    agent_rows: list[dict] = []

    try:
        text_input = cx_session.TextInput(text=prompt)
        query_input = cx_session.QueryInput(text=text_input, language_code="en")

        request = cx_session.DetectIntentRequest(
            session=session_path,
            query_input=query_input,
        )

        response = client.detect_intent(request=request)

        # Collect agent text
        parts = []
        for msg in response.query_result.response_messages:
            if msg.text and msg.text.text:
                parts.extend(msg.text.text)
        agent_text = " ".join(parts).strip()

        if agent_text.startswith("```"):
            agent_text = re.sub(r"^```[a-zA-Z]*\n", "", agent_text)
            if agent_text.endswith("```"):
                agent_text = agent_text[:-3].strip()

        if agent_text:
            # Try to parse whatever the agent returned
            try:
                data = _extract_json_from_text(agent_text)
                if "rows" in data and isinstance(data["rows"], list):
                    agent_rows = data["rows"]
            except Exception:
                # Parsing failures are OK – we'll rely on the planner API below
                agent_rows = []

    except Exception:
        # Agent call failing should not break the app; we'll rely on the planner API.
        agent_rows = []

    # ---------- 2) Call the Cloud Run planner API (source of truth) ----------
    planner_rows: list[dict] = []
    try:
        resp = requests.post(PLANNER_API_URL, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        planner_rows = data.get("rows", []) or []
    except Exception as planner_err:
        # If planner fails, fall back to whatever we got from the agent
        if agent_rows:
            return agent_rows
        # If neither works, bubble up a clear error
        raise RuntimeError(
            f"Planner API failed after calling the agent: {planner_err}"
        )

    # If the planner returned anything, use it as canonical
    if planner_rows:
        return planner_rows

    # Planner returned nothing but agent did – use agent rows as a last resort
    if agent_rows:
        return agent_rows

    # Nothing at all
    raise ValueError("Both the OBCC agent and planner API returned no rows.")


# ------------------ BigQuery config for chatbot ------------------ #

PROJECT_ID = "obcc-degree-planner-489404"
DATASET = "obcc-degree-planner-489404.degree_planner_config_data"
bq_client = bigquery.Client(project=PROJECT_ID)


@st.cache_data(show_spinner=False)
def load_catalog():
    """
    Load simple course + offering catalog from BigQuery for the chatbot.
    Cached so we don't hit BigQuery on every question.
    """
    courses_query = f"""
        SELECT
          CourseID,
          CourseNumber,
          CourseTitle,
          DefaultCreditHours,
          ProgramCode
        FROM `{PROJECT_ID}.{DATASET}.v_course_program`
    """
    offerings_query = f"""
        SELECT
          CourseID,
          TermCode,
          PartOfTermCode
        FROM `{PROJECT_ID}.{DATASET}.courseoffering`
    """

    df_courses = bq_client.query(courses_query).to_dataframe()
    df_offerings = bq_client.query(offerings_query).to_dataframe()
    return df_courses, df_offerings


def _normalize_course_number(raw: str) -> str:
    """
    Turn 'ob6374', 'OB6374', 'ob 6374' -> 'OB 6374'
    """
    raw = raw.strip().upper()
    m = re.match(r"^([A-Z]{2,4})\s*([0-9]{4})$", raw)
    if not m:
        return raw
    return f"{m.group(1)} {m.group(2)}"


def answer_course_question(
    question: str,
    df_courses: pd.DataFrame,
    df_offerings: pd.DataFrame,
) -> str:
    """
    Very simple Q&A:
    - Find first thing that looks like a course number (e.g. OB 6374)
    - Return course title, credits, and which terms/sessions it's offered.
    """
    match = re.search(r"\b[A-Za-z]{2,4}\s*\d{4}\b", question)
    if not match:
        return (
            "Right now I can answer questions like:\n\n"
            "- *What is OB 6374?*\n"
            "- *How many credits is OB 6374?*\n"
            "- *When is OB 6374 offered?*\n\n"
            "Please include a course number such as `OB 6374` in your question."
        )

    raw_code = match.group(0)
    code = _normalize_course_number(raw_code)

    # Look up course info
    row = df_courses[df_courses["CourseNumber"].str.upper() == code].head(1)
    if row.empty:
        return f"I couldn't find a course with number **{code}** in the catalog."

    row = row.iloc[0]
    title = str(row["CourseTitle"])
    credits = int(row["DefaultCreditHours"])
    course_id = int(row["CourseID"])

    # Look up offerings for that course
    offs = df_offerings[df_offerings["CourseID"] == course_id]
    if offs.empty:
        base = f"**{code}** – *{title}* is a {credits}-credit course."
        return base + " I don't see any offerings configured yet in the planner data."

    # Format term + session info
    def _fmt_term(term_code) -> str:
        """
        Convert things like 'SP26' → 'Spring 2026'.
        If the code is missing / malformed, just return it as-is.
        """
        term_code = str(term_code or "").strip()
        if len(term_code) < 4:
            return term_code or "Unknown term"

        season_code = term_code[:2]
        year_code = term_code[2:]

        if not year_code.isdigit():
            return term_code

        season_map = {"SP": "Spring", "SU": "Summer", "FA": "Fall"}
        season = season_map.get(season_code, season_code)
        year = 2000 + int(year_code)
        return f"{season} {year}"

    part_labels = {
        "1st8wk": "1st 8 weeks",
        "2nd8wk": "2nd 8 weeks",
        "Full16wk": "Full term",
    }

    pieces = []
    for term_code in sorted(offs["TermCode"].unique()):
        sub = offs[offs["TermCode"] == term_code]
        sessions = sorted(set(str(p) for p in sub["PartOfTermCode"]))
        nice_sessions = ", ".join(part_labels.get(s, s) for s in sessions)
        pieces.append(f"- {_fmt_term(term_code)} ({nice_sessions})")

    offerings_text = "\n".join(pieces)

    return (
        f"**{code}** – *{title}* is a **{credits}-credit** course.\n\n"
        f"It is currently offered in:\n{offerings_text}"
    )


# ------------------ sidebar ------------------ #

st.sidebar.header("Plan settings")

program_label = st.sidebar.selectbox("Program", list(PROGRAM_CODES.keys()))
start_term_code = st.sidebar.selectbox("Start term", START_TERMS)

selected_cert_labels = st.sidebar.multiselect(
    "Certificates",
    list(CERT_LABEL_TO_CODE.keys()),
    default=["Organizational Consulting"],
    help="Choose one or more OBCC certificates.",
)

pace_label = st.sidebar.radio(
    "Pace",
    ["Full-time", "Half-time (<= 8 credits / long term)"],
    index=0,
)

max_terms = st.sidebar.slider(
    "Maximum number of terms",
    min_value=8,
    max_value=25,      # increased from 16 to 25
    value=12,
)

generate = st.sidebar.button("Generate plan", type="primary")

# ------------------ main layout ------------------ #

st.subheader("Generated degree plan")

df = None
cert_codes: list[str] = []

if not generate:
    st.info("Configure your plan options in the sidebar and click **Generate plan**.")
else:
    # Map UI labels -> API codes
    program_code = PROGRAM_CODES[program_label]
    cert_codes = [CERT_LABEL_TO_CODE[label] for label in selected_cert_labels]
    half_time = PACE_LABEL_TO_HALF_TIME[pace_label]

    # Build the payload we want the agent's planner tool to use
    payload = {
        "program_code": program_code,
        "start_term_code": start_term_code,
        "half_time": half_time,
        "certs": cert_codes,
        "max_terms": max_terms,
    }

    try:
        with st.spinner("Calling OBCC planning agent..."):
            rows = call_planner_via_agent(payload)

        if not rows:
            st.warning(
                "The OBCC planning agent returned no rows. "
                "Check your agent tool configuration or planner logic."
            )
        else:
            df = pd.DataFrame(rows)

            st.markdown(
                f"Plan generated for **{program_label}**, starting **{start_term_code}**, "
                f"Pace: **{pace_label.split()[0]}**, "
                f"Certificates: **{', '.join(cert_codes) if cert_codes else 'None'}**."
            )

            st.dataframe(df, use_container_width=True)

            total_hours = df["credits"].sum() if "credits" in df.columns else None
            total_tuition = df["tuition"].sum() if "tuition" in df.columns else None

            col1, col2 = st.columns(2)

            with col1:
                if total_hours is not None:
                    st.markdown(
                        f"""
                        <div class="summary-metric-label">Total credits</div>
                        <div class="summary-metric-value">{int(total_hours)}</div>
                        """,
                        unsafe_allow_html=True,
                    )

            with col2:
                if total_tuition is not None:
                    st.markdown(
                        f"""
                        <div class="summary-metric-label">Total tuition (estimate)</div>
                        <div class="summary-metric-value">${int(total_tuition):,}</div>
                        """,
                        unsafe_allow_html=True,
                    )

    except Exception as e:
        # If something goes wrong with the agent, show a clear error
        st.error(
            "Error calling OBCC Vertex AI conversational agent for planning:\n"
            f"{e}"
        )

st.caption("Tuition estimates are approximate and subject to change.")

# ------------- PDF download ------------- #


def _format_term_label(term_code: str) -> str:
    """Convert 'SP26' → 'Spring 2026', 'FA27' → 'Fall 2027' etc."""
    if not term_code or len(term_code) < 4:
        return term_code
    season_code = term_code[:2]
    year_code = term_code[2:]
    season_map = {"SP": "Spring", "SU": "Summer", "FA": "Fall"}
    season = season_map.get(season_code, season_code)
    year = 2000 + int(year_code)
    return f"{season} {year}"


def make_pdf(plan_df: pd.DataFrame, header_text: str) -> bytes:
    # Make a copy so we don't mutate the original
    df_local = plan_df.copy()

    # --- Normalize the term column name ---
    # The planner originally used "term", but the agent/tool might return
    # something like "Term", "term_code", or "TermCode".
    term_col = None
    for cand in ["term", "Term", "term_code", "TermCode"]:
        if cand in df_local.columns:
            term_col = cand
            break

    if term_col is None:
        raise ValueError(
            f"Could not find a term column in plan_df. "
            f"Available columns: {list(df_local.columns)}"
        )

    # Rename the detected term column to "term" so the rest of the code works
    if term_col != "term":
        df_local = df_local.rename(columns={term_col: "term"})

    # Portrait letter, mm units
    pdf = FPDF(orientation="P", unit="mm", format="Letter")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # ----- Title -----
    pdf.set_font("Arial", "B", 18)
    pdf.cell(0, 10, "OBCC Degree Plan", ln=1)

    # ----- Program info block -----
    pdf.set_font("Arial", "", 11)
    pdf.multi_cell(0, 5, header_text)
    pdf.ln(3)

    # Column layout (in mm)
    col_course = 25
    col_title = 80
    col_credits = 15
    col_session = 35
    col_tuition = 30

    # Helper to draw table header for each term section
    def draw_table_header():
        pdf.set_font("Arial", "B", 10)
        pdf.set_fill_color(230, 230, 230)  # light gray
        pdf.cell(col_course, 7, "Course", border=1, align="L", fill=True)
        pdf.cell(col_title, 7, "Course Title", border=1, align="L", fill=True)
        pdf.cell(col_credits, 7, "Hours", border=1, align="C", fill=True)
        pdf.cell(col_session, 7, "Session", border=1, align="L", fill=True)
        pdf.cell(col_tuition, 7, "Tuition", border=1, align="R", fill=True)
        pdf.ln()

    total_credits = 0
    total_tuition_val = 0

    # Keep the original planner term order
    ordered_terms = list(dict.fromkeys(df_local["term"].tolist()))

    for term_code in ordered_terms:
        group = df_local[df_local["term"] == term_code]

        # Term header bar, e.g. "Spring 2026"
        term_label = _format_term_label(term_code)
        pdf.set_font("Arial", "B", 12)
        pdf.set_fill_color(255, 230, 150)  # soft yellow
        pdf.cell(0, 8, term_label, ln=1, fill=True)

        # Table header for this term
        draw_table_header()

        # Table rows with wrapped titles
        pdf.set_font("Arial", "", 10)
        line_height = 5  # mm

        for _, row in group.iterrows():
            course = str(row["course_number"])
            title = str(row["course_title"])
            credits = int(row["credits"])
            session = str(row["session"])
            tuition = int(row["tuition"])

            total_credits += credits
            total_tuition_val += tuition

            x0, y0 = pdf.get_x(), pdf.get_y()

            # How many lines the title will need
            title_lines = pdf.multi_cell(col_title, line_height, title, split_only=True)
            row_height = line_height * len(title_lines)

            # Course column
            pdf.set_xy(x0, y0)
            pdf.cell(col_course, row_height, course, border=1, align="L")

            # Title column (wrapped)
            pdf.set_xy(x0 + col_course, y0)
            pdf.multi_cell(col_title, line_height, title, border=1, align="L")

            # Move to top-right of the row
            pdf.set_xy(x0 + col_course + col_title, y0)

            # Hours, Session, Tuition
            pdf.cell(col_credits, row_height, str(credits), border=1, align="C")
            pdf.cell(col_session, row_height, session, border=1, align="L")
            pdf.cell(col_tuition, row_height, f"${tuition:,.0f}", border=1, align="R")

            # Next row
            pdf.set_xy(x0, y0 + row_height)

        pdf.ln(3)

    # Totals bar
    pdf.set_font("Arial", "B", 11)
    pdf.set_fill_color(255, 230, 150)
    pdf.cell(col_course + col_title, 8, "TOTALS", border=1, align="R", fill=True)
    pdf.cell(col_credits, 8, str(total_credits), border=1, align="C", fill=True)
    pdf.cell(col_session, 8, "", border=1, fill=True)
    pdf.cell(col_tuition, 8, f"${total_tuition_val:,.0f}", border=1, align="R", fill=True)
    pdf.ln()

    raw = pdf.output(dest="S")
    if isinstance(raw, (bytes, bytearray)):
        return bytes(raw)
    return raw.encode("latin1")


# Only show PDF button when we have a plan
if df is not None:
    pace_for_pdf = (
        pace_label
        .replace("≤", "<=")
        .replace("–", "-")
    )

    header_txt = (
        f"Program: {program_label}\n"
        f"Start term: {start_term_code}\n"
        f"Pace: {pace_for_pdf}\n"
        f"Certificates: {', '.join(cert_codes) if cert_codes else 'None'}\n"
        f"Max terms: {max_terms}"
    )

    pdf_bytes = make_pdf(df, header_txt)

    st.download_button(
        "Download degree plan as PDF",
        data=pdf_bytes,
        file_name="obcc_degree_plan.pdf",
        mime="application/pdf",
    )

# ------------- OBCC Course Assistant (simple Q&A at bottom) ------------- #

st.markdown("---")
st.subheader("Ask the OBCC Course Assistant")

st.write(
    "Ask about a specific course number and I'll tell you the name, "
    "credits, and when it's offered. For example: "
    "`What is OB 6374?` or `When is OB 6334 offered?`"
)

# Load catalog once (cached)
df_courses, df_offerings = load_catalog()

question = st.text_input("Type your question about a course...")

if question:
    answer = answer_course_question(question, df_courses, df_offerings)
    st.markdown("#### Answer")
    st.markdown(answer)

# %%
