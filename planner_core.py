# planner_core.py

import os
from typing import List, Dict, Any

from google.cloud import bigquery
import pandas as pd

# -------------------------
# BigQuery client & config
# -------------------------

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "obcc-degree-planner-489404")
DATASET = "obcc-degree-planner-489404.degree_planner_config_data"


def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


# -------------------------
# Display labels & tuition
# -------------------------

PART_OF_TERM_LABELS = {
    "1st8wk": "1st 8 weeks",
    "2nd8wk": "2nd 8 weeks",
    "Full16wk": "Full Term",
    "SummerFullTerm": "Summer 11 weeks",
}

TUITION_PER_CREDIT = 900


# -------------------------
# Certificate code helper
# -------------------------

CERT_ALIAS_MAP = {
    "OC": "OC",
    "Organizational Consulting": "OC",
    "TL": "TL",
    "Transformational Leadership": "TL",
    "SHR": "SHR",
    "Strategic Human Resources": "SHR",
    "COACH": "COACH",
    "Coaching": "COACH",
}


def normalize_certs(certs: List[str]) -> set:
    return {CERT_ALIAS_MAP.get(c, c) for c in certs or []}


# -------------------------
# CourseCategory → cert code mapping
# -------------------------

CATEGORY_TO_CERT = {
    "OC": "OC",
    "TL": "TL",
    "Core+TL": "TL",
    "Coaching": "COACH",
    # SHR has no dedicated category; uses IsSHR flag
}


# -------------------------
# 1. Data access helpers
# -------------------------

def get_program_config(program_code: str) -> Dict[str, Any]:
    """
    Read program-level configuration from the Program table.
    Returns TotalCreditHours, MaxCoursesPerSession, etc.
    """
    client = get_bq_client()
    query = f"""
      SELECT
        ProgramID,
        ProgramCode,
        ProgramName,
        TotalCreditHours,
        TotalSemesters,
        MaxCoursesPerSession,
        CoursesPerSummerSemester,
        CoursesPerFallSpringSemester
      FROM `{DATASET}.program`
      WHERE ProgramCode = @program_code
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("program_code", "STRING", program_code),
            ]
        ),
    )
    df = job.to_dataframe()
    if df.empty:
        raise ValueError(f"No program config found for {program_code}")
    row = df.iloc[0]
    return {
        "program_id": int(row["ProgramID"]),
        "total_credit_hours": int(row["TotalCreditHours"]),
        "total_semesters": int(row["TotalSemesters"]),
        "max_courses_per_session": int(row["MaxCoursesPerSession"]),
        "courses_per_summer": int(row["CoursesPerSummerSemester"]),
    }


def get_program_courses(program_code: str, certs: List[str]) -> pd.DataFrame:
    """
    Fetch courses for a program using the CourseCategory column and
    Is16WeekCourse flag from the updated views.

    CourseCategory values:
      Core, Core+TL, TL, Elective, SupplementalElective, OC, Coaching,
      CoreRecommended, Other

    CorePriority:
      0 = chosen cert courses
      1 = cores (Core, CoreRecommended, Core+TL when TL chosen)
      2 = electives (Elective, SupplementalElective)
      3 = everything else
    """
    client = get_bq_client()

    norm_certs = normalize_certs(certs)
    want_oc = "OC" in norm_certs
    want_shr = "SHR" in norm_certs
    want_tl = "TL" in norm_certs
    want_coaching = "COACH" in norm_certs

    if program_code == "MSLOD":
        query = f"""
        DECLARE want_oc       BOOL DEFAULT @want_oc;
        DECLARE want_shr      BOOL DEFAULT @want_shr;
        DECLARE want_tl       BOOL DEFAULT @want_tl;
        DECLARE want_coaching BOOL DEFAULT @want_coaching;

        SELECT
          CourseID,
          CourseNumber,
          CourseTitle,
          DefaultCreditHours,
          ProgramCode,
          CourseCategory,
          Is16WeekCourse,
          IsCoreRecommended,
          IsSupplementalElective,
          IsCore,
          IsOC,
          IsSHR,
          IsTL,
          IsCoaching,
          OCPreferredOrder,
          SHRPreferredOrder,

          CASE
            WHEN (
                  (want_oc       AND (CourseCategory = 'OC'))
               OR (want_shr      AND IsSHR = 1)
               OR (want_tl       AND CourseCategory IN ('TL', 'Core+TL'))
               OR (want_coaching AND CourseCategory = 'Coaching')
                 )
              THEN 0
            WHEN CourseCategory IN ('Core', 'CoreRecommended')
              THEN 1
            WHEN CourseCategory IN ('Core+TL')
              THEN 1
            WHEN CourseCategory IN ('SupplementalElective', 'Elective')
              THEN 2
            ELSE 3
          END AS CorePriority,

          CASE
            WHEN want_oc  AND IsOC  = 1 AND OCPreferredOrder  IS NOT NULL
              THEN OCPreferredOrder
            WHEN want_shr AND IsSHR = 1 AND SHRPreferredOrder IS NOT NULL
              THEN SHRPreferredOrder
            ELSE 200
          END AS OrderRank

        FROM `{DATASET}.v_course_program`
        WHERE ProgramCode = @program_code
          AND (
                CourseCategory IN ('Core', 'CoreRecommended', 'Core+TL',
                                   'SupplementalElective')
             OR (want_oc       AND CourseCategory = 'OC')
             OR (want_shr      AND IsSHR = 1)
             OR (want_tl       AND CourseCategory IN ('TL', 'Core+TL'))
             OR (want_coaching AND CourseCategory = 'Coaching')
          );
        """

        job = client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("program_code", "STRING", program_code),
                    bigquery.ScalarQueryParameter("want_oc", "BOOL", want_oc),
                    bigquery.ScalarQueryParameter("want_shr", "BOOL", want_shr),
                    bigquery.ScalarQueryParameter("want_tl", "BOOL", want_tl),
                    bigquery.ScalarQueryParameter("want_coaching", "BOOL", want_coaching),
                ]
            ),
        )

    elif program_code == "HOL-EMBA":
        query = f"""
        SELECT
          CourseID,
          CourseNumber,
          CourseTitle,
          DefaultCreditHours,
          ProgramCode,
          CourseCategory,
          Is16WeekCourse,
          IsCore,
          IsElective,
          IsCoreRecommended,
          IsSupplementalElective,
          IsOC,
          IsSHR,
          IsTL,
          IsCoaching,
          OCPreferredOrder,
          SHRPreferredOrder,

          CASE
            WHEN (
                  (@want_oc       AND (CourseCategory = 'OC' OR IsOC = 1))
               OR (@want_shr      AND IsSHR = 1)
               OR (@want_tl       AND CourseCategory IN ('TL', 'Core+TL'))
               OR (@want_coaching AND CourseCategory = 'Coaching')
                 )
              THEN 0
            WHEN CourseCategory = 'Core'
              THEN 1
            WHEN CourseCategory IN ('Core+TL')
              THEN 1
            WHEN CourseCategory IN ('Elective', 'SupplementalElective')
              THEN 2
            ELSE 3
          END AS CorePriority,

          CASE
            WHEN @want_oc  AND IsOC  = 1 AND OCPreferredOrder  IS NOT NULL
              THEN OCPreferredOrder
            WHEN @want_shr AND IsSHR = 1 AND SHRPreferredOrder IS NOT NULL
              THEN SHRPreferredOrder
            ELSE 200
          END AS OrderRank

        FROM `{DATASET}.v_course_program`
        WHERE ProgramCode = @program_code
          AND (
                CourseCategory IN ('Core', 'Core+TL', 'Elective')
             OR (@want_oc       AND (CourseCategory = 'OC' OR IsOC = 1))
             OR (@want_shr      AND IsSHR = 1)
             OR (@want_tl       AND CourseCategory IN ('TL', 'Core+TL'))
             OR (@want_coaching AND CourseCategory = 'Coaching')
          );
        """

        job = client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("program_code", "STRING", program_code),
                    bigquery.ScalarQueryParameter("want_oc", "BOOL", want_oc),
                    bigquery.ScalarQueryParameter("want_shr", "BOOL", want_shr),
                    bigquery.ScalarQueryParameter("want_tl", "BOOL", want_tl),
                    bigquery.ScalarQueryParameter("want_coaching", "BOOL", want_coaching),
                ]
            ),
        )

    else:
        raise ValueError(f"Unknown program_code {program_code}")

    df = job.to_dataframe()
    df["OrderRank"] = df["OrderRank"].fillna(999)

    df = df.sort_values(
        ["CorePriority", "OrderRank", "CourseNumber"],
        ascending=[True, True, True],
    )
    return df


def get_prereqs(program_code: str) -> pd.DataFrame:
    client = get_bq_client()

    if program_code == "MSLOD":
        program_id = 2
        is_suggested_value = 1
    elif program_code == "HOL-EMBA":
        program_id = 1
        is_suggested_value = 0
    else:
        raise ValueError(f"Unknown program_code {program_code}")

    query = f"""
      SELECT *
      FROM `{DATASET}.courseprerequisite`
      WHERE ProgramID = @program_id
        AND IsSuggested = @is_suggested
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("program_id", "INT64", program_id),
                bigquery.ScalarQueryParameter("is_suggested", "INT64", is_suggested_value),
            ]
        ),
    )
    return job.to_dataframe()


def get_offerings(program_code: str) -> pd.DataFrame:
    """
    Fetch course offerings.  The updated v_course_offering view now exposes
    SessionLabel (1st8wk | 2nd8wk | Full16wk | SummerFullTerm) and
    TermSeason (Spring | Summer | Fall) alongside the legacy columns.
    """
    client = get_bq_client()

    query = f"""
      SELECT *
      FROM `{DATASET}.v_course_offering`
      WHERE ProgramCode = @program_code
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("program_code", "STRING", program_code)
            ]
        ),
    )
    return job.to_dataframe()


def get_term_preferences(program_code: str) -> pd.DataFrame:
    client = get_bq_client()

    query = f"""
      SELECT CourseID, TermCode
      FROM `{DATASET}.v_course_term_preference`
      WHERE ProgramCode = @program_code
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("program_code", "STRING", program_code)
            ]
        ),
    )
    return job.to_dataframe()


# -------------------------
# 2. Term sequence helper
# -------------------------

def generate_term_sequence(start_term_code: str, max_terms: int) -> List[str]:
    seasons = ["SP", "SU", "FA"]

    start_season = start_term_code[:2]
    year = int(start_term_code[2:])

    if start_season not in seasons:
        raise ValueError(f"Unknown season in start_term_code: {start_term_code}")

    idx = seasons.index(start_season)

    seq: List[str] = []
    for _ in range(max_terms):
        season = seasons[idx]
        seq.append(f"{season}{year:02d}")

        idx += 1
        if idx == len(seasons):
            idx = 0
            year += 1

    return seq


# -------------------------
# 3. Compaction helper
# -------------------------

def compact_plan_terms(
    plan_terms: List[Dict[str, Any]],
    max_courses_per_term: int,
    offerings: pd.DataFrame,
    prereqs: pd.DataFrame,
) -> List[Dict[str, Any]]:
    if not plan_terms:
        return plan_terms

    # Determine the offering-column name (supports both legacy and new views)
    slot_col = (
        "SessionLabel"
        if "SessionLabel" in offerings.columns
        else "PartOfTermCode"
    )

    changed = True

    while changed:
        changed = False

        course_term: Dict[int, int] = {}
        for ti, term in enumerate(plan_terms):
            for c in term["courses"]:
                course_term[c["course_id"]] = ti

        def can_place(course_id: int, target_idx: int) -> bool:
            target_term = plan_terms[target_idx]
            season = target_term["term_code"][:2]

            off = offerings[
                (offerings["CourseID"] == course_id) &
                (offerings["TermCode"] == season)
            ]
            if off.empty:
                return False

            needed = prereqs.loc[
                prereqs["CourseID"] == course_id,
                "PrerequisiteCourseID"
            ].tolist()

            for pid in needed:
                if pid not in course_term:
                    return False
                if course_term[pid] >= target_idx:
                    return False

            return True

        for i in range(len(plan_terms)):
            if not plan_terms[i]["courses"]:
                continue

            while len(plan_terms[i]["courses"]) < max_courses_per_term:
                moved_any = False

                for j in range(i + 1, len(plan_terms)):
                    if not plan_terms[j]["courses"]:
                        continue

                    for c in list(plan_terms[j]["courses"]):
                        cid = c["course_id"]

                        if not can_place(cid, i):
                            continue

                        plan_terms[j]["courses"].remove(c)
                        plan_terms[j]["total_credits"] -= c["credits"]

                        plan_terms[i]["courses"].append(c)
                        plan_terms[i]["total_credits"] += c["credits"]

                        changed = True
                        moved_any = True
                        course_term[cid] = i
                        break

                    if moved_any:
                        break

                if not moved_any:
                    break

        while plan_terms and not plan_terms[-1]["courses"]:
            plan_terms.pop()
            changed = True

    return plan_terms


# -------------------------
# 4. Main planner
# -------------------------

def run_planner(
    program_code: str,
    start_term_code: str,
    certs: List[str],
    max_terms: int = 20,
    target_credits: int | None = None,
    half_time: bool = False,
) -> Dict[str, Any]:
    # ---- pull program-level config from BQ ----
    prog_cfg = get_program_config(program_code)

    if target_credits is None:
        target_credits = prog_cfg["total_credit_hours"]

    max_courses_per_session = prog_cfg["max_courses_per_session"]  # 2
    max_courses_per_term = 1 if half_time else (max_courses_per_session * 2)
    # Fall/Spring allow up to 2 sessions × max_courses_per_session,
    # but capped at 3 per the data ("2 or 3").  Summer = max 2.
    max_fall_spring = 3 if not half_time else 1
    max_summer = prog_cfg["courses_per_summer"] if not half_time else 1

    courses = get_program_courses(program_code, certs)
    prereqs = get_prereqs(program_code)
    offerings = get_offerings(program_code)
    term_prefs_df = get_term_preferences(program_code)

    # Determine the slot column name (supports legacy + new views)
    slot_col = (
        "SessionLabel"
        if "SessionLabel" in offerings.columns
        else "PartOfTermCode"
    )

    # Manual prereq override: FIN 6301 requires OPRE 6301 for HOL-EMBA
    # (not yet encoded in the prerequisite table)
    if program_code == "HOL-EMBA":
        try:
            fin_id = int(
                courses.loc[courses["CourseNumber"] == "FIN 6301", "CourseID"].iloc[0]
            )
            opre_id = int(
                courses.loc[courses["CourseNumber"] == "OPRE 6301", "CourseID"].iloc[0]
            )

            exists_mask = (
                (prereqs["CourseID"] == fin_id) &
                (prereqs["PrerequisiteCourseID"] == opre_id)
            )

            if not exists_mask.any():
                extra_row = {
                    "CourseID": fin_id,
                    "PrerequisiteCourseID": opre_id,
                }
                if "ProgramID" in prereqs.columns:
                    extra_row["ProgramID"] = 1
                if "IsSuggested" in prereqs.columns:
                    extra_row["IsSuggested"] = 0

                prereqs = pd.concat(
                    [prereqs, pd.DataFrame([extra_row])],
                    ignore_index=True,
                )
        except IndexError:
            pass

    term_seq = generate_term_sequence(start_term_code, max_terms)

    term_pref_map = (
        term_prefs_df
        .groupby("CourseID")["TermCode"]
        .apply(set)
        .to_dict()
    )

    start_year = int(start_term_code[2:])

    taken = set()
    plan_terms: List[Dict[str, Any]] = []
    total_credits_so_far = 0

    part_order = {"1st8wk": 0, "2nd8wk": 1, "Full16wk": 2, "SummerFullTerm": 2}

    for full_term in term_seq:
        if total_credits_so_far >= target_credits:
            break

        season = full_term[:2]
        year = int(full_term[2:])

        # Per-season course cap
        season_max = max_summer if season == "SU" else max_fall_spring

        term_courses: List[Dict[str, Any]] = []
        term_credits = 0
        term_course_count = 0

        used_8wk_slots = set()

        for _, row in courses.iterrows():
            if total_credits_so_far >= target_credits:
                break
            if term_course_count >= season_max:
                break

            cid = int(row["CourseID"])
            if cid in taken:
                continue

            course_number = row["CourseNumber"]
            is_16wk = int(row.get("Is16WeekCourse", 0)) == 1

            prefs = term_pref_map.get(cid)
            if year == start_year and prefs and season not in prefs:
                continue

            needed = prereqs.loc[
                prereqs["CourseID"] == cid,
                "PrerequisiteCourseID"
            ].tolist()
            if not set(needed).issubset(taken):
                continue

            credits = int(row["DefaultCreditHours"])
            if total_credits_so_far + credits > target_credits:
                continue

            offered = offerings[
                (offerings["CourseID"] == cid) &
                (offerings["TermCode"] == season)
            ]
            if offered.empty:
                continue

            chosen_slot = None
            slots = [str(s) for s in offered[slot_col].dropna().unique()]

            # --- 16-week courses must go to Full16wk (data-driven) ---
            if is_16wk and season in ("SP", "FA"):
                if "Full16wk" in slots:
                    chosen_slot = "Full16wk"
                else:
                    continue

            # --- Summer: use SummerFullTerm ---
            elif season == "SU":
                if "SummerFullTerm" in slots:
                    chosen_slot = "SummerFullTerm"
                else:
                    for slot in slots:
                        chosen_slot = slot
                        break

            # --- Fall / Spring: prefer 8-week slots ---
            elif season in ("SP", "FA"):
                has_real_8wk = any(s in ("1st8wk", "2nd8wk") for s in slots)

                if has_real_8wk:
                    for slot in slots:
                        if slot in ("1st8wk", "2nd8wk") and slot not in used_8wk_slots:
                            chosen_slot = slot
                            break
                else:
                    if "1st8wk" not in used_8wk_slots:
                        chosen_slot = "1st8wk"
                    elif "2nd8wk" not in used_8wk_slots:
                        chosen_slot = "2nd8wk"

            if chosen_slot is None:
                continue

            if season in ("SP", "FA") and chosen_slot in ("1st8wk", "2nd8wk"):
                used_8wk_slots.add(chosen_slot)

            label = PART_OF_TERM_LABELS.get(chosen_slot, chosen_slot)

            term_courses.append({
                "course_id": cid,
                "course_number": course_number,
                "title": row["CourseTitle"],
                "credits": credits,
                "part_of_term": chosen_slot,
                "part_of_term_label": label,
            })
            term_credits += credits
            total_credits_so_far += credits
            term_course_count += 1
            taken.add(cid)

        if term_courses:
            term_courses.sort(key=lambda c: part_order.get(c["part_of_term"], 99))

            plan_terms.append({
                "term_code": full_term,
                "total_credits": term_credits,
                "courses": term_courses,
            })

        if total_credits_so_far >= target_credits:
            break

        plan_terms = compact_plan_terms(
            plan_terms=plan_terms,
            max_courses_per_term=season_max,
            offerings=offerings,
            prereqs=prereqs,
        )

    return {
        "program_code": program_code,
        "certificates": certs,
        "start_term_code": start_term_code,
        "half_time": half_time,
        "terms": plan_terms,
        "total_credits": total_credits_so_far,
    }


# -------------------------
# 5. Tuition helpers
# -------------------------

def enrich_plan_with_tuition(
    plan: Dict[str, Any],
    tuition_per_credit: int = TUITION_PER_CREDIT,
) -> Dict[str, Any]:
    total = 0

    for term in plan["terms"]:
        term_tuition = 0
        for c in term["courses"]:
            credits = int(c["credits"])
            t = credits * tuition_per_credit
            c["tuition"] = t
            term_tuition += t
        term["term_tuition"] = term_tuition
        total += term_tuition

    plan["tuition_per_credit"] = tuition_per_credit
    plan["total_tuition"] = total
    return plan


def plan_to_table_rows(
    plan: Dict[str, Any],
    tuition_per_credit: int = TUITION_PER_CREDIT,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for term in plan["terms"]:
        term_code = term["term_code"]

        for c in term["courses"]:
            credits = int(c["credits"])
            session_label = c.get("part_of_term_label", c["part_of_term"])
            tuition = credits * tuition_per_credit

            rows.append({
                "term": term_code,
                "course_number": c["course_number"],
                "course_title": c["title"],
                "credits": credits,
                "session": session_label,
                "tuition": tuition,
            })

    return rows


# -------------------------
# 6. Simple text summary
# -------------------------

def summarize_plan(plan: Dict[str, Any], label: str = "") -> None:
    if label:
        print(f"\n===== {label} =====")
    print(
        f"Program: {plan['program_code']}, Certs: {plan['certificates']}, "
        f"Half-time: {plan.get('half_time', False)}, "
        f"Total credits: {plan['total_credits']}"
    )
    print("Terms:")
    for term in plan["terms"]:
        term_code = term["term_code"]
        t_cred = term["total_credits"]
        courses = ", ".join(
            f"{c['course_number']} ({c.get('part_of_term_label', c['part_of_term'])})"
            for c in term["courses"]
        )
        print(f"  - {term_code}: {t_cred} credits -> {courses}")
                   
