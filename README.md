# DEGREE_PLANNER_AGENT
# OBCC Degree Planner (Vertex AI + Cloud Run)

This project implements a degree planning tool for UTD OBCC programs (MS LOD and EMBA HOL).
The Streamlit UI calls a Vertex AI Conversational Agent and a Cloud Run planner
microservice, which uses BigQuery data to generate term-by-term degree plans and
estimated tuition.

## Main files

- `degree_plan_app.py` – Streamlit UI for the degree planner
- `main.py` – Backend entrypoint for the planner API (Cloud Run)
- `planner_core.py` – Core planning logic
- `Dockerfile_backend` / `Dockerfile_streamlit` – Dockerfiles for backend and UI
- `cloudbuild_backend.yaml` / `cloudbuild_streamlit.yaml` – Cloud Build configs
- `requirements.txt` – Python dependencies
