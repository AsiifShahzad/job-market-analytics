import streamlit as st
import pandas as pd
import numpy as np
import ast
import time
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
import plotly.express as px

import os

st.set_page_config(layout="wide", page_title="JobPulse AI Intelligence")

# Robust path resolution
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
DATA_PATH = os.path.join(project_root, "data", "gold", "jobs_features")
LOG_PATH = os.path.join(project_root, "logs", "pipeline.log")

@st.cache_data(ttl=86400)
def load_data():
    try:
        # Robust loader for inconsistent schemas
        all_files = []
        for root, dirs, files in os.walk(DATA_PATH):
            for file in files:
                if file.endswith(".parquet"):
                    all_files.append(os.path.join(root, file))
        
        if not all_files:
            st.error(f"Debug: No parquet files found in {os.path.abspath(DATA_PATH)}")
            return pd.DataFrame()

        dfs = []
        errors = []
        for f in all_files:
            try:
                # Read individual file
                temp_df = pd.read_parquet(f)
                # Force types to prevent schema conflicts
                if "year" in temp_df.columns:
                    temp_df["year"] = temp_df["year"].astype(int)
                dfs.append(temp_df)
            except Exception as e:
                errors.append(f"File {os.path.basename(f)}: {str(e)}")
                continue
        
        if errors:
            st.warning(f"Debug: Skipped {len(errors)} files due to errors.")
            with st.expander("Show loading errors"):
                for err in errors:
                    st.write(err)

        if not dfs:
             st.error("Debug: All files failed to load.")
             return pd.DataFrame()

        # Concatenate in pandas (handles schema evolution better than pyarrow)
        df = pd.concat(dfs, ignore_index=True)
        
        # Ensure created is datetime
        df["created"] = pd.to_datetime(df["created"], errors="coerce")
        
        # Handle extracted_skills (could be list or string depending on source)
        def parse_skills(x):
            if isinstance(x, list):
                return x
            if isinstance(x, str):
                try:
                    return ast.literal_eval(x)
                except:
                    return []
            return []
            
        if "extracted_skills" in df.columns:
            df["extracted_skills"] = df["extracted_skills"].apply(parse_skills)
        else:
            df["extracted_skills"] = []
            
        # Drop invalid dates immediately
        initial_count = len(df)
        df = df.dropna(subset=["created"])
        final_count = len(df)
        
        if initial_count > 0 and final_count == 0:
             st.error(f"Debug: Loaded {initial_count} rows but ALL had invalid dates (NaT). Check 'created' column format.")
        
        return df
    except Exception as e:
        st.error(f"Error loading data from {DATA_PATH}: {e}")
        return pd.DataFrame(columns=["created", "salary_mid", "extracted_skills", "location", "is_remote", "title", "description", "combined_text", "seniority_score", "category"])

df = load_data()

st.title("JobPulse AI – Market Intelligence Dashboard")

if df.empty:
    st.warning("No valid data available. Please check the pipeline logs.")
    st.stop()

# Safe min/max calculation
try:
    min_date = df["created"].min().to_pydatetime()
    max_date = df["created"].max().to_pydatetime()
except Exception as e:
    st.error(f"Date error: {e}")
    st.stop()

date_range = st.slider("Select time range", min_date, max_date, (min_date, max_date))

df = df[(df["created"] >= date_range[0]) & (df["created"] <= date_range[1])]

st.header("Salary Intelligence")

salary_trend = df.groupby(pd.Grouper(key="created", freq="M"))["salary_mid"].mean().reset_index()

fig_salary = px.line(
    salary_trend,
    x="created",
    y="salary_mid",
    title="Salary Growth Over Time",
    markers=True
)
st.plotly_chart(fig_salary, use_container_width=True)

st.header("Top Skills")

all_skills = df.extracted_skills.explode()
skill_counts = all_skills.value_counts().head(25).reset_index()
skill_counts.columns = ["skill", "count"]

fig_skills = px.bar(
    skill_counts,
    x="count",
    y="skill",
    orientation="h",
    title="Most In-Demand Skills"
)
st.plotly_chart(fig_skills, use_container_width=True)


st.header("Career Paths")

career_salary = (
    df.groupby("title")["salary_mid"]
    .mean()
    .sort_values(ascending=False)
    .head(20)
    .reset_index()
)

fig_career_salary = px.bar(
    career_salary,
    x="salary_mid",
    y="title",
    orientation="h",
    title="Top Paying Roles"
)
st.plotly_chart(fig_career_salary, use_container_width=True)

st.header("Geographic Trends")

geo_counts = df["location"].value_counts().head(20).reset_index()
geo_counts.columns = ["location", "count"]

fig_geo = px.bar(
    geo_counts,
    x="count",
    y="location",
    orientation="h",
    title="Top Hiring Locations"
)
st.plotly_chart(fig_geo, use_container_width=True)


# Map boolean or string values to readable labels
remote_label_map = {True: "Remote", False: "Onsite", "True": "Remote", "False": "Onsite", 1: "Remote", 0: "Onsite"}
remote_counts = df["is_remote"].map(remote_label_map).value_counts().reset_index()
remote_counts.columns = ["Type", "Count"]

# Define color mapping for clarity
color_map = {"Remote": "#2ca02c", "Onsite": "#1f77b4"}
fig_remote = px.pie(
    remote_counts,
    names="Type",
    values="Count",
    title="Remote vs Onsite",
    color="Type",
    color_discrete_map=color_map
)
fig_remote.update_traces(textinfo='percent+label')
st.plotly_chart(fig_remote, use_container_width=True)

st.markdown(
    "<span style='display:inline-block;width:16px;height:16px;background-color:#2ca02c;margin-right:8px;'></span> <b>Remote</b> "
    "<span style='display:inline-block;width:16px;height:16px;background-color:#1f77b4;margin-left:24px;margin-right:8px;'></span> <b>Onsite</b>",
    unsafe_allow_html=True
)


st.header("Job Category Trends")

cat_counts = df["category"].value_counts().head(15).reset_index()
cat_counts.columns = ["category", "count"]

fig_cat = px.bar(
    cat_counts,
    x="count",
    y="category",
    orientation="h",
    title="Top Job Categories"
)
st.plotly_chart(fig_cat, use_container_width=True)

