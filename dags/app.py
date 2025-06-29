from dotenv import load_dotenv

load_dotenv()

import os
import streamlit as st
import psycopg2
from openai import OpenAI
import traceback

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

sql_prompt = ["""
You are an expert at translating natural language into SQL for PostgreSQL. 

The database has a table called 'weather_data' with the following columns:
- city (VARCHAR)
- temperature (FLOAT)
- wind_speed (FLOAT)
- winddirection (FLOAT)
- weathercode (INT)
- request_timestamp (TIMESTAMP)

You must always return **only the SQL query** as plain text - no markdown, no code formatting.

Handle complex queries, like:
- Getting the city with the highest temperature
    -> SELECT city, temperature FROM weather_data WHERE temperature = (SELECT MAX(temperature) FROM weather_data);
- Getting the average temperature per city
    -> SELECT city, AVG(temperature) FROM weather_data GROUP BY city;
    
Avoid GROUP BY unless necessary. Always ensure the query is valid in PostgreSQL.
"""]


def get_openai_response(question, prompt):
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "user", "content": prompt[0]},
            {"role": "user", "content": question}
        ]
    )
    return response.choices[0].message.content.strip()


def detect_language(question):
    detection_prompt = f" What language is this? Respond with only the language name, like 'English' or 'Kiswahili':\n\n{question}"
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "user", "content": "You are a language detector"},
            {"role": "user", "content": detection_prompt}
        ]
    )
    return response.choices[0].message.content.lower()


def explain_sql_query(sql, question, language):
    explanation_prompt = f"""
    The user asked: "{question}"
    The SQL query generated is: 
    {sql}
    
    Explain to the user:
    1. Why this query was chosen (the reasoning and goal)
    2. Briefly describe what the query does
    
    Respond only in {language.capitalize()}.
    Start with: "I decided to write this query because..."
    """
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You explain SQL queries naturally, in the user's language"},
            {"role": "user", "content": explanation_prompt}
        ]
    )
    return response.choices[0].message.content.strip()


def run_postgres_query(query):
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DATABASE")
    )
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return colnames, rows

    st.set_page_config(page_title="Ask Weather Questions (EN or SW)")
    st.title("Ask Questions About Weather Data")

    question = st.text_input("Ask your question (in English or Kiswahili):")

    if st.button("Submit"):
        with st.spinner("Generating SQL query and running query..."):
            try:
                sql = get_openai_response(question, sql_prompt)
                lang = detect_language(question)
                explanation = explain_sql_query(sql, question, lang)

                st.subheader("Generated SQL:")
                st.code(sql, language="sql")

                st.subheader("Why this query:")
                st.write(explanation)

                colnames, results = run_postgres_query(sql)
                st.subheader("Query results:")

                if results:
                    st.dataframe([{col: row[i] for i, col in enumerate(columns)} for row in results])
                else:
                    st.info("No results found.")
            except Exception as e:
                st.error("Oops! Something went wrong while processing your request.")
                st.code(traceback.format_exc(), language="python")
                st.write("Error message:", str(e))
