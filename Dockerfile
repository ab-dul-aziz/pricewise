FROM apache/airflow:2.3.4-python3.9

USER airflow

# Upgrade pip
RUN pip install --upgrade pip

# Install required libraries
RUN pip install openai typing-extensions python-dotenv \
    nest_asyncio html_text zyte_api requests \
    beautifulsoup4 scikit-learn pandas==2.2.3 tensorflow==2.15.1 \
    matplotlib seaborn timedelta datetime
