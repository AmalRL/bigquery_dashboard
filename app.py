import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

st.title('Distinct Contact Phone Trend (Last Week)')

# -------------------------------
# Setup BigQuery credentials
# -------------------------------
try:
    # Convert secret to plain dict (fixes AttrDict issue)
    creds_dict = dict(st.secrets["bigquery_credentials"])
    credentials = service_account.Credentials.from_service_account_info(creds_dict)

    # Create BigQuery client
    client = bigquery.Client(credentials=credentials, project=creds_dict["project_id"])

except KeyError:
    st.error("BigQuery credentials not found in Streamlit secrets. Please add them as 'bigquery_credentials'.")
    st.stop()
except Exception as e:
    st.error(f"Error setting up BigQuery credentials: {e}")
    st.stop()

# -------------------------------
# Fetch data from BigQuery
# -------------------------------
@st.cache_data
def get_distinct_contact_trend():
    """Fetches distinct contact phone counts per hour for the last week from BigQuery."""
    try:
        dataset_id = '918448497760'
        messages_table_id = 'messages'

        trend_query = f"""
        SELECT
            EXTRACT(HOUR FROM TIMESTAMP(inserted_at)) AS message_hour,
            COUNT(DISTINCT contact_phone) AS distinct_contact_phone_count
        FROM `{dataset_id}.{messages_table_id}`
        WHERE TIMESTAMP(inserted_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        GROUP BY message_hour
        ORDER BY message_hour
        """

        trend_job = client.query(trend_query)
        df_trend = trend_job.to_dataframe()
        return df_trend

    except Exception as e:
        st.error(f"Error fetching data from BigQuery: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error

df_trend = get_distinct_contact_trend()

# -------------------------------
# Plotting
# -------------------------------
if not df_trend.empty:
    st.subheader('Distinct Contact Phone Count Per Hour (Last Week)')

    # Create a line plot
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.lineplot(data=df_trend, x='message_hour', y='distinct_contact_phone_count', ax=ax)

    # Set the title and labels
    ax.set_title('Distinct Contact Phone Count Per Hour (Last Week)')
    ax.set_xlabel('Hour of the Day')
    ax.set_ylabel('Distinct Contact Phone Count')

    # Set x-axis ticks to represent each hour
    ax.set_xticks(df_trend['message_hour'])

    st.pyplot(fig)
else:
    st.warning("Could not retrieve data to display the trend.")
