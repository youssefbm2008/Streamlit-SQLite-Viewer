import streamlit as st
import pandas as pd
import time
from pathlib import Path
from uuid import uuid4
from collections import deque
import sqlite3
import io

def sqlite_connect(db_bytes) -> sqlite3.Connection:
    fp = Path(str(uuid4()))
    fp.write_bytes(db_bytes.getvalue())
    con = sqlite3.connect(str(fp), check_same_thread=False)
    return con

def sql_connect(file) -> sqlite3.Connection:
    conn = sqlite3.connect(':memory:', check_same_thread=False)
    conn.executescript(file.getvalue().decode('utf-8'))
    return conn

def rename_duplicate_cols(data_frame: pd.DataFrame) -> None:
    new_cols = []
    prev_cols = []

    for col in data_frame.columns:
        prev_cols.append(col)
        count = prev_cols.count(col)

        if count > 1:
            new_cols.append(f'{col}_{count}')
        else:
            new_cols.append(col)
    data_frame.columns = new_cols

def debug(*args, **kwargs) -> None:
    print(f'{time.strftime("%X")}: ', *args, **kwargs)

@st.experimental_singleton
def get_queries() -> deque:
    return deque(maxlen=50)

queries: deque[dict] = get_queries()
tab1, tab2 = st.tabs(['Execute SQL', 'Query History'])

with tab2:
    st.write(f'Total Queries: {len(queries)}')
    for dct in reversed(queries):
        st.markdown('---')
        cols = st.columns(3)
        cols[1].text(f'Exec time: {dct["exec_time_ms"]}ms')
        cols[2].text(f'Shape: {dct["shape"]}')
        st.markdown(f'```sql \n{dct["query"]} \n```')

with tab1:
    upload_file = st.file_uploader('Upload dataset:', type=['.csv'], accept_multiple_files=False)

    while upload_file is None:
        st.stop()

    if 'conn' not in st.session_state:
        st.session_state.conn = sqlite3.connect(':memory:', check_same_thread=False)

    with st.container():
        df = pd.read_csv(upload_file)

        timer_start = time.perf_counter()

        # display dataframe and stats
        ms_elapsed = int((time.perf_counter() - timer_start) * 1000)
        cols = st.columns(3)
        cols[0].text(f'Exec time: {ms_elapsed}ms')
        cols[1].text(f'Last Query: {time.strftime("%X")}')
        cols[2].text(f'Shape: {df.shape}')

        if df.columns.has_duplicates:
            rename_duplicate_cols(df)
        st.dataframe(df)

        # save query and stats for query-history tab
        if len(queries) == 0 or (len(queries) > 0 and upload_file.name != queries[-1]['query']):
            queries.append({'time': time.strftime("%X"), 'query': upload_file.name,
                            'exec_time_ms': ms_elapsed, 'shape': df.shape})

        # a "wrapper-button" is created, so only if the user clicks "Save data to..."
        # then it will process and create the file to download
        file_name = f'streamlit_{upload_file.name.split(".")[0]}.csv'
        download_data = st.button('Save data to CSV')
        if download_data:
            st.download_button(label=file_name, data=df.to_csv(index=False).encode('utf-8'),
                               file_name=file_name, mime='text/csv')

# Other parts of the code remain unchanged.
