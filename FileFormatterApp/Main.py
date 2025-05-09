from FileFormatterApp import FileFormatterApp
import streamlit as st
import io
import pandas as pd


# User Instructions
#- To run this on my local machine via Terminal, run: streamlit run /Users/davidkang/Desktop/FormatNow/FileFormatterApp/FileFormatterApp/Main.py


def trim_empty(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop any rows and columns that are entirely NaN,
    then reset the row index.
    """
    # 1) keep only rows with at least one non-null
    df = df.loc[df.notna().any(axis=1)]
    # 2) keep only cols with at least one non-null
    df = df.loc[:, df.notna().any(axis=0)]
    # 3) reset index so it starts at 0
    return df.reset_index(drop=True)

def make_column_names_unique(cols):
    seen = {} # Create a new dictionary
    new_cols = [] # Create a new list

    for col in cols: # Iterate through cols array
        count = seen.get(col, 0) # get the count of that col. If not, then produce 0
        if count: # Checks if the count is not zero. Or, have we seen this column before?
            new_name = f"{col}_{count}" # Then we rename it to the column name with its count.
        else:
            new_name = col # If it's first time we're seeing the column name, we keep it.
        new_cols.append(new_name) # Add the new column name to new_cols
        seen[col] = count + 1 #
    return new_cols

def run_pipeline_main():
    st.title("🐲 Fusion Ha 🟠")

    # ─── Reset Button ────────────────────────────────────────────────────────────
    if "reset_counter" not in st.session_state:
        st.session_state["reset_counter"] = 0

    if st.button("↻ Reset ↺"):
        st.session_state["df_map"] = {}
        st.session_state.pop("final_df", None)
        st.session_state["reset_counter"] += 1

    # ─── Step 1: Upload one or more CSV/Excel files ──────────────────────────────
    st.markdown(
        """
        <p style="font-size:30px; font-weight:bold; margin: 20px 0 10px 0;">
          📑 Step 1: Upload files
        </p>
        """,
        unsafe_allow_html=True
    )

    uploader_key = f"uploader_{st.session_state['reset_counter']}"
    uploaded_files = st.file_uploader(
        "",
        type=["csv", "xls", "xlsx"],
        accept_multiple_files=True,
        key=uploader_key
    )

    if uploaded_files:
        st.markdown(
            """
            <p style="font-size:30px; font-weight:bold; margin: 20px 0 10px 0;">
              ✅ Step 2: Preview uploaded raw files + top 5 records
            </p>
            """,
            unsafe_allow_html=True
        )
        st.markdown("**📑 Uploaded file names:**")
        for f in uploaded_files[:100]:
            st.markdown(f"- {f.name}")

    if not uploaded_files:
        return

    # ─── Dedupe by filename ──────────────────────────────────────────────────────
    unique_files = []
    seen_names = set()
    for file in uploaded_files:
        if file.name in seen_names:
            st.warning(
                f"⚠️ Duplicate file '{file.name}' ignored. "
                "Remove it above if you want to re‑upload."
            )
        else:
            seen_names.add(file.name)
            unique_files.append(file)

    # ─── Initialize formatter and persistent df_map ─────────────────────────────
    formatter = FileFormatterApp()
    if "df_map" not in st.session_state:
        st.session_state["df_map"] = {}
    df_map = st.session_state["df_map"]
    combined_df = None

    # ─── Process each unique file ────────────────────────────────────────────────
    for file in unique_files:
        fname = file.name
        ext = fname.split(".")[-1].lower()

        # CSV branch
        if ext == "csv":
            result = formatter.csv_to_dataframe(file)
            if not result:
                st.error(f"Failed to load CSV `{fname}`.")
                continue
            df = next(iter(result.values()))

            # ─── Excel branch: handle all sheets ─────────────────────────────────────
        elif ext in ("xls", "xlsx"):
            try:
                # load all sheets into a dict: { sheet_name: DataFrame }
                sheets = pd.read_excel(file, sheet_name=None, header=None)
            except Exception as e:
                st.error(f"Failed to load Excel `{fname}`: {e}")
                continue

            for sheet_name, raw in sheets.items():
                # trim and promote header, skip entirely empty sheets
                df = trim_empty(raw)
                if df.empty or df.shape[0] < 1:
                    st.warning(f"Tab `{sheet_name}` in `{fname}` is empty; skipping.")
                    continue
                df.columns = df.iloc[0].astype(str)
                df.columns = make_column_names_unique(df.columns)
                df = df[1:].reset_index(drop=True)
                if df.empty:
                    st.warning(f"Tab `{sheet_name}` in `{fname}` has no data after header promotion; skipping.")
                    continue

                # use a composite key so each sheet is distinct
                sheet_key = f"{fname}::{sheet_name}"
                replaced = sheet_key in df_map
                df_map[sheet_key] = df

                if replaced:
                    st.success(f"🔄 Replaced tab `{sheet_name}` from `{fname}`")
                else:
                    st.success(f"✅ Loaded tab `{sheet_name}` from `{fname}`")
                st.dataframe(df.head())

            # skip the rest of the loop for this file
            continue

        else:
            st.error(f"Unsupported file type: `{fname}`")
            continue

        # Trim rows and columns that are empty, while entirely skipping empty files
        df = trim_empty(df)
        if df.empty or df.shape[0] < 1:
            st.warning(f"File `{fname}` is empty after trimming; skipping.")
            continue

        # Now, promote first non-empty row to header, skip sheets with no data
        df.columns = df.iloc[0].astype(str)
        df.columns = make_column_names_unique(df.columns)
        df = df[1:].reset_index(drop=True)
        if df.empty:
            st.warning(f"File `{fname}` has no data after header promotion; skipping.")
            continue

        # overwrite or add
        replaced = fname in df_map
        df_map[fname] = df

        if replaced:
            st.success(f"🔄 Replaced file: `{fname}`")
        else:
            st.success(f"✅ Loaded file: `{fname}`")
        st.dataframe(df.head())

    # ─── Combine & preview loaded DataFrames ────────────────────────────────────
    if df_map:
        st.markdown(
            """
            <p style="font-size:30px; font-weight:bold; margin: 20px 0 10px 0;">
              🔎 Step 3: Preview raw merged files.
            </p>
            <p style="font-size:15px; margin: 10px 10px 10px 0;">
              Please note that this simply merges all the files you uploaded into a flattened table. You will need to
              standardize column names in Step 4 for the most effective merge (e.g., annual_rev, rev_per_year → annual_revenue).
              The good news is that we already deduped the records!
            </p>
            """,
            unsafe_allow_html=True
        )
        combined_df = formatter.combine_dataframes_from_map(df_map)
        st.dataframe(combined_df)

    # ─── Column renaming UI ─────────────────────────────────────────────────────
    if combined_df is not None:
        import re  # for normalization

        st.markdown(
            """
            <p style="font-size:30px; font-weight:bold; margin: 20px 0 10px 0;">
              🧑‍💻 Step 4: Rename columns (if needed)
            </p>
            """,
            unsafe_allow_html=True
        )
        st.markdown(
            """
            <p style="font-size:15px; margin: 10px 10 10px 0;">
              😎 Good news! For your convenience, we've already grouped your column names and offered suggestions on standardized names.
              For optimal performance, however, we encourage you to select your own column names.
            </p>
            """,
            unsafe_allow_html=True
        )

        # 1) Gather all raw column names
        raw_cols = list(combined_df.columns)

        # 2) A simple “normalize” that:
        #    • splits camelCase to snake_case
        #    • lowercases
        #    • replaces spaces/hyphens with _
        #    • drops any other punctuation
        def normalize(name: str) -> str:
            s = name.strip()
            # camel → snake
            s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
            s = s.lower()
            # spaces/hyphens → underscore
            s = re.sub(r'[\s\-]+', '_', s)
            # drop non-alphanum/underscore
            s = re.sub(r'[^0-9a-z_]+', '', s)
            # collapse multiple underscores
            s = re.sub(r'_+', '_', s)
            return s.strip('_')

        # 3) Cluster by normalized key
        clusters: dict[str, list[str]] = {}
        for col in raw_cols:
            key = normalize(col)
            clusters.setdefault(key or col, []).append(col)

        # 4) Build an editable map, allow a “Suggest for me” pass
        new_names: dict[str, str] = {}

        # 5) Show each cluster, its suggestion, and a few sample values
        raw_cols = sorted(combined_df.columns) # First let's get the raw list of columns!
        for idx, (key, cols) in enumerate(clusters.items(), start=1):
            st.markdown(f"**Group {idx}:** {', '.join(f'`{c}`' for c in cols)}")

            # Build dropdown options: all raw columns + a custom option!
            options = ["I want to customize..."] + [key] + raw_cols
            default = new_names.get(cols[0], key)

            if default in options:
                default_index = options.index(default)
            else:
                default_index = len(options) - 1

            choice = st.selectbox(
                f"Choose a standard name for Group {idx}:",
                options,
                index=default_index,
                key=f"select_{key}"
            )

            if choice == "I want to customize...":
                custom = st.text_input(
                    f"Don't like your current options? Write custom name for `{key}`:",
                    value = "" if default in raw_cols else default,
                    key = f"custom_{key}"
                ).strip()
                chosen = custom or key

            else:
                chosen = choice

            # Assign that one name to every raw column in this cluster
            for col in cols:
                new_names[col] = chosen

            # show up to 3 sample non-null values across the cluster
            samples = (
                combined_df[cols]
                .stack()
                .dropna()
                .astype(str)
                .unique()[:3]
            )
            if len(samples):
                st.write("Sample values:", list(samples))

        # 6) Apply & re‑merge
        st.markdown(
            """
            <p style="font-size:30px; font-weight:bold; margin: 20px 0 10px 0;">
              🟢 Step 5: Click button below to implement new column names!
            </p>
            """,
            unsafe_allow_html=True
        )
        if st.button("🤠 Apply Column Renaming"):
            # 1) First, rename each df individually and catch intra-file duplicates
            for fname, df in df_map.items():
                renamed = df.rename(columns=new_names)
                dupes = renamed.columns[renamed.columns.duplicated()].unique().tolist()
                if dupes:
                    # build a message for each duplicated target name
                    mapping_msgs = []
                    for dup in dupes:
                        originals = [orig for orig in df.columns if new_names.get(orig) == dup]
                        mapping_msgs.append(
                            f"{', '.join(f'`{o}`' for o in originals)} to `{dup}`"
                        )
                    mapping_str = "; ".join(mapping_msgs)
                    st.error(
                        f"⚠️ This is terrible news. File `{fname}` has duplicate column name(s) after renaming column(s) "
                        f"{mapping_str}. Please ensure each original column maps to its own unique name!"
                    )
                    return

                # no intra-file duplicates, so accept this rename
                df_map[fname] = renamed

            # 2) All files passed—now safely combine
            combined_df = formatter.combine_dataframes_from_map(df_map)

            # 3) Double-check the merged result for cross-file duplicates
            final_dupes = combined_df.columns[combined_df.columns.duplicated()].unique().tolist()
            if final_dupes:
                dup_list = ", ".join("f`{d}`" for d in final_dupes)
                st.error(
                    f"⚠️ After merging, these column name(s) still collide across files: "
                    f"{dup_list}. Please pick unique names and try again!"
                )

            else:
                # 4) Store & show
                st.session_state["final_df"] = combined_df
                st.success("Columns successfully renamed and re-merged!")
                st.dataframe(combined_df)


    # ─── Export final DataFrame ─────────────────────────────────────────────────
    final_df = st.session_state.get("final_df", combined_df)
    if final_df is not None and not final_df.empty:
        st.markdown(
            """
            <p style="font-size:30px; font-weight:bold; margin: 20px 0 10px 0;">
              📤 Step 6: Download Now!
            </p>
            """,
            unsafe_allow_html=True
        )
        buf = io.StringIO()
        final_df.to_csv(buf, index=False)
        st.download_button(
            "📄 Download File",
            data=buf.getvalue(),
            file_name="fusion_ha.csv",
            mime="text/csv"
        )
    elif uploaded_files:
        st.warning("Final DataFrame is empty or missing. Nothing to export.")


if __name__ == "__main__":
    run_pipeline_main()
