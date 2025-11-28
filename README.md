---

✅ Step 1: Make sure you have TWO dataframes

We need:

all_df = all visits (all specialists, all ortho/trauma procedures)

doc_df = only visits with specialistid == 'DOCID2203'


If your current sorted_df is already filtered to DOCID2203, do this:

from pyspark.sql.functions import col, min as F_min

# 1) All visits (no filter on specialist)
all_df = df_all_visits  # <- replace with your original full dataframe (before filtering to DOCID2203)

# 2) Only this doctor
doc_df = all_df.filter(col("specialistid") == "DOCID2203")

If sorted_df is that filtered data, you can just treat it as doc_df.


---

✅ Step 2: Get FIRST visit date with DOCID2203 per member

from pyspark.sql.functions import col, min as F_min

first_doc_visit_df = (
    doc_df
    .groupby("memberid")
    .agg(
        F_min("treatmentdate").alias("first_doc2203_date")
    )
)

Now first_doc_visit_df has:

memberid

first_doc2203_date  → the first time they saw DOCID2203


If you want to sanity check:

first_doc_visit_df.orderBy("memberid").show(20, False)


---

✅ Step 3: Join back to all visits and find “later other specialists”

Now we join this with all_df:

from pyspark.sql.functions import col

visits_after_doc_df = (
    all_df
    .join(first_doc_visit_df, on="memberid", how="inner")
    .filter(
        (col("treatmentdate") > col("first_doc2203_date")) &   # after first visit with DOCID2203
        (col("specialistid") != "DOCID2203")                    # some other ortho/trauma specialist
    )
)

If you also have a flag/column for orthopaedic / trauma (e.g. dssmainspeciality or clinicalcategorymajor), you can add:

.filter(col("clinicalcategorymajor").isin("Orthopaedics", "Trauma"))  # adjust to your real values


---

✅ Step 4: Count how many patients match this pattern

patients_count_df = visits_after_doc_df.select("memberid").distinct()
patients_count_df.count()

If you want to see the actual patients + which other doctor they went to:

visits_after_doc_df.select(
    "memberid",
    "specialistid",
    "treatmentdate",
    "procedurecode",
    "procedurecodedescription"
).orderBy("memberid", "treatmentdate").show(50, False)


---

🔁 If you want to continue from your exact code

You said:

df = spark.sql(sql_query)
sorted_df = df.orderBy(col('memberid'), col('treatmentdate'))
sorted_df.display()

Assuming df here is all visits, not only DOCID2203, then:

from pyspark.sql.functions import col, min as F_min

# 1) visits to DOCID2203
doc_df = sorted_df.filter(col("specialistid") == "DOCID2203")

# 2) first visit date with DOCID2203 per member
first_doc_visit_df = (
    doc_df
    .groupby("memberid")
    .agg(F_min("treatmentdate").alias("first_doc2203_date"))
)

# 3) later visits to other specialists
visits_after_doc_df = (
    sorted_df
    .join(first_doc_visit_df, on="memberid", how="inner")
    .filter(
        (col("treatmentdate") > col("first_doc2203_date")) &
        (col("specialistid") != "DOCID2203")
    )
)

# 4) how many patients?
visits_after_doc_df.select("memberid").distinct().count()

That’s the clean PySpark version matching exactly what the clinician asked:

> “Patients who see this doctor first (DOCID2203) and then later go on to have a procedure with any other orthopaedic/trauma specialist.”



If you paste your real column names (memberid
