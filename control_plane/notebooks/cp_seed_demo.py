# COMMAND ----------
%run cp_framework

# COMMAND ----------
# Seed a synthetic SENSITIVE table for the data-security demo: silver.hr.employees
# (schema-enabled lakehouse -> lands at silver / Tables / hr / employees). Reproducible + promotable.
from pyspark.sql import Row

rows = [
    Row(id=1, name="Alice Wong",    email="alice.wong@corp.com",    salary=132000, ssn="111-11-1111", region="BC"),
    Row(id=2, name="Bob Singh",     email="bob.singh@corp.com",     salary=98000,  ssn="222-22-2222", region="BC"),
    Row(id=3, name="Carla Diaz",    email="carla.diaz@corp.com",    salary=145000, ssn="333-33-3333", region="ON"),
    Row(id=4, name="Dan ONeil",     email="dan.oneil@corp.com",     salary=87000,  ssn="444-44-4444", region="AB"),
    Row(id=5, name="Eve Laurent",   email="eve.laurent@corp.com",   salary=156000, ssn="555-55-5555", region="ON"),
    Row(id=6, name="Frank Muller",  email="frank.muller@corp.com",  salary=76000,  ssn="666-66-6666", region="BC"),
]
df = spark.createDataFrame(rows)                                   # noqa: F821
write_path(df, tpath("silver", "employees", "hr"), mode="overwrite")
print("wrote silver.hr.employees:", df.count(), "rows")
notebookutils.notebook.exit("ok")                                 # noqa: F821
