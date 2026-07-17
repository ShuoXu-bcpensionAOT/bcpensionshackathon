# PARAMETERS
run_id = "manual"
src_server = ""
src_user = ""
src_password = ""

# COMMAND ----------
%run cp_framework

# COMMAND ----------
# Orchestrate the full control plane: setup -> bronze -> silver -> gold.
import traceback


def run_nb(name, params, timeout=2400):
    print(f">>> {name}")
    r = notebookutils.notebook.run(name, timeout, params)
    print(f"<<< {name}: {r}")
    return r


try:
    src = {"src_server": src_server, "src_user": src_user, "src_password": src_password}
    run_nb("cp_01_setup", {"run_id": run_id})
    run_nb("cp_02_ingest_bronze", {"run_id": run_id, **src})
    run_nb("cp_03_build_silver", {"run_id": run_id})
    run_nb("cp_04_build_gold", {"run_id": run_id})
    finish_run(run_id, "SUCCEEDED", {"orchestrated": True})
    print("PIPELINE COMPLETE ·", run_id)
except Exception:
    finish_run(run_id, "FAILED")
    files_put(f"_cp_err_orch_{run_id}.txt", traceback.format_exc())
    raise
