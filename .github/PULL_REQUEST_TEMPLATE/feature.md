## Mô tả thay đổi
<!-- DAG nào? Logic thay đổi gì? -->

## Loại thay đổi
- [ ] DAG mới
- [ ] Sửa logic DAG hiện có
- [ ] Thêm/sửa task
- [ ] Cập nhật dependencies (Dockerfile)
- [ ] Config (connections, variables)

## Test plan
- [ ] `python -m py_compile dags/*.py` pass
- [ ] `ruff check dags/` pass (chạy local: `pip install ruff && ruff check dags/`)
- [ ] DAG visible trong Airflow UI (không bị parse error)
- [ ] Chạy thử DAG trong dev Airflow (trigger manually)

## Checklist
- [ ] PR target đúng branch (feature → **stg**)
- [ ] Connection IDs trong DAG dùng biến, không hardcode
- [ ] DAG có `tags` để dễ filter trong UI
- [ ] `catchup=False` nếu không cần backfill

## Promotion checklist (stg→uat hoặc uat→prd)
- [ ] DAG đã chạy thành công ít nhất 1 lần ở môi trường source
- [ ] Không có XCom / Variable nào cần migrate thủ công
- [ ] Image `ghcr.io/paraline-platform/airflow:<branch>-<sha>` đã build thành công
