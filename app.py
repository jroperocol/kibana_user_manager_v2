from __future__ import annotations

import base64
import csv
import io
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from elastic_client import create_user, delete_user, list_roles, list_users, test_connection
from utils_io import (
    load_instances_from_csv,
    load_instances_from_json,
    validate_instance_row,
)

st.set_page_config(page_title="kibana_user_manager", layout="wide")
st.title("kibana_user_manager")
st.caption("Gestión masiva de usuarios/roles en múltiples instancias Elasticsearch/Kibana")
st.markdown("**Powered by GoAI**")


DEFAULT_SUPERUSERS = [
    {"username": "jropero", "full_name": "Jorge Ropero", "email": "jropero@broadvoice.com", "roles": "superuser", "password": "Gocontact2021"},
    {"username": "mfonseca", "full_name": "Marcio Fonseca", "email": "mfonseca@broadvoice.com", "roles": "superuser", "password": "Gocontact2021"},
    {"username": "svega", "full_name": "Sevastian Vega", "email": "svega@broadvoice.com", "roles": "superuser", "password": "Gocontact2021"},
    {"username": "ppimenta", "full_name": "Paulo Pimenta", "email": "ppimenta@broadvoice.com", "roles": "superuser", "password": "Gocontact2021"},
    {"username": "dpires", "full_name": "David Pires", "email": "dpires@broadvoice.com", "roles": "superuser", "password": "Gocontact2021"},
    {"username": "nfrade", "full_name": "Nuno Frade", "email": "nfrade@broadvoice.com", "roles": "superuser", "password": "Gocontact2021"},
    {"username": "cpatino", "full_name": "Camilo Patino", "email": "cpatino@broadvoice.com", "roles": "superuser", "password": "Gocontact2021"},
]


if "instances" not in st.session_state:
    st.session_state.instances = []
if "auth" not in st.session_state:
    st.session_state.auth = {"mode": "Basic Auth", "username": "", "password": "", "api_key": ""}
if "instance_auth" not in st.session_state:
    st.session_state.instance_auth = {}
if "auth_logs" not in st.session_state:
    st.session_state.auth_logs = []
if "instances_signature" not in st.session_state:
    st.session_state.instances_signature = ""


def get_auth_headers() -> Dict[str, str]:
    auth = st.session_state.auth
    headers = {"Content-Type": "application/json"}

    if auth["mode"] == "Basic Auth":
        username = auth.get("username", "")
        password = auth.get("password", "")
        if not username or not password:
            return {}
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("utf-8")
        headers["Authorization"] = f"Basic {token}"
    else:
        api_key = auth.get("api_key", "")
        if not api_key:
            return {}
        headers["Authorization"] = f"ApiKey {api_key}"

    return headers


def instances_dict() -> Dict[str, str]:
    return {item["name"]: item["base_url"] for item in st.session_state.instances}


def parse_bulk_users_from_text(text: str) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 3:
            continue
        username, password, raw_roles = parts[0], parts[1], ",".join(parts[2:])
        roles = [r.strip() for r in raw_roles.split(";") if r.strip()]
        if username and password:
            rows.append({"username": username, "password": password, "roles": roles})
    return rows


def parse_bulk_users_from_csv(content: bytes) -> List[Dict[str, object]]:
    text_buffer = io.StringIO(content.decode("utf-8"))
    reader = csv.DictReader(text_buffer)
    result: List[Dict[str, object]] = []
    for row in reader:
        username = (row.get("username") or "").strip()
        password = (row.get("password") or "").strip()
        roles_raw = (row.get("roles") or "").strip()
        roles = [r.strip() for r in roles_raw.split(";") if r.strip()]
        if username and password:
            result.append({"username": username, "password": password, "roles": roles})
    return result


def parse_roles(raw_roles: object) -> List[str]:
    if isinstance(raw_roles, list):
        return [str(role).strip() for role in raw_roles if str(role).strip()]

    role_text = str(raw_roles or "").strip()
    if not role_text:
        return []

    separators = [";", ","]
    roles = [role_text]
    for separator in separators:
        if separator in role_text:
            roles = [part.strip() for part in role_text.split(separator)]
            break

    return [role for role in roles if role]


def short_message(resp: Dict[str, Any]) -> str:
    message = str(resp.get("message") or "").strip()
    status_code = resp.get("status_code")
    if status_code in (401, 403):
        return "Unauthorized" if status_code == 401 else "Forbidden"
    if message:
        return message.splitlines()[0][:120]
    if resp.get("ok"):
        return "OK"
    return "Error"


def truncate_detail(value: object, max_len: int = 1000) -> str:
    text = str(value or "")
    return text[:max_len] + ("..." if len(text) > max_len else "")


def is_auth_failure(resp: Dict[str, Any]) -> bool:
    return resp.get("status_code") in (401, 403)


def record_auth(
    instance_name: str,
    base_url: str,
    ok: bool,
    status_code: object,
    msg_short: str,
    detail: str,
    action: str,
) -> None:
    ts = datetime.utcnow().isoformat(timespec="seconds")
    st.session_state.instance_auth[base_url] = {
        "name": instance_name,
        "base_url": base_url,
        "ok": bool(ok),
        "status_code": status_code,
        "message_short": msg_short,
        "detail": truncate_detail(detail, 1000),
        "last_checked": ts,
        "action": action,
    }
    st.session_state.auth_logs.append(
        {
            "ts": ts,
            "instance": instance_name,
            "base_url": base_url,
            "action": action,
            "status_code": status_code,
            "message": msg_short,
            "detail": truncate_detail(detail, 1000),
        }
    )


def check_all_instances_auth(instances: List[Dict[str, str]], headers: Dict[str, str], action: str = "test_connection") -> None:
    if not headers:
        return
    for item in instances:
        resp = test_connection(item["base_url"], headers)
        status_code = resp.get("status_code")
        msg_short = short_message(resp)
        detail = resp.get("message") or resp.get("data") or ""
        record_auth(item["name"], item["base_url"], bool(resp.get("ok")), status_code, msg_short, str(detail), action)


def get_operable_instances(all_instances: Dict[str, str]) -> Dict[str, str]:
    operable: Dict[str, str] = {}
    for name, base_url in all_instances.items():
        auth_state = st.session_state.instance_auth.get(base_url)
        if auth_state is None or auth_state.get("ok", True):
            operable[name] = base_url
    return operable


def build_auth_status_rows(instances: List[Dict[str, str]]) -> List[Dict[str, object]]:
    rows = []
    for item in instances:
        auth_state = st.session_state.instance_auth.get(item["base_url"], {})
        known = bool(auth_state)
        ok = bool(auth_state.get("ok")) if known else False
        rows.append(
            {
                "name": item["name"],
                "base_url": item["base_url"],
                "auth_status": "OK" if known and ok else ("FAIL" if known else "PENDING"),
                "http_status": auth_state.get("status_code", ""),
                "message_short": auth_state.get("message_short", "No verificado"),
                "timestamp": auth_state.get("last_checked", ""),
            }
        )
    return rows


def build_auth_report_excel(instances: List[Dict[str, str]]) -> bytes:
    auth_rows = []
    for item in instances:
        auth_state = st.session_state.instance_auth.get(item["base_url"], {})
        auth_rows.append(
            {
                "name": item["name"],
                "base_url": item["base_url"],
                "auth_ok": bool(auth_state.get("ok", False)),
                "status_code": auth_state.get("status_code", ""),
                "message_short": auth_state.get("message_short", "No verificado"),
            }
        )

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        pd.DataFrame(auth_rows).to_excel(writer, index=False, sheet_name="auth_status")
        if st.session_state.auth_logs:
            pd.DataFrame(st.session_state.auth_logs).to_excel(writer, index=False, sheet_name="logs")
    return buffer.getvalue()


with st.sidebar:
    st.header("Instancias")

    editable_df = pd.DataFrame(st.session_state.instances or [{"name": "", "base_url": ""}])
    updated_df = st.data_editor(
        editable_df,
        num_rows="dynamic",
        use_container_width=True,
        key="instances_editor",
    )

    cleaned_instances = []
    for _, row in updated_df.iterrows():
        candidate = {
            "name": str(row.get("name", "")).strip(),
            "base_url": str(row.get("base_url", "")).strip().rstrip("/"),
        }
        if not candidate["name"] and not candidate["base_url"]:
            continue
        valid, _ = validate_instance_row(candidate)
        if valid:
            cleaned_instances.append(candidate)
    st.session_state.instances = cleaned_instances

    current_signature = "|".join(f"{item['name']}::{item['base_url']}" for item in st.session_state.instances)

    import_file = st.file_uploader("Importar CSV/JSON", type=["csv", "json"])
    if import_file is not None:
        bytes_data = import_file.read()
        if import_file.name.lower().endswith(".csv"):
            loaded, errors = load_instances_from_csv(bytes_data)
        else:
            loaded, errors = load_instances_from_json(bytes_data)

        if loaded:
            merged = {(x["name"], x["base_url"]): x for x in st.session_state.instances}
            for item in loaded:
                merged[(item["name"], item["base_url"])] = item
            st.session_state.instances = list(merged.values())
            current_signature = "|".join(f"{item['name']}::{item['base_url']}" for item in st.session_state.instances)
            st.success(f"Instancias importadas: {len(loaded)}")
        for err in errors:
            st.warning(err)

    headers = get_auth_headers()

    if headers and current_signature != st.session_state.instances_signature and st.session_state.instances:
        check_all_instances_auth(st.session_state.instances, headers, action="instances_changed")
        st.session_state.instances_signature = current_signature

    if st.button("Probar conexión"):
        if not headers:
            st.warning("Configura autenticación antes de probar conexión.")
        elif not st.session_state.instances:
            st.warning("Agrega al menos una instancia.")
        else:
            check_all_instances_auth(st.session_state.instances, headers, action="manual_test_connection")
            st.session_state.instances_signature = current_signature

    st.divider()
    st.subheader("Estado de autenticación")

    auth_rows = build_auth_status_rows(st.session_state.instances)
    if auth_rows:
        status_df = pd.DataFrame(auth_rows)
        ok_count = int((status_df["auth_status"] == "OK").sum())
        fail_count = int((status_df["auth_status"] == "FAIL").sum())
        pending_count = int((status_df["auth_status"] == "PENDING").sum())
        st.caption(f"OK: {ok_count} | FAIL: {fail_count} | PENDING: {pending_count}")
        st.dataframe(status_df, use_container_width=True)
        st.download_button(
            "Descargar reporte Excel",
            data=build_auth_report_excel(st.session_state.instances),
            file_name="auth_report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        st.caption("Sin instancias cargadas.")

    show_logs = st.checkbox("Mostrar logs", value=False)
    if show_logs:
        with st.expander("Logs de autenticación", expanded=True):
            if st.session_state.auth_logs:
                st.dataframe(pd.DataFrame(st.session_state.auth_logs[-200:]), use_container_width=True)
            else:
                st.caption("No hay logs disponibles.")

    st.divider()
    st.header("Autenticación")

    mode = st.radio("Método", ["Basic Auth", "API Key"], index=0 if st.session_state.auth["mode"] == "Basic Auth" else 1)
    st.session_state.auth["mode"] = mode

    if mode == "Basic Auth":
        st.session_state.auth["username"] = st.text_input("Username", value=st.session_state.auth.get("username", ""))
        st.session_state.auth["password"] = st.text_input("Password", type="password", value=st.session_state.auth.get("password", ""))
    else:
        st.session_state.auth["api_key"] = st.text_input("API Key", type="password", value=st.session_state.auth.get("api_key", ""))


tab_users, tab_create, tab_roles = st.tabs(["Usuarios", "Crear usuarios", "Roles"])

with tab_users:
    st.subheader("Listado de usuarios")
    all_instances = instances_dict()
    operable_instances = get_operable_instances(all_instances)
    failed_count = len(all_instances) - len(operable_instances)
    if not all_instances:
        st.info("No hay instancias configuradas.")
    elif not operable_instances:
        st.warning(f"No hay instancias operables. Fallaron autenticación: {failed_count}.")
    else:
        if failed_count:
            st.caption(f"Se excluyeron {failed_count} instancia(s) con auth fallida.")
        selected_name = st.selectbox("Instancia", list(operable_instances.keys()), key="users_instance")
        base_url = operable_instances[selected_name]
        headers = get_auth_headers()

        if st.button("Refrescar", key="refresh_users"):
            if not headers:
                st.warning("Completa autenticación.")
            else:
                users_resp = list_users(base_url, headers)
                if is_auth_failure(users_resp):
                    record_auth(
                        selected_name,
                        base_url,
                        False,
                        users_resp.get("status_code"),
                        short_message(users_resp),
                        str(users_resp.get("message", "")),
                        "list_users",
                    )
                st.session_state.users_data = users_resp

        users_resp = st.session_state.get("users_data", {})
        if users_resp.get("ok"):
            users_map = users_resp.get("data", {})
            user_rows = []
            for username, payload in users_map.items():
                user_rows.append(
                    {
                        "username": username,
                        "enabled": payload.get("enabled", True),
                        "roles": ", ".join(payload.get("roles", [])),
                    }
                )
            st.dataframe(pd.DataFrame(user_rows), use_container_width=True)

            selected_to_delete = st.multiselect(
                "Usuarios a eliminar",
                options=[r["username"] for r in user_rows],
            )
            confirm_text = st.text_input('Confirmación: escribe DELETE para borrar en masa')

            if st.button("Eliminar seleccionados", type="primary"):
                if confirm_text != "DELETE":
                    st.error("Confirmación inválida.")
                elif not selected_to_delete:
                    st.warning("Selecciona al menos un usuario.")
                else:
                    if not headers:
                        st.warning("Completa autenticación.")
                    else:
                        results = []
                        for username in selected_to_delete:
                            resp = delete_user(base_url, headers, username)
                            if is_auth_failure(resp):
                                record_auth(
                                    selected_name,
                                    base_url,
                                    False,
                                    resp.get("status_code"),
                                    short_message(resp),
                                    str(resp.get("message", "")),
                                    "delete_user",
                                )
                            results.append(
                                {
                                    "instancia": selected_name,
                                    "usuario": username,
                                    "ok": resp.get("ok"),
                                    "status": resp.get("status_code"),
                                    "mensaje": resp.get("message", "deleted" if resp.get("ok") else "error"),
                                }
                            )
                        st.dataframe(pd.DataFrame(results), use_container_width=True)
        elif users_resp:
            st.error(f"Error: {users_resp.get('status_code')} - {users_resp.get('message')}")

with tab_create:
    st.subheader("Crear usuarios")
    all_instances = instances_dict()
    operable_instances = get_operable_instances(all_instances)
    failed_count = len(all_instances) - len(operable_instances)
    if not all_instances:
        st.info("No hay instancias configuradas.")
    elif not operable_instances:
        st.warning(f"No hay instancias operables. Fallaron autenticación: {failed_count}.")
    else:
        if failed_count:
            st.caption(f"Se excluyeron {failed_count} instancia(s) con auth fallida.")
        target = st.selectbox("Instancia destino", ["Todas"] + list(operable_instances.keys()), key="create_instance")
        headers = get_auth_headers()

        if not headers:
            st.warning("Completa autenticación para consultar roles y crear usuarios.")
        else:
            target_instances = list(operable_instances.items()) if target == "Todas" else [(target, operable_instances[target])]

            all_role_names = set()
            role_errors = []
            for instance_name, instance_url in target_instances:
                roles_resp = list_roles(instance_url, headers)
                if roles_resp.get("ok"):
                    all_role_names.update(roles_resp.get("data", {}).keys())
                else:
                    role_errors.append(instance_name)
                    if is_auth_failure(roles_resp):
                        record_auth(
                            instance_name,
                            instance_url,
                            False,
                            roles_resp.get("status_code"),
                            short_message(roles_resp),
                            str(roles_resp.get("message", "")),
                            "list_roles",
                        )

            if role_errors:
                st.caption(f"Roles no disponibles en {len(role_errors)} instancia(s).")

            available_roles = sorted(all_role_names)
            st.caption("Roles disponibles")
            st.write(available_roles if available_roles else "No se pudieron cargar roles.")

            st.markdown("#### Crear un usuario")
            with st.form("single_create_form"):
                new_username = st.text_input("Username")
                new_password = st.text_input("Password", type="password")
                new_roles = st.multiselect("Roles", options=available_roles)
                submit_single = st.form_submit_button("Crear usuario")

            if submit_single:
                if not new_username or not new_password:
                    st.error("Username y password son obligatorios.")
                else:
                    results = []
                    for instance_name, instance_url in target_instances:
                        resp = create_user(instance_url, headers, new_username, new_password, new_roles)
                        if is_auth_failure(resp):
                            record_auth(
                                instance_name,
                                instance_url,
                                False,
                                resp.get("status_code"),
                                short_message(resp),
                                str(resp.get("message", "")),
                                "create_user_single",
                            )
                        results.append(
                            {
                                "instancia": instance_name,
                                "usuario": new_username,
                                "ok": resp.get("ok"),
                                "status": resp.get("status_code"),
                                "mensaje": resp.get("message", "created" if resp.get("ok") else "error"),
                            }
                        )
                    st.dataframe(pd.DataFrame(results), use_container_width=True)

            st.markdown("#### Bulk create")
            st.caption("Formato textarea: username,password,role1;role2")
            bulk_text = st.text_area("Entradas bulk")
            bulk_csv = st.file_uploader("o carga CSV (username,password,roles)", type=["csv"], key="bulk_csv")

            if st.button("Ejecutar bulk create"):
                bulk_entries = parse_bulk_users_from_text(bulk_text)
                if bulk_csv is not None:
                    bulk_entries.extend(parse_bulk_users_from_csv(bulk_csv.read()))

                if not bulk_entries:
                    st.warning("No hay entradas bulk válidas.")
                else:
                    results = []
                    for instance_name, instance_url in target_instances:
                        for entry in bulk_entries:
                            resp = create_user(
                                instance_url,
                                headers,
                                str(entry["username"]),
                                str(entry["password"]),
                                list(entry["roles"]),
                            )
                            if is_auth_failure(resp):
                                record_auth(
                                    instance_name,
                                    instance_url,
                                    False,
                                    resp.get("status_code"),
                                    short_message(resp),
                                    str(resp.get("message", "")),
                                    "create_user_bulk",
                                )
                            results.append(
                                {
                                    "instancia": instance_name,
                                    "usuario": entry["username"],
                                    "ok": resp.get("ok"),
                                    "status": resp.get("status_code"),
                                    "mensaje": resp.get("message", "created" if resp.get("ok") else "error"),
                                }
                            )
                    st.dataframe(pd.DataFrame(results), use_container_width=True)

            st.markdown("#### Feature opcional: usuarios default (superuser)")
            enable_default_superusers = st.checkbox("Crear usuarios default (superuser)", value=False)

            if enable_default_superusers:
                default_password = st.text_input(
                    "Password global",
                    type="password",
                    value="Gocontact2021",
                    key="default_superusers_global_password",
                )

                apply_password = st.button("Aplicar a todos", key="apply_default_superuser_password")

                if "default_superusers_table" not in st.session_state:
                    st.session_state.default_superusers_table = [dict(row) for row in DEFAULT_SUPERUSERS]

                if apply_password:
                    st.session_state.default_superusers_table = [
                        {**row, "password": default_password} for row in st.session_state.default_superusers_table
                    ]

                default_users_df = pd.DataFrame(st.session_state.default_superusers_table)
                edited_default_users_df = st.data_editor(
                    default_users_df,
                    use_container_width=True,
                    num_rows="dynamic",
                    key="default_superusers_editor",
                )
                st.session_state.default_superusers_table = edited_default_users_df.to_dict("records")

                confirm_default_superusers = st.checkbox(
                    "Confirmo que quiero crear usuarios SUPERUSER en la(s) instancia(s) seleccionada(s).",
                    value=False,
                )

                if st.button("Crear usuarios default", type="primary"):
                    if not confirm_default_superusers:
                        st.error("Debes confirmar antes de crear usuarios SUPERUSER.")
                    else:
                        created_count = 0
                        failures = []

                        for instance_name, instance_url in target_instances:
                            for row in st.session_state.default_superusers_table:
                                username = str(row.get("username", "")).strip()
                                password = str(row.get("password", "")).strip()
                                full_name = str(row.get("full_name", "")).strip()
                                email = str(row.get("email", "")).strip()
                                roles = parse_roles(row.get("roles", ""))

                                if not username or not password:
                                    failures.append(
                                        {
                                            "instancia": instance_name,
                                            "username": username or "(vacío)",
                                            "error": "username/password requeridos",
                                        }
                                    )
                                    continue

                                resp = create_user(
                                    instance_url,
                                    headers,
                                    username,
                                    password,
                                    roles,
                                    full_name=full_name,
                                    email=email,
                                )
                                if is_auth_failure(resp):
                                    record_auth(
                                        instance_name,
                                        instance_url,
                                        False,
                                        resp.get("status_code"),
                                        short_message(resp),
                                        str(resp.get("message", "")),
                                        "create_user_default",
                                    )

                                if resp.get("ok"):
                                    created_count += 1
                                else:
                                    failures.append(
                                        {
                                            "instancia": instance_name,
                                            "username": username,
                                            "error": resp.get("message", "error"),
                                        }
                                    )

                        failed_count = len(failures)
                        st.success(f"Resumen: creados={created_count}, fallidos={failed_count}")
                        if failures:
                            st.dataframe(pd.DataFrame(failures), use_container_width=True)

with tab_roles:
    st.subheader("Roles por instancia")
    all_instances = instances_dict()
    operable_instances = get_operable_instances(all_instances)
    failed_count = len(all_instances) - len(operable_instances)
    if not all_instances:
        st.info("No hay instancias configuradas.")
    elif not operable_instances:
        st.warning(f"No hay instancias operables. Fallaron autenticación: {failed_count}.")
    else:
        if failed_count:
            st.caption(f"Se excluyeron {failed_count} instancia(s) con auth fallida.")
        selected_name = st.selectbox("Instancia", list(operable_instances.keys()), key="roles_instance")
        headers = get_auth_headers()

        if st.button("Refrescar roles"):
            if not headers:
                st.warning("Completa autenticación.")
            else:
                roles_resp = list_roles(operable_instances[selected_name], headers)
                if is_auth_failure(roles_resp):
                    record_auth(
                        selected_name,
                        operable_instances[selected_name],
                        False,
                        roles_resp.get("status_code"),
                        short_message(roles_resp),
                        str(roles_resp.get("message", "")),
                        "list_roles_tab",
                    )
                st.session_state.roles_data = roles_resp

        roles_resp = st.session_state.get("roles_data", {})
        if roles_resp.get("ok"):
            role_rows = []
            for role_name, role_data in roles_resp.get("data", {}).items():
                role_rows.append(
                    {
                        "role": role_name,
                        "cluster": ", ".join(role_data.get("cluster", [])),
                        "indices_count": len(role_data.get("indices", [])),
                        "applications_count": len(role_data.get("applications", [])),
                        "run_as_count": len(role_data.get("run_as", [])),
                    }
                )
            st.dataframe(pd.DataFrame(role_rows), use_container_width=True)
        elif roles_resp:
            st.error(f"Error: {roles_resp.get('status_code')} - {roles_resp.get('message')}")
