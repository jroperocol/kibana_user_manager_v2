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

I18N = {
    "app_title": {"ES": "kibana_user_manager", "EN": "kibana_user_manager", "PT": "kibana_user_manager"},
    "app_caption": {
        "ES": "Gestión masiva de usuarios/roles en múltiples instancias Elasticsearch/Kibana",
        "EN": "Bulk management of users/roles across multiple Elasticsearch/Kibana instances",
        "PT": "Gestão em massa de usuários/funções em múltiplas instâncias Elasticsearch/Kibana",
    },
    "tab_users": {"ES": "Usuarios", "EN": "Users", "PT": "Usuários"},
    "tab_create": {"ES": "Crear usuarios", "EN": "Create users", "PT": "Criar usuários"},
    "tab_roles": {"ES": "Roles", "EN": "Roles", "PT": "Perfis"},
    "language": {"ES": "Idioma", "EN": "Language", "PT": "Idioma"},
    "instances": {"ES": "Instancias", "EN": "Instances", "PT": "Instâncias"},
    "import_file": {"ES": "Importar CSV/JSON", "EN": "Import CSV/JSON", "PT": "Importar CSV/JSON"},
    "auth_verify_all": {
        "ES": "Verificar autenticación (todas las instancias)",
        "EN": "Verify authentication (all instances)",
        "PT": "Verificar autenticação (todas as instâncias)",
    },
    "auth_verified_progress": {"ES": "{done}/{total} verificadas", "EN": "{done}/{total} verified", "PT": "{done}/{total} verificadas"},
    "auth_summary_done": {"ES": "Verificación finalizada. OK: {ok} | FAIL: {fail}", "EN": "Verification finished. OK: {ok} | FAIL: {fail}", "PT": "Verificação concluída. OK: {ok} | FAIL: {fail}"},
    "status": {"ES": "Estado", "EN": "Status", "PT": "Status"},
    "download_auth_report": {
        "ES": "Descargar reporte autenticación (Excel)",
        "EN": "Download authentication report (Excel)",
        "PT": "Baixar relatório de autenticação (Excel)",
    },
    "download_logs": {"ES": "Descargar logs (xlsx o csv)", "EN": "Download logs (xlsx or csv)", "PT": "Baixar logs (xlsx ou csv)"},
    "show_logs": {"ES": "Mostrar logs", "EN": "Show logs", "PT": "Mostrar logs"},
    "auth_section": {"ES": "Autenticación", "EN": "Authentication", "PT": "Autenticação"},
    "method": {"ES": "Método", "EN": "Method", "PT": "Método"},
    "apply_credentials": {
        "ES": "Aplicar credenciales / Refresh credentials",
        "EN": "Apply credentials / Refresh credentials",
        "PT": "Aplicar credenciais / Refresh credentials",
    },
    "credentials_applied": {
        "ES": "Credenciales aplicadas. Se limpió el estado dependiente de autenticación.",
        "EN": "Credentials applied. Auth-dependent state has been reset.",
        "PT": "Credenciais aplicadas. O estado dependente de autenticação foi limpo.",
    },
    "global_search_delete": {"ES": "Buscar/Borrar usuario global", "EN": "Global user search/delete", "PT": "Buscar/Excluir usuário global"},
    "reset_delete_section": {
        "ES": "Reset sección de borrado / New deletion",
        "EN": "Reset deletion section / New deletion",
        "PT": "Reset seção de exclusão / Nova exclusão",
    },
    "download_delete_report": {
        "ES": "Descargar reporte Excel / Download Excel report",
        "EN": "Download Excel report",
        "PT": "Baixar relatório Excel",
    },
    "users_list_title": {"ES": "Listado de usuarios", "EN": "User list", "PT": "Lista de usuários"},
    "username_label": {"ES": "Username", "EN": "Username", "PT": "Username"},
    "password_label": {"ES": "Password", "EN": "Password", "PT": "Password"},
    "search_user_label": {
        "ES": "Buscar usuario (username, full_name, email)",
        "EN": "Search user (username, full_name, email)",
        "PT": "Buscar usuário (username, full_name, email)",
    },
    "users_multi_report_title": {
        "ES": "Reporte multi-instancia de usuarios",
        "EN": "Multi-instance users report",
        "PT": "Relatório multi-instância de usuários",
    },
    "all_authenticated_instances": {
        "ES": "Todas las instancias autenticadas",
        "EN": "All authenticated instances",
        "PT": "Todas as instâncias autenticadas",
    },
    "run_report": {"ES": "Run report", "EN": "Run report", "PT": "Run report"},
    "clear_report": {"ES": "Clear report", "EN": "Clear report", "PT": "Clear report"},
    "download_users_report_excel": {
        "ES": "Descargar reporte usuarios (Excel)",
        "EN": "Download users report (Excel)",
        "PT": "Baixar relatório de usuários (Excel)",
    },
    "users_report_suggestion": {
        "ES": "Selecciona instancias y ejecuta Run report.",
        "EN": "Select instances and run report.",
        "PT": "Selecione instâncias e execute Run report.",
    },
    "default_superusers_select_col": {"ES": "selected", "EN": "selected", "PT": "selected"},
    "default_superusers_selected_summary": {
        "ES": "Seleccionados {selected} de {total} usuarios default",
        "EN": "Selected {selected} of {total} default users",
        "PT": "Selecionados {selected} de {total} usuários padrão",
    },
    "default_superusers_no_selection": {
        "ES": "Selecciona al menos un usuario default para crear.",
        "EN": "Select at least one default user to create.",
        "PT": "Selecione pelo menos um usuário padrão para criar.",
    },
    "default_create_running": {
        "ES": "Creando usuarios default... {done}/{total}",
        "EN": "Creating default users... {done}/{total}",
        "PT": "Criando usuários padrão... {done}/{total}",
    },
    "default_create_summary": {
        "ES": "Seleccionados={selected} | instancias={instances} | intentos={attempted} | creados={created} | already_exists={exists} | fallidos={failed}",
        "EN": "Selected={selected} | instances={instances} | attempts={attempted} | created={created} | already_exists={exists} | failed={failed}",
        "PT": "Selecionados={selected} | instâncias={instances} | tentativas={attempted} | criados={created} | already_exists={exists} | falhas={failed}",
    },
    "download_default_create_report": {
        "ES": "Download Excel report",
        "EN": "Download Excel report",
        "PT": "Download Excel report",
    },
}


def t(key: str, **kwargs: object) -> str:
    lang = st.session_state.get("lang", "ES")
    base = I18N.get(key, {})
    text = base.get(lang) or base.get("ES") or key
    if kwargs:
        return text.format(**kwargs)
    return text


st.set_page_config(page_title="kibana_user_manager", layout="wide")
header_left, header_right = st.columns([6, 1])
with header_right:
    st.selectbox(t("language"), options=["ES", "EN", "PT"], key="lang")

st.title(t("app_title"))
st.caption(t("app_caption"))
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
if "auth_report_df" not in st.session_state:
    st.session_state.auth_report_df = pd.DataFrame()
if "auth_logs" not in st.session_state:
    st.session_state.auth_logs = []
if "global_search_results" not in st.session_state:
    st.session_state.global_search_results = []
if "users_multi_report_rows" not in st.session_state:
    st.session_state.users_multi_report_rows = []
if "default_create_last_rows" not in st.session_state:
    st.session_state.default_create_last_rows = []
if "lang" not in st.session_state:
    st.session_state.lang = "ES"
if "auth_input_username" not in st.session_state:
    st.session_state.auth_input_username = st.session_state.auth.get("username", "")
if "auth_input_password" not in st.session_state:
    st.session_state.auth_input_password = st.session_state.auth.get("password", "")
if "auth_input_api_key" not in st.session_state:
    st.session_state.auth_input_api_key = st.session_state.auth.get("api_key", "")


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


def truncate_detail(value: object, max_len: int = 600) -> str:
    text = str(value or "")
    return text[:max_len] + ("..." if len(text) > max_len else "")


def short_message(resp: Dict[str, Any]) -> str:
    status_code = resp.get("status_code")
    if status_code == 401:
        return "Unauthorized"
    if status_code == 403:
        return "Forbidden"

    message = str(resp.get("message") or "").strip().lower()
    if "timeout" in message:
        return "Timeout"
    if "ssl" in message:
        return "SSL error"
    if "not found" in message:
        return "Not Found"
    if message:
        return str(resp.get("message")).splitlines()[0][:120]
    if resp.get("ok"):
        return "OK"
    return "Error"


def is_auth_failure(resp: Dict[str, Any]) -> bool:
    return resp.get("status_code") in (401, 403)


def log_event(
    instance: str,
    base_url: str,
    action: str,
    status_code: object,
    message_short: str,
    detail: object,
) -> None:
    st.session_state.auth_logs.append(
        {
            "ts": datetime.utcnow().isoformat(timespec="seconds"),
            "instance": instance,
            "base_url": base_url,
            "action": action,
            "status_code": status_code,
            "message_short": message_short,
            "detail_trunc": truncate_detail(detail, 1000),
        }
    )


def upsert_instance_auth(instance_name: str, base_url: str, auth_ok: bool, status_code: object, message_short_text: str) -> None:
    st.session_state.instance_auth[base_url] = {
        "name": instance_name,
        "auth_ok": bool(auth_ok),
        "status_code": status_code,
        "message_short": message_short_text,
        "last_checked": datetime.utcnow().isoformat(timespec="seconds"),
    }


def build_auth_report_df() -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    for item in st.session_state.instances:
        auth_state = st.session_state.instance_auth.get(item["base_url"])
        if auth_state:
            rows.append(
                {
                    "name": item["name"],
                    "base_url": item["base_url"],
                    "auth_ok": bool(auth_state.get("auth_ok", False)),
                    "status_code": auth_state.get("status_code", ""),
                    "message_short": auth_state.get("message_short", ""),
                    "last_checked": auth_state.get("last_checked", ""),
                }
            )
        else:
            rows.append(
                {
                    "name": item["name"],
                    "base_url": item["base_url"],
                    "auth_ok": "",
                    "status_code": "",
                    "message_short": "No verificado",
                    "last_checked": "",
                }
            )
    return pd.DataFrame(rows)


def refresh_auth_report_df() -> None:
    st.session_state.auth_report_df = build_auth_report_df()


def has_auth_report() -> bool:
    return not st.session_state.auth_report_df.empty


def get_operable_instances(all_instances: Dict[str, str]) -> Dict[str, str]:
    if not has_auth_report():
        return all_instances

    operable: Dict[str, str] = {}
    for name, base_url in all_instances.items():
        auth_state = st.session_state.instance_auth.get(base_url)
        if auth_state and bool(auth_state.get("auth_ok", False)):
            operable[name] = base_url
    return operable


def handle_auth_response(instance_name: str, base_url: str, action: str, resp: Dict[str, Any]) -> None:
    message_short_text = short_message(resp)
    if resp.get("ok"):
        upsert_instance_auth(instance_name, base_url, True, resp.get("status_code"), message_short_text)
    elif is_auth_failure(resp):
        upsert_instance_auth(instance_name, base_url, False, resp.get("status_code"), message_short_text)
    log_event(
        instance=instance_name,
        base_url=base_url,
        action=action,
        status_code=resp.get("status_code"),
        message_short=message_short_text,
        detail=resp.get("message") or resp.get("data") or "",
    )
    refresh_auth_report_df()


def build_auth_report_excel() -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        st.session_state.auth_report_df.to_excel(writer, index=False, sheet_name="auth_status")
        if st.session_state.auth_logs:
            pd.DataFrame(st.session_state.auth_logs).to_excel(writer, index=False, sheet_name="logs")
    return buffer.getvalue()


def build_logs_csv() -> bytes:
    if not st.session_state.auth_logs:
        return b""
    return pd.DataFrame(st.session_state.auth_logs).to_csv(index=False).encode("utf-8")


def reset_auth_dependent_state() -> None:
    st.session_state.instance_auth = {}
    st.session_state.auth_report_df = pd.DataFrame()
    st.session_state.auth_logs = []
    st.session_state.users_data = {}
    st.session_state.roles_data = {}


def reset_delete_section_state() -> None:
    delete_keys = [
        "delete_query",
        "delete_results",
        "delete_selected_matches",
        "delete_confirm_text",
        "delete_confirm_check",
        "delete_all_matches",
        "delete_last_report_rows",
        "delete_last_target_user",
    ]
    for key in delete_keys:
        if key in st.session_state:
            del st.session_state[key]


def build_delete_report_excel() -> bytes:
    rows = st.session_state.get("delete_last_report_rows", [])
    if not rows:
        return b""
    df = pd.DataFrame(rows)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="delete_report")
    return buffer.getvalue()


def build_users_multi_report_excel(rows: List[Dict[str, object]]) -> bytes:
    if not rows:
        return b""
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, index=False, sheet_name="users")
    return buffer.getvalue()


def build_default_create_report_excel(rows: List[Dict[str, object]]) -> bytes:
    if not rows:
        return b""
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, index=False, sheet_name="create_default_users")
    return buffer.getvalue()


with st.sidebar:
    st.header(t("instances"))

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

    import_file = st.file_uploader(t("import_file"), type=["csv", "json"])
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
            st.success(f"Instancias importadas: {len(loaded)}")

        if errors:
            st.caption(f"Importación con {len(errors)} advertencia(s). Revisa logs si hace falta.")
            for err in errors:
                log_event("import", "-", "import_instances", None, "Import warning", err)

    headers = get_auth_headers()

    if st.button(t("auth_verify_all"), type="primary"):
        if not headers:
            st.warning("Configura autenticación antes de verificar.")
        elif not st.session_state.instances:
            st.warning("Agrega al menos una instancia.")
        else:
            total = len(st.session_state.instances)
            progress = st.progress(0)
            progress_text = st.empty()
            table_placeholder = st.empty()
            running_rows: List[Dict[str, object]] = []

            for index, item in enumerate(st.session_state.instances, start=1):
                resp = test_connection(item["base_url"], headers)
                message_short_text = short_message(resp)
                auth_ok = bool(resp.get("ok"))
                upsert_instance_auth(item["name"], item["base_url"], auth_ok, resp.get("status_code"), message_short_text)
                log_event(
                    instance=item["name"],
                    base_url=item["base_url"],
                    action="verify_auth",
                    status_code=resp.get("status_code"),
                    message_short=message_short_text,
                    detail=resp.get("message") or resp.get("data") or "",
                )
                running_rows.append(
                    {
                        "name": item["name"],
                        "base_url": item["base_url"],
                        "auth_ok": auth_ok,
                        "status_code": resp.get("status_code", ""),
                        "message_short": message_short_text,
                    }
                )
                table_placeholder.dataframe(pd.DataFrame(running_rows), use_container_width=True)
                progress.progress(index / total)
                progress_text.caption(t("auth_verified_progress", done=index, total=total))

            refresh_auth_report_df()
            report_df = st.session_state.auth_report_df
            ok_count = int((report_df["auth_ok"] == True).sum()) if not report_df.empty else 0
            fail_count = int((report_df["auth_ok"] == False).sum()) if not report_df.empty else 0
            st.success(t("auth_summary_done", ok=ok_count, fail=fail_count))

    st.divider()
    st.subheader(t("status"))

    if has_auth_report():
        report_df = st.session_state.auth_report_df
        ok_count = int((report_df["auth_ok"] == True).sum())
        fail_count = int((report_df["auth_ok"] == False).sum())
        st.caption(f"Instancias autenticadas: {ok_count} | No autenticadas: {fail_count}")
        st.dataframe(report_df[["name", "base_url", "auth_ok", "status_code", "message_short"]], use_container_width=True)

        st.download_button(
            t("download_auth_report"),
            data=build_auth_report_excel(),
            file_name="auth_report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        st.caption("Sin reporte de autenticación. Ejecuta verificación para filtrar instancias no autenticadas.")

    if st.session_state.auth_logs:
        st.download_button(
            t("download_logs"),
            data=build_logs_csv(),
            file_name="auth_logs.csv",
            mime="text/csv",
        )

    show_logs = st.checkbox(t("show_logs"), value=False)
    if show_logs:
        with st.expander("Logs", expanded=True):
            st.dataframe(pd.DataFrame(st.session_state.auth_logs[-300:]), use_container_width=True)

    st.divider()
    st.header(t("auth_section"))

    mode = st.radio(t("method"), ["Basic Auth", "API Key"], index=0 if st.session_state.auth["mode"] == "Basic Auth" else 1)
    st.session_state.auth["mode"] = mode

    if mode == "Basic Auth":
        st.text_input(t("username_label"), value=st.session_state.auth_input_username, key="auth_input_username")
        st.text_input(t("password_label"), type="password", value=st.session_state.auth_input_password, key="auth_input_password")
    else:
        st.text_input("API Key", type="password", value=st.session_state.auth_input_api_key, key="auth_input_api_key")

    if st.button(t("apply_credentials"), key="apply_credentials_btn_sidebar"):
        st.session_state.auth["username"] = st.session_state.get("auth_input_username", "")
        st.session_state.auth["password"] = st.session_state.get("auth_input_password", "")
        st.session_state.auth["api_key"] = st.session_state.get("auth_input_api_key", "")
        reset_auth_dependent_state()
        st.success(t("credentials_applied"))


tab_users, tab_create, tab_roles = st.tabs([t("tab_users"), t("tab_create"), t("tab_roles")])

with tab_users:
    st.subheader(t("users_list_title"))
    all_instances = instances_dict()
    operable_instances = get_operable_instances(all_instances)

    if has_auth_report():
        not_auth_count = len(all_instances) - len(operable_instances)
        if not_auth_count > 0:
            st.caption(f"{not_auth_count} instancias no autenticadas. Ver reporte para detalles.")
    else:
        st.caption("Sugerencia: ejecuta 'Verificar autenticación (todas las instancias)' para filtrar instancias no autenticadas.")

    if not all_instances:
        st.info("No hay instancias configuradas.")
    elif not operable_instances:
        st.warning("No hay instancias autenticadas para operar.")
    else:
        selected_name = st.selectbox("Instancia", list(operable_instances.keys()), key="users_instance")
        base_url = operable_instances[selected_name]
        headers = get_auth_headers()

        if st.button("Refrescar", key="refresh_users"):
            if not headers:
                st.warning("Completa autenticación.")
            else:
                users_resp = list_users(base_url, headers)
                handle_auth_response(selected_name, base_url, "list_users", users_resp)
                st.session_state.users_data = users_resp

        users_resp = st.session_state.get("users_data", {})
        if users_resp.get("ok"):
            users_map = users_resp.get("data", {})
            user_rows = []
            for username, payload in users_map.items():
                user_rows.append(
                    {
                        "username": username,
                        "full_name": payload.get("full_name", ""),
                        "email": payload.get("email", ""),
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
                elif not headers:
                    st.warning("Completa autenticación.")
                else:
                    results = []
                    for username in selected_to_delete:
                        resp = delete_user(base_url, headers, username)
                        handle_auth_response(selected_name, base_url, "delete_user", resp)
                        results.append(
                            {
                                "instancia": selected_name,
                                "usuario": username,
                                "ok": resp.get("ok"),
                                "status": resp.get("status_code"),
                                "mensaje": short_message(resp),
                            }
                        )
                    st.dataframe(pd.DataFrame(results), use_container_width=True)
        elif users_resp:
            st.warning(f"No se pudo listar usuarios ({short_message(users_resp)}).")

    st.markdown("---")
    st.markdown(f"#### {t('users_multi_report_title')}")
    report_all_instances = st.checkbox(t("all_authenticated_instances"), value=False, key="users_report_all_instances")
    report_instance_options = list(operable_instances.keys())
    report_selected_instances = st.multiselect(
        "Instancias",
        options=report_instance_options,
        default=report_instance_options if report_all_instances else [],
        key="users_report_instances",
        disabled=report_all_instances,
    )

    report_targets = report_instance_options if report_all_instances else report_selected_instances

    cols_report_actions = st.columns(2)
    with cols_report_actions[0]:
        if st.button(t("run_report"), key="run_users_multi_report"):
            headers = get_auth_headers()
            if not headers:
                st.warning("Completa autenticación.")
            elif not report_targets:
                st.warning(t("users_report_suggestion"))
            else:
                report_rows = []
                total_targets = len(report_targets)
                progress = st.progress(0)
                for index, instance_name in enumerate(report_targets, start=1):
                    instance_url = operable_instances.get(instance_name)
                    if not instance_url:
                        continue
                    users_resp = list_users(instance_url, headers)
                    handle_auth_response(instance_name, instance_url, "users_multi_report", users_resp)
                    if users_resp.get("ok"):
                        users_map = users_resp.get("data", {})
                        for username, payload in users_map.items():
                            report_rows.append(
                                {
                                    "instance_name": instance_name,
                                    "base_url": instance_url,
                                    "username": username,
                                    "full_name": payload.get("full_name", ""),
                                    "email": payload.get("email", ""),
                                    "enabled": payload.get("enabled", True),
                                    "roles": ", ".join(payload.get("roles", [])),
                                }
                            )
                    progress.progress(index / total_targets)
                st.session_state.users_multi_report_rows = report_rows

    with cols_report_actions[1]:
        if st.button(t("clear_report"), key="clear_users_multi_report"):
            st.session_state.users_multi_report_rows = []

    if st.session_state.get("users_multi_report_rows"):
        users_multi_df = pd.DataFrame(st.session_state.users_multi_report_rows)
        st.dataframe(users_multi_df, use_container_width=True)
        st.download_button(
            t("download_users_report_excel"),
            data=build_users_multi_report_excel(st.session_state.users_multi_report_rows),
            file_name="users_report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    st.markdown("---")
    st.markdown(f"#### {t('global_search_delete')}")
    cols_delete_hdr = st.columns([3, 2])
    with cols_delete_hdr[1]:
        if st.button(t("reset_delete_section")):
            reset_delete_section_state()
            st.success("OK")

    global_query = st.text_input(t("search_user_label"), key="delete_query")

    if st.button("Buscar en instancias autenticadas"):
        headers = get_auth_headers()
        if not headers:
            st.warning("Completa autenticación.")
        elif not global_query.strip():
            st.warning("Ingresa un texto de búsqueda.")
        else:
            query = global_query.strip().lower()
            search_rows = []
            target_instances = get_operable_instances(instances_dict())
            total = len(target_instances)
            progress = st.progress(0)
            for index, (instance_name, instance_url) in enumerate(target_instances.items(), start=1):
                resp = list_users(instance_url, headers)
                handle_auth_response(instance_name, instance_url, "global_search_list_users", resp)
                if resp.get("ok"):
                    users_map = resp.get("data", {})
                    for username, payload in users_map.items():
                        full_name = str(payload.get("full_name", ""))
                        email = str(payload.get("email", ""))
                        haystack = f"{username} {full_name} {email}".lower()
                        if query in haystack:
                            search_rows.append(
                                {
                                    "match_id": f"{instance_name}::{username}",
                                    "instance_name": instance_name,
                                    "base_url": instance_url,
                                    "username": username,
                                    "full_name": full_name,
                                    "email": email,
                                    "roles": ", ".join(payload.get("roles", [])),
                                }
                            )
                progress.progress(index / total if total else 1.0)

            st.session_state.delete_results = search_rows
            st.success(f"Búsqueda completada. Coincidencias: {len(search_rows)}")

    global_results = st.session_state.get("delete_results", [])
    if global_results:
        global_df = pd.DataFrame(global_results)
        st.dataframe(global_df[["instance_name", "base_url", "username", "full_name", "email", "roles"]], use_container_width=True)

        delete_all_matches = st.checkbox("Borrar todas las coincidencias", value=False, key="delete_all_matches")
        selected_matches = []
        if not delete_all_matches:
            selected_matches = st.multiselect(
                "Seleccionar coincidencias a borrar",
                options=global_df["match_id"].tolist(),
                key="delete_selected_matches",
            )

        delete_confirm_text = st.text_input("Confirmación fuerte: escribe DELETE", key="delete_confirm_text")
        delete_confirm_check = st.checkbox(
            "Confirmo borrar este usuario en todas las instancias seleccionadas",
            value=False,
            key="delete_confirm_check",
        )

        if st.button("Eliminar coincidencias seleccionadas", type="primary"):
            headers = get_auth_headers()
            if not headers:
                st.warning("Completa autenticación.")
            elif delete_confirm_text != "DELETE" or not delete_confirm_check:
                st.error("Debes confirmar con DELETE y checkbox.")
            else:
                target_ids = set(global_df["match_id"].tolist()) if delete_all_matches else set(selected_matches)
                targets = [row for row in global_results if row["match_id"] in target_ids]
                if not targets:
                    st.warning("No hay coincidencias seleccionadas para borrar.")
                else:
                    progress = st.progress(0)
                    final_results = []
                    total = len(targets)
                    deleted_usernames = sorted({row["username"] for row in targets})
                    for index, row in enumerate(targets, start=1):
                        resp = delete_user(row["base_url"], headers, row["username"])
                        handle_auth_response(row["instance_name"], row["base_url"], "global_delete_user", resp)
                        final_results.append(
                            {
                                "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
                                "deleted_user": row["username"],
                                "instancia": row["instance_name"],
                                "base_url": row["base_url"],
                                "usuario": row["username"],
                                "resultado": "deleted" if resp.get("ok") else ("not_found" if resp.get("status_code") == 404 else "failed"),
                                "status_code": resp.get("status_code"),
                                "error": short_message(resp) if not resp.get("ok") else "",
                            }
                        )
                        progress.progress(index / total)

                    st.success("Borrado global finalizado.")
                    st.dataframe(pd.DataFrame(final_results), use_container_width=True)
                    st.session_state.delete_last_report_rows = final_results
                    st.session_state.delete_last_target_user = ", ".join(deleted_usernames)

    if st.session_state.get("delete_last_report_rows"):
        st.download_button(
            t("download_delete_report"),
            data=build_delete_report_excel(),
            file_name="delete_report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

with tab_create:
    st.subheader("Crear usuarios")
    all_instances = instances_dict()
    operable_instances = get_operable_instances(all_instances)

    if has_auth_report():
        not_auth_count = len(all_instances) - len(operable_instances)
        if not_auth_count > 0:
            st.caption(f"{not_auth_count} instancias no autenticadas. Ver reporte para detalles.")
    else:
        st.caption("Sugerencia: ejecuta verificación de autenticación para filtrar instancias no autenticadas.")

    if not all_instances:
        st.info("No hay instancias configuradas.")
    elif not operable_instances:
        st.warning("No hay instancias autenticadas para operar.")
    else:
        target = st.selectbox("Instancia destino", ["Todas"] + list(operable_instances.keys()), key="create_instance")
        headers = get_auth_headers()

        if not headers:
            st.warning("Completa autenticación para consultar roles y crear usuarios.")
        else:
            target_instances = list(operable_instances.items()) if target == "Todas" else [(target, operable_instances[target])]

            all_role_names = set()
            role_errors = 0
            for instance_name, instance_url in target_instances:
                roles_resp = list_roles(instance_url, headers)
                handle_auth_response(instance_name, instance_url, "list_roles", roles_resp)
                if roles_resp.get("ok"):
                    all_role_names.update(roles_resp.get("data", {}).keys())
                else:
                    role_errors += 1

            if role_errors:
                st.caption(f"Roles no disponibles en {role_errors} instancia(s).")

            available_roles = sorted(all_role_names)
            st.caption("Roles disponibles")
            st.write(available_roles if available_roles else "No se pudieron cargar roles.")

            st.markdown("#### Crear un usuario")
            with st.form("single_create_form"):
                new_username = st.text_input(t("username_label"))
                new_password = st.text_input(t("password_label"), type="password")
                new_roles = st.multiselect("Roles", options=available_roles)
                submit_single = st.form_submit_button("Crear usuario")

            if submit_single:
                if not new_username or not new_password:
                    st.error("Username y password son obligatorios.")
                else:
                    results = []
                    for instance_name, instance_url in target_instances:
                        resp = create_user(instance_url, headers, new_username, new_password, new_roles)
                        handle_auth_response(instance_name, instance_url, "create_user_single", resp)
                        results.append(
                            {
                                "instancia": instance_name,
                                "usuario": new_username,
                                "ok": resp.get("ok"),
                                "status": resp.get("status_code"),
                                "mensaje": short_message(resp),
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
                            handle_auth_response(instance_name, instance_url, "create_user_bulk", resp)
                            results.append(
                                {
                                    "instancia": instance_name,
                                    "usuario": entry["username"],
                                    "ok": resp.get("ok"),
                                    "status": resp.get("status_code"),
                                    "mensaje": short_message(resp),
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
                    st.session_state.default_superusers_table = [{**dict(row), "selected": False} for row in DEFAULT_SUPERUSERS]

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

                selected_default_rows = [
                    row for row in st.session_state.default_superusers_table if bool(row.get("selected", False))
                ]
                st.caption(
                    t(
                        "default_superusers_selected_summary",
                        selected=len(selected_default_rows),
                        total=len(st.session_state.default_superusers_table),
                    )
                )

                confirm_default_superusers = st.checkbox(
                    "Confirmo que quiero crear usuarios SUPERUSER en la(s) instancia(s) seleccionada(s).",
                    value=False,
                )

                if st.button("Crear usuarios default", type="primary"):
                    if not confirm_default_superusers:
                        st.error("Debes confirmar antes de crear usuarios SUPERUSER.")
                    elif not selected_default_rows:
                        st.warning(t("default_superusers_no_selection"))
                    else:
                        created_count = 0
                        already_exists_count = 0
                        results = []
                        target_instances_count = len(target_instances)
                        selected_count = len(selected_default_rows)
                        attempted_count = selected_count * target_instances_count
                        progress = st.progress(0)
                        status_line = st.empty()
                        done_count = 0

                        for instance_name, instance_url in target_instances:
                            instance_users_resp = list_users(instance_url, headers)
                            handle_auth_response(instance_name, instance_url, "list_users_before_create_default", instance_users_resp)
                            instance_users_map = instance_users_resp.get("data", {}) if instance_users_resp.get("ok") else {}

                            for row in selected_default_rows:
                                username = str(row.get("username", "")).strip()
                                password = str(row.get("password", "")).strip()
                                full_name = str(row.get("full_name", "")).strip()
                                email = str(row.get("email", "")).strip()
                                roles = parse_roles(row.get("roles", ""))
                                done_count += 1
                                progress.progress(done_count / attempted_count if attempted_count else 1.0)
                                status_line.caption(t("default_create_running", done=done_count, total=attempted_count))

                                if not username or not password:
                                    results.append(
                                        {
                                            "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
                                            "instancia": instance_name,
                                            "base_url": instance_url,
                                            "username": username or "(vacío)",
                                            "resultado": "failed",
                                            "status_code": "",
                                            "mensaje": "username/password requeridos",
                                        }
                                    )
                                    continue

                                if username in instance_users_map:
                                    already_exists_count += 1
                                    results.append(
                                        {
                                            "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
                                            "instancia": instance_name,
                                            "base_url": instance_url,
                                            "username": username,
                                            "resultado": "already_exists",
                                            "status_code": "",
                                            "mensaje": "already_exists",
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
                                handle_auth_response(instance_name, instance_url, "create_user_default", resp)

                                if resp.get("ok"):
                                    created_count += 1
                                    results.append(
                                        {
                                            "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
                                            "instancia": instance_name,
                                            "base_url": instance_url,
                                            "username": username,
                                            "resultado": "created",
                                            "status_code": resp.get("status_code"),
                                            "mensaje": short_message(resp),
                                        }
                                    )
                                else:
                                    message_lc = str(resp.get("message") or "").lower()
                                    is_already_exists = resp.get("status_code") == 409 or "already exists" in message_lc or "resource_already_exists_exception" in message_lc
                                    if is_already_exists:
                                        already_exists_count += 1
                                        result_category = "already_exists"
                                    else:
                                        result_category = "failed"
                                    results.append(
                                        {
                                            "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
                                            "instancia": instance_name,
                                            "base_url": instance_url,
                                            "username": username,
                                            "resultado": result_category,
                                            "status_code": resp.get("status_code"),
                                            "mensaje": short_message(resp),
                                        }
                                    )

                        failed_count = len([row for row in results if row.get("resultado") == "failed"])
                        st.success(t("default_create_summary", selected=selected_count, instances=target_instances_count, attempted=attempted_count, created=created_count, exists=already_exists_count, failed=failed_count))
                        if results:
                            st.dataframe(pd.DataFrame(results), use_container_width=True, height=400)
                            st.session_state.default_create_last_rows = results

                if st.session_state.get("default_create_last_rows"):
                    st.download_button(
                        t("download_default_create_report"),
                        data=build_default_create_report_excel(st.session_state.default_create_last_rows),
                        file_name="create_default_users_report.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="download_default_create_report_btn",
                    )

with tab_roles:
    st.subheader("Roles por instancia")
    all_instances = instances_dict()
    operable_instances = get_operable_instances(all_instances)

    if has_auth_report():
        not_auth_count = len(all_instances) - len(operable_instances)
        if not_auth_count > 0:
            st.caption(f"{not_auth_count} instancias no autenticadas. Ver reporte para detalles.")
    else:
        st.caption("Sugerencia: ejecuta verificación de autenticación para filtrar instancias no autenticadas.")

    if not all_instances:
        st.info("No hay instancias configuradas.")
    elif not operable_instances:
        st.warning("No hay instancias autenticadas para operar.")
    else:
        selected_name = st.selectbox("Instancia", list(operable_instances.keys()), key="roles_instance")
        headers = get_auth_headers()

        if st.button("Refrescar roles"):
            if not headers:
                st.warning("Completa autenticación.")
            else:
                roles_resp = list_roles(operable_instances[selected_name], headers)
                handle_auth_response(selected_name, operable_instances[selected_name], "list_roles_tab", roles_resp)
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
            st.warning(f"No se pudieron listar roles ({short_message(roles_resp)}).")
