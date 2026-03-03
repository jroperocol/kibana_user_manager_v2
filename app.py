from __future__ import annotations

import base64
import csv
import io
from typing import Dict, List

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
            st.success(f"Instancias importadas: {len(loaded)}")
        for err in errors:
            st.error(err)

    if st.button("Probar conexión"):
        headers = get_auth_headers()
        if not headers:
            st.warning("Configura autenticación antes de probar conexión.")
        elif not st.session_state.instances:
            st.warning("Agrega al menos una instancia.")
        else:
            for item in st.session_state.instances:
                resp = test_connection(item["base_url"], headers)
                if resp.get("ok"):
                    st.success(f"{item['name']}: OK")
                else:
                    st.error(f"{item['name']}: Error ({resp.get('status_code')}) - {resp.get('message')}")

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
    if not all_instances:
        st.info("No hay instancias configuradas.")
    else:
        selected_name = st.selectbox("Instancia", list(all_instances.keys()), key="users_instance")
        base_url = all_instances[selected_name]
        headers = get_auth_headers()

        if st.button("Refrescar", key="refresh_users"):
            if not headers:
                st.warning("Completa autenticación.")
            else:
                users_resp = list_users(base_url, headers)
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
    if not all_instances:
        st.info("No hay instancias configuradas.")
    else:
        target = st.selectbox("Instancia destino", ["Todas"] + list(all_instances.keys()), key="create_instance")
        headers = get_auth_headers()

        if not headers:
            st.warning("Completa autenticación para consultar roles y crear usuarios.")
        else:
            target_instances = list(all_instances.items()) if target == "Todas" else [(target, all_instances[target])]

            all_role_names = set()
            role_errors = []
            for instance_name, instance_url in target_instances:
                roles_resp = list_roles(instance_url, headers)
                if roles_resp.get("ok"):
                    all_role_names.update(roles_resp.get("data", {}).keys())
                else:
                    role_errors.append(
                        f"{instance_name}: {roles_resp.get('status_code')} - {roles_resp.get('message')}"
                    )

            for err in role_errors:
                st.error(f"Error cargando roles: {err}")

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
    if not all_instances:
        st.info("No hay instancias configuradas.")
    else:
        selected_name = st.selectbox("Instancia", list(all_instances.keys()), key="roles_instance")
        headers = get_auth_headers()

        if st.button("Refrescar roles"):
            if not headers:
                st.warning("Completa autenticación.")
            else:
                roles_resp = list_roles(all_instances[selected_name], headers)
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
