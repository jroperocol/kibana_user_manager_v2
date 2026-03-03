# kibana_user_manager

Aplicación Streamlit para gestionar usuarios y roles en múltiples instancias de Elasticsearch/Kibana.

## Características (MVP)

- Carga de instancias manualmente (tabla editable) o por archivo CSV/JSON.
- Autenticación por sesión:
  - Basic Auth (usuario/contraseña)
  - API Key (`Authorization: ApiKey <key>`)
- Gestión de usuarios:
  - Listar usuarios por instancia
  - Ver roles asignados
  - Crear usuarios individuales
  - Crear usuarios en bulk (textarea o CSV)
  - Eliminar usuarios seleccionados con confirmación extra (`DELETE`)
- Listado de roles por instancia (solo lectura).
- Manejo básico de errores por instancia (401/403/timeout/SSL/otros).

## Estructura

- `app.py` - UI en Streamlit
- `elastic_client.py` - llamadas HTTP a APIs de seguridad de Elasticsearch
- `models.py` - dataclasses base
- `utils_io.py` - carga/validación de CSV/JSON
- `requirements.txt`
- `sample_instances.csv`

## Instalación y ejecución

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

## Formato de instancias

### CSV

Debe incluir columnas `name,base_url`:

```csv
name,base_url
dev-cluster,https://es-dev.example.com:9200
prod-cluster,https://es-prod.example.com:9200
```

### JSON

```json
[
  {"name": "dev-cluster", "base_url": "https://es-dev.example.com:9200"},
  {"name": "prod-cluster", "base_url": "https://es-prod.example.com:9200"}
]
```

## Bulk create (usuarios)

- Textarea: una línea por usuario con formato:

```text
username,password,role1;role2
```

- CSV de bulk: columnas `username,password,roles` donde `roles` usa `;` como separador.

## Notas de seguridad

- **No se guardan contraseñas en disco**.
- Las credenciales se ingresan en la UI y permanecen en memoria (`st.session_state`) durante la sesión activa.
- No subir credenciales, API keys ni archivos sensibles al repositorio.
- Se requiere confirmación explícita para borrados masivos (`DELETE`).
