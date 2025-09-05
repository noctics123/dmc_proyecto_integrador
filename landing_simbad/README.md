# SIMBAD – AAyP Hipotecarios (GCP Cloud Run Job)

Lógica: descarga mensual desde `apis.sb.gob.do` y sube CSV a GCS.
- Job (larga duración): `python -m simbad.runner`
- Servicio HTTP (pruebas): FastAPI `GET /healthz`, `POST /run`

## Env requeridos
- `GCS_BUCKET` (destino)
- `SB_API_KEY` (Secret Manager)
- Opcionales: `LANDING_PREFIX=landing`, `SB_TIPO_ENTIDAD=AAyP`, `SB_CARTERA_OBJ=Créditos Hipotecarios`,
  `SB_START_YEAR=2012`, `SB_DATASET=simbad_carteras_aayp_hipotecarios`, `SB_KEEP_MONTHLY=false`,
  `SB_MAKE_CONSOLIDATED=false`.

## Despliegue Job
Habilita APIs (Run, AR, Build, Secret), crea SA `run-landing-simbad-sa`, otorga `storage.objectCreator` en el bucket y
`secretAccessor` al secreto `sb_api_key`. Luego:

```bash
gcloud builds submit --config=cloudbuild.simbad.job.yaml       --substitutions=_REGION=us-west2,_AR_HOST=us-docker.pkg.dev,_AR_REPO=containers,_IMAGE=simbad-harvester,_JOB=simbad-harvester-job, _SA=run-landing-simbad-sa@$(gcloud config get-value project).iam.gserviceaccount.com,_GCS_BUCKET=TU_BUCKET,_LANDING_PREFIX=landing/simbad, _SB_DATASET=simbad_carteras_aayp_hipotecarios,_SB_TIPO_ENTIDAD=AAyP,_SB_START_YEAR=2012,_SB_KEEP_MONTHLY=false,_SB_API_KEY_SECRET=sb_api_key, _TASK_TIMEOUT=21600s,_TASKS=1,_CPU=2,_MEMORY=2Gi
```

Ejecutar de nuevo:
```bash
gcloud run jobs execute simbad-harvester-job --region us-west2
```

## Servicio HTTP (opcional)
Despliega con tu propio cloudbuild (no incluido aquí) o `docker run` local. Autenticación OIDC necesaria en Cloud Run.
