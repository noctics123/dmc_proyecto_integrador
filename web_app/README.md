# DMC Pipeline Web App

## 🚀 Cómo Ejecutar Localmente

### Prerrequisitos
- Node.js 18+ 
- Python 3.11+
- GCP credentials configuradas

### Opción 1: Solo Frontend (Mock Data)
```bash
cd web_app/frontend
npm install
npm start
```
La app se abrirá en: http://localhost:3000

### Opción 2: Frontend + Backend Completo
```bash
# Terminal 1 - Backend
cd web_app/backend
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# Terminal 2 - Frontend  
cd web_app/frontend
npm install
npm start
```

### Opción 3: Docker (Recomendado)
```bash
# Solo backend
cd web_app/backend
docker build -t dmc-backend .
docker run -p 8000:8000 dmc-backend

# Luego frontend normal
cd ../frontend
npm install && npm start
```

## 🌐 Opciones de Deploy

### GitHub Pages (Solo Frontend - Demo)
Para una demo estática sin backend:
```bash
cd web_app/frontend
npm run build
# Copiar build/ a docs/ del repo principal
```

### Vercel (Frontend + API Routes)
```bash
npm install -g vercel
cd web_app/frontend
vercel --prod
```

### Netlify (Frontend)
```bash
cd web_app/frontend
npm run build
# Drag & drop build/ folder to netlify.com
```

### Google Cloud Run (Producción)
```bash
# Ya configurado en cloudbuild.etl-pipeline.yaml
git tag webapp-v1.0.0
git push origin webapp-v1.0.0
```

## 🔧 Variables de Entorno

### Backend (.env)
```
PROJECT_ID=proyecto-integrador-dae-2025
REGION=us-central1
BUCKET=dae-integrador-2025
```

### Frontend (.env.local)
```
REACT_APP_API_URL=http://localhost:8000
REACT_APP_PROJECT_ID=proyecto-integrador-dae-2025
```