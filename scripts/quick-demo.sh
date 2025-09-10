#!/bin/bash

# Quick Demo Setup for DMC Pipeline Web App
# This script sets up the web app for local testing

set -e

echo "🚀 DMC Pipeline - Quick Demo Setup"
echo "===================================="
echo ""

# Check if we're in the right directory
if [ ! -f "web_app/frontend/package.json" ]; then
    echo "❌ Error: Please run this script from the project root directory"
    exit 1
fi

echo "📋 Checking prerequisites..."

# Check Node.js
if ! command -v node &> /dev/null; then
    echo "❌ Node.js not found. Please install Node.js 18+ from https://nodejs.org/"
    exit 1
fi

NODE_VERSION=$(node --version | cut -d'v' -f2 | cut -d'.' -f1)
if [ "$NODE_VERSION" -lt 16 ]; then
    echo "❌ Node.js version $NODE_VERSION is too old. Please install Node.js 18+"
    exit 1
fi

echo "✅ Node.js $(node --version) found"

# Check npm
if ! command -v npm &> /dev/null; then
    echo "❌ npm not found. Please install npm"
    exit 1
fi

echo "✅ npm $(npm --version) found"

# Setup frontend
echo ""
echo "📦 Setting up frontend..."
cd web_app/frontend

# Install dependencies
echo "Installing dependencies (this may take a few minutes)..."
npm install

# Create environment file
echo "Creating environment configuration..."
cat > .env.local << 'EOF'
REACT_APP_API_URL=http://localhost:8000
REACT_APP_PROJECT_ID=proyecto-integrador-dae-2025
REACT_APP_DEMO_MODE=true
EOF

# Create a simple start script
cat > start-demo.js << 'EOF'
const { spawn } = require('child_process');
const fs = require('fs');

console.log('🌟 Starting DMC Pipeline Demo...');
console.log('');

// Check if backend is running
const checkBackend = () => {
    const http = require('http');
    const options = {
        hostname: 'localhost',
        port: 8000,
        path: '/health',
        method: 'GET',
        timeout: 1000
    };

    return new Promise((resolve) => {
        const req = http.request(options, (res) => {
            resolve(res.statusCode === 200);
        });
        req.on('error', () => resolve(false));
        req.on('timeout', () => resolve(false));
        req.end();
    });
};

const startFrontend = async () => {
    const backendRunning = await checkBackend();
    
    if (!backendRunning) {
        console.log('⚠️  Backend not detected at http://localhost:8000');
        console.log('   The app will run in DEMO MODE with mock data');
        console.log('');
        console.log('💡 To run with full backend:');
        console.log('   1. Open another terminal');
        console.log('   2. cd web_app/backend');
        console.log('   3. pip install -r requirements.txt');
        console.log('   4. uvicorn main:app --reload');
        console.log('');
    } else {
        console.log('✅ Backend detected - running with full functionality');
        console.log('');
    }

    console.log('🌐 Starting frontend at http://localhost:3000');
    console.log('   Press Ctrl+C to stop');
    console.log('');

    const child = spawn('npm', ['start'], { stdio: 'inherit' });
    
    process.on('SIGINT', () => {
        console.log('\n👋 Shutting down demo...');
        child.kill('SIGINT');
        process.exit(0);
    });
};

startFrontend();
EOF

echo ""
echo "✅ Frontend setup completed!"
echo ""

# Go back to project root
cd ../..

echo "🎉 Demo setup completed successfully!"
echo ""
echo "🚀 To start the demo:"
echo ""
echo "   Option 1 - Frontend only (with mock data):"
echo "   cd web_app/frontend"
echo "   node start-demo.js"
echo ""
echo "   Option 2 - Full stack (requires Python):"
echo "   Terminal 1: cd web_app/backend && pip install -r requirements.txt && uvicorn main:app --reload"
echo "   Terminal 2: cd web_app/frontend && node start-demo.js"
echo ""
echo "   Option 3 - GitHub Pages demo:"
echo "   Just push to main branch and enable GitHub Pages with /docs folder"
echo ""
echo "📱 The app will open automatically at: http://localhost:3000"
echo ""
echo "🔗 Static demo available at: https://noctics123.github.io/dmc_proyecto_integrador"
echo ""
echo "🎯 Happy testing! 🚀"