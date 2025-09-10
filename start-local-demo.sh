#!/bin/bash

# DMC Pipeline - Local Demo Startup Script
# This script starts both frontend and backend for local testing

echo "🚀 DMC Data Pipeline - Local Demo"
echo "=================================="
echo ""

# Check prerequisites
echo "📋 Checking prerequisites..."

# Check Node.js
if ! command -v node &> /dev/null; then
    echo "❌ Node.js not found. Please install Node.js 18+ from https://nodejs.org/"
    echo "   Then run this script again."
    exit 1
fi

# Check Python
if ! command -v python &> /dev/null && ! command -v python3 &> /dev/null; then
    echo "❌ Python not found. Please install Python 3.8+ from https://python.org/"
    echo "   Then run this script again."
    exit 1
fi

# Use python3 if available, otherwise python
PYTHON_CMD="python"
if command -v python3 &> /dev/null; then
    PYTHON_CMD="python3"
fi

echo "✅ Node.js $(node --version) found"
echo "✅ Python $($PYTHON_CMD --version) found"

# Check if we're in the right directory
if [ ! -f "web_app/frontend/package.json" ]; then
    echo "❌ Error: Please run this script from the project root directory"
    echo "   Expected to find: web_app/frontend/package.json"
    exit 1
fi

echo "✅ Project structure verified"
echo ""

# Setup function
setup_frontend() {
    echo "📦 Setting up frontend..."
    cd web_app/frontend
    
    if [ ! -d "node_modules" ]; then
        echo "Installing npm dependencies (this may take a few minutes)..."
        npm install
        if [ $? -ne 0 ]; then
            echo "❌ Failed to install frontend dependencies"
            exit 1
        fi
    else
        echo "✅ Dependencies already installed"
    fi
    
    echo "✅ Frontend setup complete"
    cd ../..
}

setup_backend() {
    echo "🔧 Setting up backend..."
    cd web_app/backend
    
    # Check if virtual environment exists
    if [ ! -d "venv" ]; then
        echo "Creating Python virtual environment..."
        $PYTHON_CMD -m venv venv
        if [ $? -ne 0 ]; then
            echo "❌ Failed to create virtual environment"
            exit 1
        fi
    fi
    
    # Activate virtual environment
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
        source venv/Scripts/activate
    else
        source venv/bin/activate
    fi
    
    # Install dependencies
    echo "Installing Python dependencies..."
    pip install -r requirements_simple.txt > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "❌ Failed to install backend dependencies"
        exit 1
    fi
    
    echo "✅ Backend setup complete"
    cd ../..
}

# Setup both components
setup_frontend
setup_backend

echo ""
echo "🎉 Setup completed successfully!"
echo ""
echo "🚀 Starting DMC Pipeline Demo..."
echo ""

# Create startup script for better process management
cat > start_services.sh << 'EOF'
#!/bin/bash

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "🛑 Shutting down services..."
    kill $BACKEND_PID 2>/dev/null
    kill $FRONTEND_PID 2>/dev/null
    echo "👋 Demo stopped. Thanks for using DMC Pipeline!"
    exit 0
}

# Set trap for cleanup
trap cleanup SIGINT SIGTERM

echo "🔧 Starting backend API..."
cd web_app/backend
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    source venv/Scripts/activate
else
    source venv/bin/activate
fi

python main_simple.py &
BACKEND_PID=$!

# Wait for backend to start
echo "⏳ Waiting for backend to start..."
sleep 3

# Check if backend is running
if ! curl -s http://localhost:8000/health > /dev/null; then
    echo "⚠️ Backend might not be ready yet, but continuing..."
fi

echo "🌐 Starting frontend..."
cd ../frontend

# Set environment for demo mode
export REACT_APP_DEMO_MODE=false  # Use backend API
export REACT_APP_API_URL=http://localhost:8000

npm start &
FRONTEND_PID=$!

echo ""
echo "✅ Both services started!"
echo ""
echo "🔗 Access the application:"
echo "   Frontend: http://localhost:3000"
echo "   Backend API: http://localhost:8000"
echo "   API Docs: http://localhost:8000/docs"
echo ""
echo "📱 The frontend should open automatically in your browser"
echo "⚠️  Press Ctrl+C to stop all services"
echo ""

# Wait for both processes
wait $BACKEND_PID $FRONTEND_PID
EOF

chmod +x start_services.sh

echo "📋 Instructions:"
echo ""
echo "   Option 1 - Auto Start (Recommended):"
echo "   ./start_services.sh"
echo ""
echo "   Option 2 - Manual Start:"
echo "   Terminal 1: cd web_app/backend && source venv/bin/activate && python main_simple.py"
echo "   Terminal 2: cd web_app/frontend && npm start"
echo ""
echo "   Option 3 - Frontend Only (Mock Data):"
echo "   cd web_app/frontend && npm start"
echo ""

# Ask user preference
echo "🤔 How would you like to start the demo?"
echo "   1) Auto start both backend and frontend (recommended)"
echo "   2) I'll start them manually"
echo "   3) Just show me the instructions"
echo ""
read -p "Enter your choice (1-3): " choice

case $choice in
    1)
        echo ""
        echo "🚀 Starting auto demo..."
        ./start_services.sh
        ;;
    2)
        echo ""
        echo "📋 Manual startup instructions:"
        echo ""
        echo "Terminal 1 (Backend):"
        echo "cd web_app/backend"
        if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
            echo "source venv/Scripts/activate"
        else
            echo "source venv/bin/activate"
        fi
        echo "python main_simple.py"
        echo ""
        echo "Terminal 2 (Frontend):"
        echo "cd web_app/frontend"
        echo "npm start"
        echo ""
        ;;
    3)
        echo ""
        echo "📚 All setup is complete! You can start the services anytime using:"
        echo "./start_services.sh"
        echo ""
        ;;
    *)
        echo "Invalid choice. Run ./start_services.sh when ready."
        ;;
esac

echo "🎯 Happy testing! 🚀"