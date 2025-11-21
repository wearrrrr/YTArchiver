VENV_DIR="venv"

if [ -f "$VENV_DIR/bin/activate" ]; then
    . "$VENV_DIR/bin/activate"
else
    echo "No POSIX venv found at $VENV_DIR/bin/activate"
fi
