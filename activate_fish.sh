set VENV_DIR venv

if test -f "$VENV_DIR/bin/activate.fish"
    source "$VENV_DIR/bin/activate.fish"
else
    echo "No fish venv found at $VENV_DIR/bin/activate.fish"
end
